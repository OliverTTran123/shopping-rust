mod scan;
mod timeutil;

use std::collections::HashMap;
use std::env;
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Date32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use scan::{csv_paths, for_each_row};
use timeutil::{price_val, ts_to_ms};

const DAY_MS: i64 = 24 * 60 * 60 * 1000;
const GAP_MS: i64 = 30 * 60 * 1000;
const WEEK_MS: i64 = 7 * DAY_MS;

struct RawEvent {
    ingest: i64,
    user: Option<String>,
    ts: Option<i64>,
    event_type: Option<String>,
    product: Option<String>,
    price: Option<String>,
}

struct Ev {
    ts: i64,
    purchase: Option<(i64, String)>,
}

struct ClickAcc {
    user: Option<String>,
    campaign: Option<String>,
    ts: Option<i64>,
}

struct Click {
    click_id: String,
    campaign: String,
    ts: i64,
}

struct Sess {
    start: i64,
    day: i32,
    n_events: i64,
    revenue: i64,
    n_purchases: i64,
}

#[derive(Default)]
struct UserAcc {
    sessions: i64,
    events: i64,
    revenue: i64,
    cats: HashMap<String, i64>,
}

#[derive(Default)]
struct CampAcc {
    sessions: i64,
    purchasing: i64,
    revenue: i64,
    assisted: i64,
}

fn keep_min_str(slot: &mut Option<String>, value: Option<String>) {
    let Some(value) = value else { return };
    match slot {
        None => *slot = Some(value),
        Some(cur) if value.as_str() < cur.as_str() => *slot = Some(value),
        _ => {}
    }
}

fn find(parent: &mut [usize], mut x: usize) -> usize {
    let mut root = x;
    while parent[root] != root {
        root = parent[root];
    }
    while parent[x] != root {
        let next = parent[x];
        parent[x] = root;
        x = next;
    }
    root
}

fn union(parent: &mut [usize], a: usize, b: usize) {
    let mut ra = find(parent, a);
    let mut rb = find(parent, b);
    if ra == rb {
        return;
    }
    if ra > rb {
        std::mem::swap(&mut ra, &mut rb);
    }
    parent[rb] = ra;
}

/// user_id -> person name, only when the name differs. Codes follow byte order, so the
/// smallest code in a component is the person's name.
fn person_names(edges: &[(String, String)]) -> HashMap<String, String> {
    let mut ids: Vec<String> = Vec::new();
    let mut index: HashMap<String, usize> = HashMap::new();
    for (a, b) in edges {
        for id in [a, b] {
            if let std::collections::hash_map::Entry::Vacant(slot) = index.entry(id.clone()) {
                slot.insert(ids.len());
                ids.push(id.clone());
            }
        }
    }
    let mut order: Vec<usize> = (0..ids.len()).collect();
    order.sort_by(|&i, &j| ids[i].as_bytes().cmp(ids[j].as_bytes()));
    let mut rank = vec![0usize; ids.len()];
    let mut sorted = Vec::with_capacity(ids.len());
    for (code, &i) in order.iter().enumerate() {
        rank[i] = code;
        sorted.push(ids[i].clone());
    }
    let mut parent: Vec<usize> = (0..sorted.len()).collect();
    for (a, b) in edges {
        union(&mut parent, rank[index[a]], rank[index[b]]);
    }
    let mut names = HashMap::new();
    for i in 0..sorted.len() {
        let root = find(&mut parent, i);
        if root != i {
            names.insert(sorted[i].clone(), sorted[root].clone());
        }
    }
    names
}

fn person_of<'a>(names: &'a HashMap<String, String>, id: &'a str) -> &'a str {
    names.get(id).map(String::as_str).unwrap_or(id)
}

fn load_lookup(path: &Path) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for_each_row(&vec![path.to_path_buf()], 2, |mut fields| {
        let val = fields.pop().flatten();
        let key = fields.pop().flatten();
        if let (Some(key), Some(val)) = (key, val) {
            keep_min_str(map.entry(key).or_default(), Some(val));
        }
    });
    map.into_iter().filter_map(|(k, v)| v.map(|v| (k, v))).collect()
}

fn load_links(dir: &Path) -> HashMap<String, String> {
    let mut edges = Vec::new();
    for_each_row(&csv_paths(dir), 2, |fields| {
        if let (Some(a), Some(b)) = (fields[0].clone(), fields[1].clone()) {
            if a != b {
                edges.push((a, b));
            }
        }
    });
    person_names(&edges)
}

fn load_events(
    dir: &Path,
    names: &HashMap<String, String>,
    products: &HashMap<String, String>,
    window_start: i64,
    window_end: i64,
) -> HashMap<String, Vec<Ev>> {
    let mut raw: HashMap<String, RawEvent> = HashMap::new();
    for_each_row(&csv_paths(dir), 7, |fields| {
        let Some(event_id) = fields[0].clone() else { return };
        let Some(ingest) = fields[3].as_deref().and_then(ts_to_ms) else { return };
        if raw.get(&event_id).is_some_and(|prev| prev.ingest <= ingest) {
            return;
        }
        raw.insert(
            event_id,
            RawEvent {
                ingest,
                user: fields[1].clone(),
                ts: fields[2].as_deref().and_then(ts_to_ms),
                event_type: fields[4].clone(),
                product: fields[5].clone(),
                price: fields[6].clone(),
            },
        );
    });

    let mut by_person: HashMap<String, Vec<Ev>> = HashMap::new();
    for ev in raw.into_values() {
        let Some(user) = ev.user else { continue };
        let Some(ts) = ev.ts else { continue };
        if ts < window_start || ts >= window_end || ev.ingest - ts > DAY_MS {
            continue;
        }
        let Some(product) = ev.product else { continue };
        let purchase = match ev.event_type.as_deref() {
            Some("view") | Some("cart") => None,
            Some("purchase") => {
                let price = ev.price.as_deref().and_then(price_val);
                let Some(price) = price else { continue };
                let category = products.get(&product).cloned().unwrap_or_else(|| "unknown".to_string());
                Some((price, category))
            }
            _ => continue,
        };
        let person = person_of(names, &user).to_string();
        by_person.entry(person).or_default().push(Ev { ts, purchase });
    }
    by_person
}

fn load_clicks(
    dir: &Path,
    names: &HashMap<String, String>,
    campaigns: &HashMap<String, String>,
    click_start: i64,
    window_end: i64,
) -> HashMap<String, Vec<Click>> {
    let mut raw: HashMap<String, ClickAcc> = HashMap::new();
    for_each_row(&csv_paths(dir), 4, |fields| {
        let Some(click_id) = fields[0].clone() else { return };
        let acc = raw.entry(click_id).or_insert(ClickAcc { user: None, campaign: None, ts: None });
        keep_min_str(&mut acc.user, fields[1].clone());
        keep_min_str(&mut acc.campaign, fields[2].clone());
        if let Some(ts) = fields[3].as_deref().and_then(ts_to_ms) {
            acc.ts = Some(acc.ts.map_or(ts, |cur| cur.min(ts)));
        }
    });

    let mut by_person: HashMap<String, Vec<Click>> = HashMap::new();
    for (click_id, acc) in raw {
        let Some(user) = acc.user else { continue };
        let Some(ts) = acc.ts else { continue };
        if ts < click_start || ts >= window_end {
            continue;
        }
        let Some(campaign) = acc.campaign else { continue };
        if !campaigns.contains_key(&campaign) {
            continue;
        }
        let person = person_of(names, &user).to_string();
        by_person.entry(person).or_default().push(Click { click_id, campaign, ts });
    }
    by_person
}

fn build_sessions(events: &mut [Ev]) -> Vec<Sess> {
    events.sort_by_key(|ev| ev.ts);
    let mut out = Vec::new();
    let mut cur: Option<Sess> = None;
    let mut last_ts: Option<i64> = None;
    for ev in events.iter() {
        let fresh = last_ts.map(|t| ev.ts - t > GAP_MS).unwrap_or(true);
        if fresh {
            if let Some(done) = cur.take() {
                out.push(done);
            }
            cur = Some(Sess {
                start: ev.ts,
                day: ev.ts.div_euclid(DAY_MS) as i32,
                n_events: 0,
                revenue: 0,
                n_purchases: 0,
            });
        }
        let sess = cur.as_mut().expect("session");
        sess.n_events += 1;
        if let Some((price, _)) = &ev.purchase {
            sess.revenue += price;
            sess.n_purchases += 1;
        }
        last_ts = Some(ev.ts);
    }
    if let Some(done) = cur {
        out.push(done);
    }
    out
}

fn fold_purchases(person: &str, events: &[Ev], sessions: &[Sess], users: &mut HashMap<(String, i32), UserAcc>) {
    let mut si = 0usize;
    for ev in events {
        while si + 1 < sessions.len() && ev.ts >= sessions[si + 1].start {
            si += 1;
        }
        if let Some((price, category)) = &ev.purchase {
            let acc = users.entry((person.to_string(), sessions[si].day)).or_default();
            *acc.cats.entry(category.clone()).or_insert(0) += price;
        }
    }
    for sess in sessions {
        let acc = users.entry((person.to_string(), sess.day)).or_default();
        acc.sessions += 1;
        acc.events += sess.n_events;
        acc.revenue += sess.revenue;
    }
}

fn attribute(sessions: &[Sess], clicks: &mut [Click], camps: &mut HashMap<(String, i32), CampAcc>) {
    clicks.sort_by(|a, b| a.ts.cmp(&b.ts).then(a.click_id.as_str().cmp(b.click_id.as_str())));
    let mut lo = 0usize;
    let mut hi = 0usize;
    let mut in_win: HashMap<String, i32> = HashMap::new();
    for sess in sessions {
        let left = sess.start - WEEK_MS;
        while hi < clicks.len() && clicks[hi].ts <= sess.start {
            *in_win.entry(clicks[hi].campaign.clone()).or_insert(0) += 1;
            hi += 1;
        }
        while lo < hi && clicks[lo].ts < left {
            let drop = if let Some(count) = in_win.get_mut(&clicks[lo].campaign) {
                *count -= 1;
                *count == 0
            } else {
                false
            };
            if drop {
                in_win.remove(&clicks[lo].campaign);
            }
            lo += 1;
        }
        let credit = if hi > 0 && sess.start - clicks[hi - 1].ts <= WEEK_MS {
            let best_ts = clicks[hi - 1].ts;
            let mut i = hi - 1;
            while i > 0 && clicks[i - 1].ts == best_ts {
                i -= 1;
            }
            clicks[i].campaign.clone()
        } else {
            "organic".to_string()
        };
        let acc = camps.entry((credit, sess.day)).or_default();
        acc.sessions += 1;
        if sess.n_purchases > 0 {
            acc.purchasing += 1;
        }
        acc.revenue += sess.revenue;
        let assisted: Vec<String> = in_win
            .iter()
            .filter(|(camp, count)| **count > 0 && camp.as_str() != "organic")
            .map(|(camp, _)| camp.clone())
            .collect();
        for camp in assisted {
            camps.entry((camp, sess.day)).or_default().assisted += 1;
        }
    }
}

fn top_category(cats: &HashMap<String, i64>) -> Option<String> {
    let mut best: Option<(&str, i64)> = None;
    for (name, &rev) in cats {
        match best {
            None => best = Some((name, rev)),
            Some((best_name, best_rev)) if rev > best_rev || (rev == best_rev && name.as_str() < best_name) => {
                best = Some((name, rev));
            }
            _ => {}
        }
    }
    best.map(|(name, _)| name.to_string())
}

fn write_parquet(path: &Path, schema: Arc<Schema>, cols: Vec<Arc<dyn arrow_array::Array>>) {
    let batch = RecordBatch::try_new(schema.clone(), cols).unwrap_or_else(|err| panic!("batch: {err}"));
    let file = File::create(path).unwrap_or_else(|err| panic!("create {}: {err}", path.display()));
    let props = WriterProperties::builder().set_compression(Compression::UNCOMPRESSED).build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap_or_else(|err| panic!("writer: {err}"));
    writer.write(&batch).unwrap_or_else(|err| panic!("write: {err}"));
    writer.close().unwrap_or_else(|err| panic!("close: {err}"));
}

fn prepare(dir: &Path) {
    if dir.exists() {
        for ent in fs::read_dir(dir).unwrap_or_else(|err| panic!("read {}: {err}", dir.display())) {
            let path = ent.unwrap_or_else(|err| panic!("{err}")).path();
            if path.is_file() {
                fs::remove_file(&path).unwrap_or_else(|err| panic!("remove {}: {err}", path.display()));
            }
        }
    }
    fs::create_dir_all(dir).unwrap_or_else(|err| panic!("mkdir {}: {err}", dir.display()));
}

fn write_outputs(
    out: &Path,
    users: HashMap<(String, i32), UserAcc>,
    camps: HashMap<(String, i32), CampAcc>,
    channels: &HashMap<String, String>,
) {
    let user_dir = out.join("user_daily");
    let camp_dir = out.join("campaign_daily");
    prepare(&user_dir);
    prepare(&camp_dir);

    let mut user_rows: Vec<(String, i32, UserAcc)> = users.into_iter().map(|((id, day), acc)| (id, day, acc)).collect();
    user_rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    let mut ids = Vec::with_capacity(user_rows.len());
    let mut days = Vec::with_capacity(user_rows.len());
    let mut sessions = Vec::with_capacity(user_rows.len());
    let mut events = Vec::with_capacity(user_rows.len());
    let mut revenue = Vec::with_capacity(user_rows.len());
    let mut tops = Vec::with_capacity(user_rows.len());
    for (id, day, acc) in &user_rows {
        ids.push(id.as_str());
        days.push(*day);
        sessions.push(acc.sessions);
        events.push(acc.events);
        revenue.push(acc.revenue);
        tops.push(top_category(&acc.cats));
    }
    let user_schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]));
    write_parquet(
        &user_dir.join("part-0.parquet"),
        user_schema,
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(Date32Array::from(days)),
            Arc::new(Int64Array::from(sessions)),
            Arc::new(Int64Array::from(events)),
            Arc::new(Int64Array::from(revenue)),
            Arc::new(StringArray::from(tops)),
        ],
    );

    let mut camp_rows: Vec<(String, i32, CampAcc)> = camps.into_iter().map(|((id, day), acc)| (id, day, acc)).collect();
    camp_rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    let mut cids = Vec::with_capacity(camp_rows.len());
    let mut channels_out = Vec::with_capacity(camp_rows.len());
    let mut cdays = Vec::with_capacity(camp_rows.len());
    let mut csessions = Vec::with_capacity(camp_rows.len());
    let mut purchasing = Vec::with_capacity(camp_rows.len());
    let mut crevenue = Vec::with_capacity(camp_rows.len());
    let mut assisted = Vec::with_capacity(camp_rows.len());
    for (id, day, acc) in &camp_rows {
        let channel = if id == "organic" {
            "organic".to_string()
        } else {
            channels.get(id).cloned().unwrap_or_default()
        };
        cids.push(id.clone());
        channels_out.push(channel);
        cdays.push(*day);
        csessions.push(acc.sessions);
        purchasing.push(acc.purchasing);
        crevenue.push(acc.revenue);
        assisted.push(if id == "organic" { 0 } else { acc.assisted });
    }
    let camp_schema = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]));
    write_parquet(
        &camp_dir.join("part-0.parquet"),
        camp_schema,
        vec![
            Arc::new(StringArray::from(cids)),
            Arc::new(StringArray::from(channels_out)),
            Arc::new(Date32Array::from(cdays)),
            Arc::new(Int64Array::from(csessions)),
            Arc::new(Int64Array::from(purchasing)),
            Arc::new(Int64Array::from(crevenue)),
            Arc::new(Int64Array::from(assisted)),
        ],
    );
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let data = args.next().or_else(|| env::var("DATA_DIR").ok()).unwrap_or_else(|| "/data".to_string());
    let out = args.next().or_else(|| env::var("OUTPUT_DIR").ok()).unwrap_or_else(|| "/output".to_string());
    let root = Path::new(&data);
    let started = Instant::now();
    let mut mark = started;

    let manifest: serde_json::Value = serde_json::from_slice(&fs::read(root.join("manifest.json"))?)?;
    let window_start = manifest["window_start"].as_str().and_then(ts_to_ms).ok_or("bad window_start")?;
    let window_end = manifest["window_end"].as_str().and_then(ts_to_ms).ok_or("bad window_end")?;
    let products = load_lookup(&root.join("products.csv"));
    let campaigns = load_lookup(&root.join("campaigns.csv"));
    let stage = |mark: &mut Instant, label: &str| {
        let now = Instant::now();
        eprintln!("stage {:8.3}s  {label}", now.duration_since(*mark).as_secs_f64());
        *mark = now;
    };
    stage(&mut mark, "lookups");
    let names = load_links(&root.join("links"));
    stage(&mut mark, "links");
    let mut events = load_events(&root.join("events"), &names, &products, window_start, window_end);
    stage(&mut mark, "events");
    let mut clicks = load_clicks(&root.join("clicks"), &names, &campaigns, window_start - WEEK_MS, window_end);
    stage(&mut mark, "clicks");

    let mut users: HashMap<(String, i32), UserAcc> = HashMap::new();
    let mut camps: HashMap<(String, i32), CampAcc> = HashMap::new();
    for (person, evs) in events.iter_mut() {
        let sessions = build_sessions(evs);
        fold_purchases(person, evs, &sessions, &mut users);
        let mut person_clicks = clicks.remove(person).unwrap_or_default();
        attribute(&sessions, &mut person_clicks, &mut camps);
    }
    stage(&mut mark, "sessions");
    write_outputs(Path::new(&out), users, camps, &campaigns);
    stage(&mut mark, "write");
    eprintln!("stage {:8.3}s  total", started.elapsed().as_secs_f64());
    Ok(())
}

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}
