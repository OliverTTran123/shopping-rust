//! CSV dialect: only `\n` ends a line, commas always separate, quotes wrap a whole field.

use std::fs;
use std::path::{Path, PathBuf};

/// `*.csv` files directly in `dir`, byte-sorted by file name. Missing dir yields none.
pub fn csv_paths(dir: &Path) -> Vec<PathBuf> {
    let Ok(rd) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut paths: Vec<PathBuf> = rd
        .filter_map(|ent| ent.ok())
        .map(|ent| ent.path())
        .filter(|path| {
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                return false;
            };
            path.is_file() && name.ends_with(".csv") && !name.starts_with('.')
        })
        .collect();
    paths.sort_by(|a, b| a.file_name().cmp(&b.file_name()));
    paths
}

fn field_ok(raw: &[u8]) -> bool {
    if !raw.contains(&b'"') {
        return true;
    }
    raw.len() >= 2 && raw[0] == b'"' && raw[raw.len() - 1] == b'"' && !raw[1..raw.len() - 1].contains(&b'"')
}

/// None if the field is malformed. Some(None) is null. Some(Some) is the unquoted value.
fn unquote(raw: &[u8]) -> Option<Option<String>> {
    if !field_ok(raw) {
        return None;
    }
    let inner = if raw.contains(&b'"') { &raw[1..raw.len() - 1] } else { raw };
    if inner.is_empty() {
        return Some(None);
    }
    Some(Some(String::from_utf8(inner.to_vec()).ok()?))
}

fn split_fields(line: &[u8], width: usize) -> Option<Vec<Option<String>>> {
    let mut fields = Vec::with_capacity(width);
    let mut start = 0usize;
    for i in 0..=line.len() {
        if i == line.len() || line[i] == b',' {
            fields.push(unquote(&line[start..i])?);
            start = i + 1;
        }
    }
    if fields.len() == width { Some(fields) } else { None }
}

/// Invoke `f` for every well-formed data line. The first line of each file is the header.
pub fn for_each_row(paths: &[PathBuf], width: usize, mut f: impl FnMut(Vec<Option<String>>)) {
    for path in paths {
        let bytes = fs::read(path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
        let mut start = 0usize;
        let mut header = true;
        for i in 0..=bytes.len() {
            if i == bytes.len() || bytes[i] == b'\n' {
                if header {
                    header = false;
                } else if let Some(fields) = split_fields(&bytes[start..i], width) {
                    f(fields);
                }
                start = i + 1;
                if i == bytes.len() {
                    break;
                }
            }
        }
    }
}
