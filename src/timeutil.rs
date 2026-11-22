//! Timestamp and price rules from rules.md, matching the DuckDB pipeline.

const MONTH_DAYS: [i64; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

fn div_floor(a: i64, b: i64) -> i64 {
    let q = a / b;
    let r = a % b;
    if r != 0 && (a < 0) != (b < 0) {
        q - 1
    } else {
        q
    }
}

fn leap(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn valid_clock(year: i64, month: i64, day: i64, hour: i64, minute: i64, second: i64) -> bool {
    if !(1..=12).contains(&month) || !(0..=23).contains(&hour) || !(0..=59).contains(&minute) || !(0..=59).contains(&second)
    {
        return false;
    }
    let limit = if month == 2 && leap(year) { 29 } else { MONTH_DAYS[month as usize] };
    (1..=limit).contains(&day)
}

fn epoch_ms(mut year: i64, month: i64, day: i64, hour: i64, minute: i64, second: i64) -> i64 {
    if month <= 2 {
        year -= 1;
    }
    let era = if year >= 0 {
        div_floor(year, 400)
    } else {
        div_floor(year - 399, 400)
    };
    let yoe = year - era * 400;
    let month_adj = month + if month > 2 { -3 } else { 9 };
    let doy = div_floor(153 * month_adj + 2, 5) + day - 1;
    let doe = yoe * 365 + div_floor(yoe, 4) - div_floor(yoe, 100) + doy;
    let days = era * 146097 + doe - 719468;
    (days * 86400 + hour * 3600 + minute * 60 + second) * 1000
}

fn parse_fixed(bytes: &[u8]) -> Option<i64> {
    let mut n = 0i64;
    for &b in bytes {
        if !b.is_ascii_digit() {
            return None;
        }
        n = n * 10 + (b - b'0') as i64;
    }
    Some(n)
}

fn parse_clock(b: &[u8]) -> Option<(i64, i64, i64, i64, i64, i64)> {
    if b.len() != 19
        || b[4] != b'-'
        || b[7] != b'-'
        || b[10] != b'T'
        || b[13] != b':'
        || b[16] != b':'
    {
        return None;
    }
    let y = parse_fixed(&b[0..4])?;
    let mo = parse_fixed(&b[5..7])?;
    let d = parse_fixed(&b[8..10])?;
    let h = parse_fixed(&b[11..13])?;
    let mi = parse_fixed(&b[14..16])?;
    let s = parse_fixed(&b[17..19])?;
    if !valid_clock(y, mo, d, h, mi, s) {
        return None;
    }
    Some((y, mo, d, h, mi, s))
}

fn parse_i64_digits(v: &str) -> Option<i64> {
    if v.is_empty() {
        return None;
    }
    let mut n: i64 = 0;
    for b in v.bytes() {
        if !b.is_ascii_digit() {
            return None;
        }
        n = n.checked_mul(10)?.checked_add((b - b'0') as i64)?;
    }
    Some(n)
}

pub fn ts_to_ms(v: &str) -> Option<i64> {
    let b = v.as_bytes();
    if b.len() == 20 && b[19] == b'Z' {
        let (y, mo, d, h, mi, s) = parse_clock(&b[..19])?;
        return Some(epoch_ms(y, mo, d, h, mi, s));
    }
    if b.len() == 25 && (b[19] == b'+' || b[19] == b'-') && b[22] == b':' {
        let (y, mo, d, h, mi, s) = parse_clock(&b[..19])?;
        let oh = parse_fixed(&b[20..22])?;
        let om = parse_fixed(&b[23..25])?;
        if oh > 23 || om > 59 {
            return None;
        }
        let mut off = (oh * 3600 + om * 60) * 1000;
        if b[19] == b'-' {
            off = -off;
        }
        return Some(epoch_ms(y, mo, d, h, mi, s) - off);
    }
    if !v.is_empty() && v.bytes().all(|c| c.is_ascii_digit()) {
        return parse_i64_digits(v);
    }
    None
}

pub fn price_val(v: &str) -> Option<i64> {
    if v.is_empty() {
        return None;
    }
    let mut n: i64 = 0;
    for b in v.bytes() {
        if !b.is_ascii_digit() {
            return None;
        }
        n = n.checked_mul(10)?.checked_add((b - b'0') as i64)?;
        if n > 10_000_000 {
            return None;
        }
    }
    if n == 0 { None } else { Some(n) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_match_oracle() {
        let good = [
            ("1970-01-01T00:00:00Z", 0),
            ("1970-01-02T00:00:00Z", 86_400_000),
            ("86400000", 86_400_000),
            ("0", 0),
            ("000", 0),
            ("2026-10-03T23:58:10Z", 1_791_071_890_000),
            ("2026-10-03T19:58:10-04:00", 1_791_071_890_000),
            ("2026-10-04T11:58:10+12:00", 1_791_071_890_000),
            ("0001791071890000", 1_791_071_890_000),
            ("1791071890000", 1_791_071_890_000),
            ("2026-10-03T10:00:00-00:00", 1_791_021_600_000),
            ("2026-10-03T10:00:00+23:59", 1_790_935_260_000),
            ("2028-02-29T00:00:00Z", 1_835_395_200_000),
            ("2028-02-29T23:59:59-10:00", 1_835_517_599_000),
            ("2024-12-31T23:59:59Z", 1_735_689_599_000),
            ("2025-01-01T00:00:00Z", 1_735_689_600_000),
            ("2027-03-01T00:00:00+00:00", 1_803_859_200_000),
            ("2000-02-29T12:00:00Z", 951_825_600_000),
            ("0000-01-01T00:00:00Z", -62_167_219_200_000),
            ("9999-12-31T23:59:59Z", 253_402_300_799_000),
            ("9223372036854775807", i64::MAX),
            ("00009223372036854775807", i64::MAX),
        ];
        for (text, want) in good {
            assert_eq!(ts_to_ms(text), Some(want), "{text}");
        }
        let bad = [
            "2026-02-29T00:00:00Z",
            "2026-10-03T24:00:00Z",
            "2026-10-03T10:00:60Z",
            "2026-10-03T10:00:00z",
            "2026-10-03T10:00:00+24:00",
            "2026-10-03T10:00:00+05:60",
            "2026-10-03T10:00:00+0200",
            "2026-13-03T10:00:00Z",
            "2026-04-31T10:00:00Z",
            "2026-10-03 10:00:00Z",
            "2026-10-03T10:00:00",
            "2026-10-03T10:00:00.5Z",
            "2026-10-03T10:00:00Z\r",
            "1791071890000.0",
            "1.79e12",
            "-5",
            "9223372036854775808",
            "١٧٩١٠٢١٦٠٠٠٠٠",
            "２026-10-03T10:00:00Z",
            "",
        ];
        for text in bad {
            assert_eq!(ts_to_ms(text), None, "{text}");
        }
    }

    #[test]
    fn prices_match_oracle() {
        let cases = [
            ("0250", Some(250)),
            ("1", Some(1)),
            ("10000000", Some(10_000_000)),
            ("10000001", None),
            ("0", None),
            ("000", None),
            ("", None),
            ("12.5", None),
            ("-5", None),
            ("+5", None),
            ("99999999999999999999999", None),
            ("1000\r", None),
            ("０250", None),
            ("٢٥٠", None),
        ];
        for (text, want) in cases {
            assert_eq!(price_val(text), want, "{text}");
        }
    }
}
