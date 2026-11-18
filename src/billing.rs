//! Calendar helpers for monthly credit and `tpmo` periods.
//!
//! The end-of-month rule: a reset day past the end of a short month means
//! that month's last day.

/// Days since 1970-01-01 (Hinnant's days-from-civil algorithm).
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + (d as i64) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Inverse of `days_from_civil` (Hinnant's civil-from-days algorithm).
pub fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m as u32, d as u32)
}

/// Number of days in a month, handling leap years.
pub fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            // Leap year: divisible by 4, but not by 100 unless also by 400
            let is_leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
            if is_leap { 29 } else { 28 }
        }
        _ => 0, // invalid month
    }
}

/// Parse strict `YYYY-MM-DD` to days since the epoch.
/// Rejects month 0 or 13, impossible days, years outside 1970..=9999, and any extra characters.
pub fn parse_date(s: &str) -> Option<i64> {
    if s.len() != 10 {
        return None;
    }
    let year: i64 = s[0..4].parse().ok()?;
    if !(1970..=9999).contains(&year) {
        return None;
    }
    if &s[4..5] != "-" || &s[7..8] != "-" {
        return None;
    }
    let month: u32 = s[5..7].parse().ok()?;
    if !(1..=12).contains(&month) {
        return None;
    }
    let day: u32 = s[8..10].parse().ok()?;
    let dim = days_in_month(year, month);
    if day == 0 || day > dim {
        return None;
    }
    Some(days_from_civil(year, month, day))
}

/// Unix seconds at `reset_hour`:00 UTC on day `min(reset_day, days_in_month(y, m))` of month (y, m).
/// This is the end-of-month rule: a reset day past the end of a short month means that month's last day.
fn boundary(y: i64, m: u32, reset_day: u8, reset_hour: u8) -> u64 {
    let dim = days_in_month(y, m);
    let day = u32::from(reset_day).min(dim);
    let days = days_from_civil(y, m, day);
    let secs = days * 86_400 + i64::from(reset_hour) * 3600;
    u64::try_from(secs.max(0)).unwrap_or(0)
}

/// The start of the monthly period containing `unix`.
/// Take the civil (y, m) of `unix`. If `unix >= boundary(y, m)`, return that boundary.
/// Otherwise return the previous month's boundary.
pub fn month_start(unix: u64, reset_day: u8, reset_hour: u8) -> u64 {
    let days = i64::try_from(unix / 86_400).unwrap_or(0);
    let (y, m, _) = civil_from_days(days);
    let b = boundary(y, m, reset_day, reset_hour);
    if unix >= b {
        b
    } else {
        // previous month
        let (py, pm) = if m == 1 { (y - 1, 12) } else { (y, m - 1) };
        boundary(py, pm, reset_day, reset_hour)
    }
}

/// The boundary of the month after the month of `month_start(..)`.
pub fn next_month_start(unix: u64, reset_day: u8, reset_hour: u8) -> u64 {
    let start = month_start(unix, reset_day, reset_hour);
    let days = i64::try_from(start / 86_400).unwrap_or(0);
    let (y, m, _) = civil_from_days(days);
    let (ny, nm) = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
    boundary(ny, nm, reset_day, reset_hour)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to construct a Unix timestamp from civil date and hour.
    fn at(y: i64, m: u32, d: u32, h: u8) -> u64 {
        let days = days_from_civil(y, m, d);
        u64::try_from(days * 86_400 + i64::from(h) * 3600).unwrap()
    }

    #[test]
    fn civil_round_trip_and_known_days() {
        // days_from_civil
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(2000, 3, 1), 11017);
        assert_eq!(days_from_civil(2026, 11, 18), 20775);
        assert_eq!(days_from_civil(2028, 2, 29), 21243);

        // civil_from_days inverts
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(11017), (2000, 3, 1));
        assert_eq!(civil_from_days(20775), (2026, 11, 18));
        assert_eq!(civil_from_days(21243), (2028, 2, 29));
    }

    #[test]
    fn parse_date_accepts_only_real_dates() {
        assert_eq!(parse_date("2026-11-18"), Some(20775));
        assert_eq!(parse_date("2026-02-30"), None);
        assert_eq!(parse_date("2026-13-01"), None);
        assert_eq!(parse_date("26-11-18"), None);
        assert_eq!(parse_date("2026-11-18x"), None);
        assert_eq!(parse_date(""), None);
        assert_eq!(parse_date("1969-12-31"), None);
        assert_eq!(parse_date("10000-01-01"), None);
    }

    #[test]
    fn month_period_end_of_month_rule() {
        // reset_day 31: short months fall back to last day
        // 2026-02-15: month start is 2026-01-31
        let start = month_start(at(2026, 2, 15, 0), 31, 0);
        assert_eq!(start, at(2026, 1, 31, 0));
        let next = next_month_start(at(2026, 2, 15, 0), 31, 0);
        assert_eq!(next, at(2026, 2, 28, 0));

        // At the boundary instant, it belongs to that period
        let start = month_start(at(2026, 2, 28, 0), 31, 0);
        assert_eq!(start, at(2026, 2, 28, 0));
        let next = next_month_start(at(2026, 2, 28, 0), 31, 0);
        assert_eq!(next, at(2026, 3, 31, 0));

        // Leap year: reset_day 30 at 2028-02-29
        let start = month_start(at(2028, 2, 29, 12), 30, 0);
        assert_eq!(start, at(2028, 2, 29, 0));
    }

    #[test]
    fn month_period_reset_hour_and_year_boundary() {
        // reset_hour = 8
        // 2026-11-15 07:59:59 -> period started 2026-10-15 08:00
        let start = month_start(at(2026, 11, 15, 7) + 3599, 15, 8);
        assert_eq!(start, at(2026, 10, 15, 8));

        // 2026-11-15 08:00:00 -> period started 2026-11-15 08:00
        let start = month_start(at(2026, 11, 15, 8), 15, 8);
        assert_eq!(start, at(2026, 11, 15, 8));

        // reset_day = 1, year boundary
        // 2026-12-31 23:59:59 -> period started 2026-12-01 00:00
        let start = month_start(at(2026, 12, 31, 23) + 3599, 1, 0);
        assert_eq!(start, at(2026, 12, 1, 0));

        let next = next_month_start(at(2026, 12, 31, 23) + 3599, 1, 0);
        assert_eq!(next, at(2027, 1, 1, 0));
    }
}
