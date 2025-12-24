use chrono::TimeZone;

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Normalize an epoch timestamp to milliseconds.
///
/// Some exchanges mix units:
/// - seconds:  1_766_000_000
/// - millis:   1_766_000_000_000
/// - micros:   1_766_000_000_000_000
/// - nanos:    1_766_000_000_000_000_000
pub fn normalize_epoch_to_ms(ts: i64) -> i64 {
    if ts <= 0 {
        return ts;
    }
    // nanoseconds (>= ~2001-09 in ns)
    if ts >= 1_000_000_000_000_000_000 {
        return ts / 1_000_000;
    }
    // microseconds (>= ~2001-09 in us)
    if ts >= 1_000_000_000_000_000 {
        return ts / 1_000;
    }
    // milliseconds (>= ~2001-09 in ms)
    if ts >= 1_000_000_000_000 {
        return ts;
    }
    // seconds (>= ~2001-09 in s)
    if ts >= 1_000_000_000 {
        return ts.saturating_mul(1_000);
    }
    ts
}

pub fn format_time_str_ms(local_ts_ms: i64) -> String {
    // Example: 2025-12-16 05:24:47.713
    let dt = chrono::Local
        .timestamp_millis_opt(local_ts_ms)
        .single()
        .unwrap_or_else(|| chrono::Local::now());
    dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}

pub fn now_local_time_str() -> String {
    // Example: 2025-12-16 05:24:47.713
    chrono::Local::now()
        .format("%Y-%m-%d %H:%M:%S%.3f")
        .to_string()
}
