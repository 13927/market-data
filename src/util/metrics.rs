use std::sync::atomic::{AtomicU64, Ordering};

static DROPPED_EVENTS: AtomicU64 = AtomicU64::new(0);

pub fn inc_dropped_events(n: u64) {
    if n == 0 {
        return;
    }
    DROPPED_EVENTS.fetch_add(n, Ordering::Relaxed);
}

pub fn dropped_events_total() -> u64 {
    DROPPED_EVENTS.load(Ordering::Relaxed)
}
