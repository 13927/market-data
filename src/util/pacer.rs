use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::{sleep_until, Instant};

/// A simple global rate limiter: each `wait()` reserves the next slot separated by `interval`.
/// Intended for exchange control-message pacing (subscribe/unsubscribe) to avoid server throttles.
pub struct Pacer {
    interval: Duration,
    next_at: Mutex<Instant>,
}

impl Pacer {
    pub fn new(interval: Duration) -> Self {
        // Allow the first call to proceed immediately.
        let now = Instant::now();
        Self {
            interval,
            next_at: Mutex::new(now),
        }
    }

    pub async fn wait(&self) {
        if self.interval.is_zero() {
            return;
        }
        let deadline = {
            let mut next_at = self.next_at.lock().await;
            let now = Instant::now();
            let deadline = (*next_at).max(now);
            // Reserve the next slot.
            *next_at = deadline + self.interval;
            deadline
        };
        sleep_until(deadline).await;
    }
}

