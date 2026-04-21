use std::sync::Mutex;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const RING_SIZE: usize = 24 * 3600; // 86 400 one-second buckets

#[derive(Clone, Copy, Default)]
struct Bucket {
    ts: u64,
    queries: u64,
    errors: u64,
}

pub struct Metrics {
    started_at: Instant,
    rows_loaded: u64,
    ring: Mutex<Vec<Bucket>>,
}

pub struct StatusSnapshot {
    pub uptime_secs: u64,
    pub rows_loaded: u64,
    pub queries_1m: u64,
    pub queries_1h: u64,
    pub queries_24h: u64,
    pub errors_1m: u64,
    pub errors_1h: u64,
    pub errors_24h: u64,
}

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

impl Metrics {
    pub fn new(rows_loaded: u64) -> Self {
        Self {
            started_at: Instant::now(),
            rows_loaded,
            ring: Mutex::new(vec![Bucket::default(); RING_SIZE]),
        }
    }

    pub fn record(&self, is_error: bool) {
        let ts = unix_secs();
        let idx = (ts as usize) % RING_SIZE;
        let mut ring = self.ring.lock().expect("metrics mutex poisoned");
        let b = &mut ring[idx];
        if b.ts != ts {
            *b = Bucket { ts, queries: 0, errors: 0 };
        }
        b.queries += 1;
        if is_error {
            b.errors += 1;
        }
    }

    pub fn snapshot(&self) -> StatusSnapshot {
        let now = unix_secs();
        let ring = self.ring.lock().expect("metrics mutex poisoned");

        let mut queries_1m = 0u64;
        let mut queries_1h = 0u64;
        let mut queries_24h = 0u64;
        let mut errors_1m = 0u64;
        let mut errors_1h = 0u64;
        let mut errors_24h = 0u64;

        for i in 0..RING_SIZE {
            let ts = now.saturating_sub(i as u64);
            let b = &ring[ts as usize % RING_SIZE];
            if b.ts != ts {
                continue;
            }
            queries_24h += b.queries;
            errors_24h += b.errors;
            if i < 3600 {
                queries_1h += b.queries;
                errors_1h += b.errors;
            }
            if i < 60 {
                queries_1m += b.queries;
                errors_1m += b.errors;
            }
        }

        StatusSnapshot {
            uptime_secs: self.started_at.elapsed().as_secs(),
            rows_loaded: self.rows_loaded,
            queries_1m,
            queries_1h,
            queries_24h,
            errors_1m,
            errors_1h,
            errors_24h,
        }
    }
}
