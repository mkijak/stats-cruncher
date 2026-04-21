//! Process-level resource probes. Used at natural checkpoints to fail fast
//! when the app is about to exceed its configured budget — deliberately not a
//! per-allocation tracker, so there's no overhead on the hot path.

use crate::error::{AppError, AppResult};

/// Current process RSS (resident set size) in bytes, as reported by the OS.
/// Returns 0 on platforms where `memory-stats` can't answer (shouldn't happen
/// on Linux/macOS/Windows).
pub fn current_rss_bytes() -> u64 {
    memory_stats::memory_stats()
        .map(|s| s.physical_mem as u64)
        .unwrap_or(0)
}

/// Fail with [`AppError::Storage`] if current RSS exceeds `limit_bytes`.
/// Call at natural checkpoints: ingestion chunk boundaries and query admission.
pub fn check_memory(limit_bytes: u64) -> AppResult<()> {
    let rss = current_rss_bytes();
    if rss > limit_bytes {
        return Err(AppError::Storage(format!(
            "memory limit exceeded: RSS {rss} bytes > limit {limit_bytes} bytes"
        )));
    }
    Ok(())
}
