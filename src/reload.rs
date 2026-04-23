use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::api::AppState;
use crate::config::{self, AppConfig, SourceConfig};
use crate::error::AppResult;
use crate::execution::Coordinator;
use crate::ingestion;

pub async fn watcher(config_path: PathBuf, state: AppState, initial_cfg: AppConfig) {
    let interval_secs = initial_cfg.engine.reload_interval_mins as u64 * 60;

    let mut cfg = initial_cfg;
    let mut last_cfg_mtime = mtime(&config_path);
    let mut last_data_mtime = mtime(data_path(&cfg));
    // Mtimes seen at the previous poll, set when a change is first detected.
    let mut pending: Option<(Option<SystemTime>, Option<SystemTime>)> = None;
    let mut sleep_secs = interval_secs;

    loop {
        tokio::time::sleep(Duration::from_secs(sleep_secs)).await;

        let curr_cfg_mtime = mtime(&config_path);
        let curr_data_mtime = mtime(data_path(&cfg));

        if curr_cfg_mtime.is_none() || curr_data_mtime.is_none() {
            tracing::warn!("reload skipped: one or more watched files are missing");
            pending = None;
            sleep_secs = interval_secs;
            continue;
        }

        let any_changed =
            curr_cfg_mtime != last_cfg_mtime || curr_data_mtime != last_data_mtime;

        if !any_changed {
            pending = None;
            sleep_secs = interval_secs;
            continue;
        }

        // A change was detected. Check if it matches the snapshot from the previous poll.
        let stable = pending
            .as_ref()
            .is_some_and(|(pc, pd)| *pc == curr_cfg_mtime && *pd == curr_data_mtime);

        if stable {
            tracing::info!("detected stable file change, reloading");
            match reload(&config_path, &state, &mut cfg).await {
                Ok(rows) => {
                    last_cfg_mtime = curr_cfg_mtime;
                    last_data_mtime = mtime(data_path(&cfg));
                    state.metrics.set_rows_loaded(rows);
                    tracing::info!(rows, "reload complete");
                }
                Err(e) => {
                    tracing::error!(error = %e, "reload failed, keeping current state");
                }
            }
            pending = None;
            sleep_secs = interval_secs;
        } else {
            // First detection or still changing — record mtimes and retry in 1 minute.
            tracing::debug!("file change detected, waiting for writes to finish");
            pending = Some((curr_cfg_mtime, curr_data_mtime));
            sleep_secs = 60;
        }
    }
}

async fn reload(config_path: &Path, state: &AppState, cfg: &mut AppConfig) -> AppResult<u64> {
    let new_cfg = config::load(config_path)?;
    let store = Arc::new(ingestion::run(&new_cfg).await?);
    let rows = store.row_count() as u64;
    state.coordinator.store(Arc::new(Coordinator::start(new_cfg.clone(), store)));
    *cfg = new_cfg;
    Ok(rows)
}

fn data_path(cfg: &AppConfig) -> &Path {
    match &cfg.source {
        SourceConfig::Csv { path, .. } => path,
        SourceConfig::Sqlite { path, .. } => path,
    }
}

fn mtime(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path).ok()?.modified().ok()
}
