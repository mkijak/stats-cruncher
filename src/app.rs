use std::path::PathBuf;
use std::sync::Arc;

use arc_swap::ArcSwap;
use tokio::net::TcpListener;
use tokio::signal;

use crate::api;
use crate::api::AppState;
use crate::config::AppConfig;
use crate::error::{AppError, AppResult};
use crate::execution::Coordinator;
use crate::ingestion;
use crate::metrics::Metrics;
use crate::reload;

pub struct App {
    config_path: PathBuf,
    cfg: AppConfig,
    state: AppState,
    ingestor: Box<dyn ingestion::Ingestor>,
}

impl App {
    pub async fn bootstrap(config_path: PathBuf, cfg: AppConfig) -> AppResult<Self> {
        let ingestor = ingestion::select(&cfg);
        let (ingestor, store) = reload::bootstrap_reload(ingestor).await?;
        let store = Arc::new(store);
        let rows = store.row_count() as u64;
        let coordinator = Arc::new(ArcSwap::new(Arc::new(Coordinator::start(cfg.clone(), store))));
        let metrics = Arc::new(Metrics::new(rows));
        let state = AppState { coordinator, metrics };
        Ok(Self { config_path, cfg, state, ingestor })
    }

    pub async fn run(self) -> AppResult<()> {
        let App { config_path, cfg, state, ingestor } = self;
        if cfg.engine.reload_interval_mins > 0 {
            tokio::spawn(reload::watcher(
                config_path,
                state.clone(),
                cfg.clone(),
                ingestor,
            ));
        } else {
            drop(ingestor);
        }
        let router = api::router(state);
        let bind = cfg.api.bind.as_str();
        let listener = TcpListener::bind(bind).await.map_err(|e| {
            AppError::Execution(format!("binding HTTP listener to {bind:?}: {e}"))
        })?;
        let local = listener.local_addr().map_err(AppError::Io)?;
        tracing::info!(addr = %local, "HTTP server listening");
        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(|e| AppError::Execution(format!("HTTP server error: {e}")))
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = signal::ctrl_c().await {
            tracing::error!(error = %e, "failed to install Ctrl+C handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("received Ctrl+C, shutting down"),
        _ = terminate => tracing::info!("received SIGTERM, shutting down"),
    }
}
