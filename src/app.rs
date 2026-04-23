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
}

impl App {
    pub async fn bootstrap(config_path: PathBuf, cfg: AppConfig) -> AppResult<Self> {
        let store = Arc::new(ingestion::run(&cfg).await?);
        let rows = store.row_count() as u64;
        let coordinator = Arc::new(ArcSwap::new(Arc::new(Coordinator::start(cfg.clone(), store))));
        let metrics = Arc::new(Metrics::new(rows));
        let state = AppState { coordinator, metrics };
        Ok(Self { config_path, cfg, state })
    }

    pub async fn run(self) -> AppResult<()> {
        if self.cfg.engine.reload_interval_mins > 0 {
            tokio::spawn(reload::watcher(
                self.config_path.clone(),
                self.state.clone(),
                self.cfg.clone(),
            ));
        }
        let router = api::router(self.state);
        let bind = self.cfg.api.bind.as_str();
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
