use clap::Parser;
use tracing_subscriber::EnvFilter;

use stats_cruncher::app::App;
use stats_cruncher::config;
use stats_cruncher::error::AppError;

#[derive(Parser, Debug)]
#[command(name = "stats-cruncher")]
struct Cli {
    #[arg(long)]
    config: std::path::PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    init_tracing();
    let cli = Cli::parse();
    let cfg = config::load(&cli.config).map_err(|e| {
        tracing::error!(path = %cli.config.display(), error = %e, "failed to load config");
        e
    })?;
    let app = App::bootstrap(cfg).await.map_err(|e| {
        tracing::error!(error = %e, "failed to bootstrap app");
        e
    })?;
    app.run().await.map_err(|e| {
        tracing::error!(error = %e, "server exited with error");
        e
    })
}

/// Filtering follows `RUST_LOG` (defaults to `info`). Setting
/// `RUST_LOG_FORMAT=json` swaps to line-delimited JSON for log shippers.
fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));
    let json = std::env::var("RUST_LOG_FORMAT")
        .map(|v| v.eq_ignore_ascii_case("json"))
        .unwrap_or(false);
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);
    if json {
        builder.json().init();
    } else {
        builder.compact().init();
    }
}
