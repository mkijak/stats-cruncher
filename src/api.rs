mod docs;
mod dto;
mod handlers;
mod router;

use std::sync::Arc;
use arc_swap::ArcSwap;

use crate::execution::Coordinator;
use crate::metrics::Metrics;

#[derive(Clone)]
pub struct AppState {
    pub coordinator: Arc<ArcSwap<Coordinator>>,
    pub metrics: Arc<Metrics>,
}

pub use router::router;
