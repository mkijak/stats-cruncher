use std::sync::Arc;

use axum::Router;
use axum::routing::{get, post};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::api::docs::ApiDoc;
use crate::api::handlers;
use crate::execution::Coordinator;

pub fn router(coordinator: Arc<Coordinator>) -> Router {
    Router::new()
        .route("/health", get(handlers::health))
        .route("/query", post(handlers::query))
        .with_state(coordinator)
        .merge(SwaggerUi::new("/docs").url("/docs.json", ApiDoc::openapi()))
}
