use axum::Router;
use axum::routing::{get, post};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::api::AppState;
use crate::api::docs::ApiDoc;
use crate::api::handlers;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/status", get(handlers::status))
        .route("/query", post(handlers::query))
        .with_state(state)
        .merge(SwaggerUi::new("/docs").url("/docs.json", ApiDoc::openapi()))
}
