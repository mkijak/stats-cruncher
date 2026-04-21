use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;

use crate::api::dto::{ErrorResponse, HealthResponse, QueryRequest, QueryResponse};
use crate::error::AppError;
use crate::execution::Coordinator;
use crate::query::Query;

#[utoipa::path(
    get,
    path = "/health",
    responses(
        (status = 200, description = "Service is ready to accept queries", body = HealthResponse)
    )
)]
pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse::ok())
}

#[utoipa::path(
    post,
    path = "/query",
    request_body = QueryRequest,
    responses(
        (status = 200, description = "Query executed successfully", body = QueryResponse),
        (status = 400, description = "Malformed query", body = ErrorResponse),
        (status = 500, description = "Query execution failed", body = ErrorResponse)
    )
)]
pub async fn query(
    State(coordinator): State<Arc<Coordinator>>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    let query = Query::try_from(req).map_err(|e| {
        (StatusCode::BAD_REQUEST, Json(ErrorResponse::new(e)))
    })?;
    let response = coordinator.submit(query).await.map_err(|e| {
        let status = match &e {
            AppError::Query(_) => StatusCode::BAD_REQUEST,
            _ => {
                tracing::error!(error = %e, "query execution failed");
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        (status, Json(ErrorResponse::new(e.to_string())))
    })?;
    Ok(Json(response.into()))
}
