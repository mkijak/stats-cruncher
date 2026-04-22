use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;

use crate::api::AppState;
use crate::api::dto::{ErrorResponse, QueryRequest, QueryResponse, StatusResponse, WindowCounts};
use crate::error::AppError;
use crate::query::Query;

#[utoipa::path(
    get,
    path = "/status",
    tag = "Stats",
    responses(
        (status = 200, description = "Runtime stats", body = StatusResponse)
    )
)]
pub async fn status(State(state): State<AppState>) -> Json<StatusResponse> {
    let s = state.metrics.snapshot();
    Json(StatusResponse {
        uptime_secs: s.uptime_secs,
        rows_loaded: s.rows_loaded,
        queries: WindowCounts { last_1m: s.queries_1m, last_1h: s.queries_1h, last_24h: s.queries_24h },
        errors: WindowCounts { last_1m: s.errors_1m, last_1h: s.errors_1h, last_24h: s.errors_24h },
    })
}

#[utoipa::path(
    post,
    path = "/query",
    tag = "Stats",
    request_body = QueryRequest,
    responses(
        (status = 200, description = "Query executed successfully", body = QueryResponse),
        (status = 400, description = "Malformed query", body = ErrorResponse),
        (status = 500, description = "Query execution failed", body = ErrorResponse)
    )
)]
pub async fn query(
    State(state): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    let query = Query::try_from(req).map_err(|e| {
        state.metrics.record(true);
        (StatusCode::BAD_REQUEST, Json(ErrorResponse::new(e)))
    })?;
    match state.coordinator.submit(query).await {
        Ok(response) => {
            state.metrics.record(false);
            Ok(Json(response.into()))
        }
        Err(e) => {
            state.metrics.record(true);
            let status = match &e {
                AppError::Query(_) => StatusCode::BAD_REQUEST,
                _ => {
                    tracing::error!(error = %e, "query execution failed");
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            };
            Err((status, Json(ErrorResponse::new(e.to_string()))))
        }
    }
}
