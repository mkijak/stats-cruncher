use utoipa::OpenApi;

use crate::api::dto::{
    ColumnStatsDto, ErrorResponse, QueryRequest, QueryResponse, RangeDto, StatusResponse,
    WindowCounts,
};

#[derive(OpenApi)]
#[openapi(
    paths(
        crate::api::handlers::status,
        crate::api::handlers::query,
    ),
    tags(
          (name = "Stats", description = "Stats cruncher endpoints"),
    ),
    components(schemas(
        StatusResponse,
        WindowCounts,
        QueryRequest,
        RangeDto,
        QueryResponse,
        ColumnStatsDto,
        ErrorResponse,
    )),
    info(
        title = "stats-cruncher",
        description = "In-memory OLAP engine HTTP API."
    )
)]
pub struct ApiDoc;
