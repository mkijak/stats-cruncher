use std::ops::Range;
use std::sync::Arc;

use crate::query::{Query, QueryId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChunkId(pub u64);

/// One unit of work: "run this query over this row range of the store".
/// Workers receive these off the task channel and produce a partial result.
#[derive(Debug, Clone)]
pub struct Task {
    pub chunk_id: ChunkId,
    pub query_id: QueryId,
    pub query: Arc<Query>,
    pub rows: Range<usize>,
}
