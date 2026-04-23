use crate::query::Response;

/// Accumulates partial results produced by workers for a single query and
/// yields the final response once every chunk has reported in.
pub struct Aggregator {
    expected_chunks: usize,
    received_chunks: usize,
    acc: Response,
}

impl Aggregator {
    pub fn new(expected_chunks: usize) -> Self {
        Self {
            expected_chunks,
            received_chunks: 0,
            acc: Response::default(),
        }
    }

    /// Merge one partial result. Returns `Some(Response)` once all chunks have
    /// been folded in, otherwise `None`.
    pub fn merge(&mut self, partial: PartialResult) -> Option<Response> {
        self.acc.matched_rows += partial.matched_rows;
        for (name, stats) in partial.numeric {
            self.acc
                .numeric
                .entry(name)
                .and_modify(|existing| existing.merge(&stats))
                .or_insert(stats);
        }
        for (name, counts) in partial.string_counts {
            let col_entry = self.acc.string_counts.entry(name).or_default();
            for (value, count) in counts {
                *col_entry.entry(value).or_insert(0) += count;
            }
        }
        self.received_chunks += 1;
        if self.received_chunks >= self.expected_chunks {
            Some(std::mem::take(&mut self.acc))
        } else {
            None
        }
    }
}

/// Per-chunk output from a worker. Shares the shape of [`Response`] — each
/// chunk produces a partial with counts and per-column stats scoped to its
/// row range, and the aggregator folds them together.
pub type PartialResult = Response;
