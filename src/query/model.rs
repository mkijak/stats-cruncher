use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueryId(pub u64);

#[derive(Debug, Clone)]
pub struct Query {
    pub must: Must,
    pub must_not: MustNot,
    pub ranges: BTreeMap<String, RangeFilter>,
}

#[derive(Debug, Clone, Default)]
pub struct Must(pub BTreeMap<String, StringMatch>);

#[derive(Debug, Clone, Default)]
pub struct MustNot(pub BTreeMap<String, StringMatch>);

#[derive(Debug, Clone)]
pub struct StringMatch {
    pub values: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct RangeFilter {
    pub lower: Option<NumericBound>,
    pub upper: Option<NumericBound>,
}

#[derive(Debug, Clone, Copy)]
pub struct NumericBound {
    pub value: f64,
    pub comparison: Comparison,
}

#[derive(Debug, Clone, Copy)]
pub enum Comparison {
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Debug, Clone, Default)]
pub struct Response {
    pub matched_rows: u64,
    pub numeric: BTreeMap<String, NumericStats>,
}

#[derive(Debug, Clone, Copy)]
pub struct NumericStats {
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
}

impl NumericStats {
    pub fn empty() -> Self {
        Self { count: 0, sum: 0.0, min: f64::INFINITY, max: f64::NEG_INFINITY }
    }

    pub fn observe(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
    }

    pub fn merge(&mut self, other: &NumericStats) {
        self.count += other.count;
        self.sum += other.sum;
        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }
    }
}

impl Default for NumericStats {
    fn default() -> Self {
        Self::empty()
    }
}
