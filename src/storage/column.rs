use crate::storage::dictionary::StringDictionary;

pub type ColumnName = String;

pub enum Column {
    Integer(Vec<i64>),
    Float(Vec<f64>),
    String(StringColumn),
    DateTime(Vec<i64>),
}

impl Column {
    pub fn len(&self) -> usize {
        match self {
            Column::Integer(v) => v.len(),
            Column::Float(v) => v.len(),
            Column::String(sc) => sc.count,
            Column::DateTime(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            Column::Integer(_) => "integer",
            Column::Float(_) => "float",
            Column::String(_) => "string",
            Column::DateTime(_) => "date-time",
        }
    }
}

pub struct StringColumn {
    pub dictionary: StringDictionary,
    pub count: usize,
}

impl StringColumn {
    pub fn new() -> Self {
        Self { dictionary: StringDictionary::new(), count: 0 }
    }
}

impl Default for StringColumn {
    fn default() -> Self {
        Self::new()
    }
}
