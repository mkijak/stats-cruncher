use std::collections::HashMap;

use roaring::RoaringBitmap;

/// Dictionary-encoded string column index.
///
/// Each distinct string is assigned a stable `u32` code. Alongside the code,
/// a [`RoaringBitmap`] of every row that carried that value is kept. Query-time
/// `must` / `must_not` filters collapse to bitwise operations over those bitmaps.
pub struct StringDictionary {
    codes: HashMap<String, u32>,
    values: Vec<String>,
    postings: Vec<RoaringBitmap>,
}

impl StringDictionary {
    pub fn new() -> Self {
        Self { codes: HashMap::new(), values: Vec::new(), postings: Vec::new() }
    }

    /// map a string and record the row it appeared in. Returns the code.
    pub fn intern(&mut self, value: &str, row: u32) -> u32 {
        if let Some(&code) = self.codes.get(value) {
            self.postings[code as usize].insert(row);
            return code;
        }
        let code = self.values.len() as u32;
        self.codes.insert(value.to_owned(), code);
        self.values.push(value.to_owned());
        let mut bm = RoaringBitmap::new();
        bm.insert(row);
        self.postings.push(bm);
        code
    }

    pub fn code_of(&self, value: &str) -> Option<u32> {
        self.codes.get(value).copied()
    }

    pub fn value_of(&self, code: u32) -> Option<&str> {
        self.values.get(code as usize).map(String::as_str)
    }

    pub fn postings(&self, code: u32) -> Option<&RoaringBitmap> {
        self.postings.get(code as usize)
    }

    pub fn cardinality(&self) -> usize {
        self.values.len()
    }
}

impl Default for StringDictionary {
    fn default() -> Self {
        Self::new()
    }
}
