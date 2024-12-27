use std::ops::{Bound, RangeBounds};

use crate::error::Result;

// Defines an abstract storage engine interface for plugging in different storage engines
// Currently supports in-memory and simple disk-based KV storage.
pub trait Engine {
    type EngineIterator<'a>: EngineIterator
    where
        Self: 'a;

    // Set key/value
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;

    // Fetch key's data
    fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>>;

    // Remove key's data; ignore if key DNE
    fn delete(&mut self, key: Vec<u8>) -> Result<()>;

    // Scanner
    fn scan(&mut self, range: impl RangeBounds<Vec<u8>>) -> Self::EngineIterator<'_>;

    // Scan the prefic
    fn scan_prefix(&mut self, prefix: Vec<u8>) -> Self::EngineIterator<'_> {
        // start: aaaa
        // end:   aaab
        let start = Bound::Included(prefix.clone());
        let mut bound_prefix = prefix.clone();
        if let Some(last) = bound_prefix.iter_mut().last() {
            *last += 1;
        };
        let end = Bound::Excluded(bound_prefix);

        self.scan((start, end))
    }
}

pub trait EngineIterator: DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> {}
