//! Redis list implementation.
//!
//! Redis lists are implemented as a VecDeque for O(1) push/pop operations
//! on both ends, matching Redis's quicklist semantics.

use bytes::Bytes;
use std::collections::VecDeque;
use std::iter::FromIterator;

/// A Redis list with O(1) operations on both ends.
///
/// # Implementation Notes
///
/// Redis uses a "quicklist" (linked list of ziplists) for memory efficiency.
/// We use `VecDeque` which provides similar O(1) head/tail operations with
/// better cache locality for most workloads.
///
/// For very large lists, a future optimization could implement chunked storage.
#[derive(Debug, Clone, Default)]
pub struct ViatorList {
    inner: VecDeque<Bytes>,
}

impl ViatorList {
    /// Create a new empty list.
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: VecDeque::new(),
        }
    }

    /// Create a list with pre-allocated capacity.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: VecDeque::with_capacity(capacity),
        }
    }

    /// Returns the number of elements in the list.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if the list is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Push an element to the front of the list (LPUSH).
    #[inline]
    pub fn push_front(&mut self, value: Bytes) {
        self.inner.push_front(value);
    }

    /// Push an element to the back of the list (RPUSH).
    #[inline]
    pub fn push_back(&mut self, value: Bytes) {
        self.inner.push_back(value);
    }

    /// Push multiple elements to the front (LPUSH with multiple values).
    pub fn push_front_multi(&mut self, values: impl IntoIterator<Item = Bytes>) {
        for value in values {
            self.inner.push_front(value);
        }
    }

    /// Push multiple elements to the back (RPUSH with multiple values).
    pub fn push_back_multi(&mut self, values: impl IntoIterator<Item = Bytes>) {
        self.inner.extend(values);
    }

    /// Pop an element from the front (LPOP).
    #[inline]
    pub fn pop_front(&mut self) -> Option<Bytes> {
        self.inner.pop_front()
    }

    /// Pop an element from the back (RPOP).
    #[inline]
    pub fn pop_back(&mut self) -> Option<Bytes> {
        self.inner.pop_back()
    }

    /// Pop multiple elements from the front.
    pub fn pop_front_multi(&mut self, count: usize) -> Vec<Bytes> {
        let count = count.min(self.inner.len());
        (0..count).filter_map(|_| self.inner.pop_front()).collect()
    }

    /// Pop multiple elements from the back.
    pub fn pop_back_multi(&mut self, count: usize) -> Vec<Bytes> {
        let count = count.min(self.inner.len());
        (0..count).filter_map(|_| self.inner.pop_back()).collect()
    }

    /// Get element at index (LINDEX).
    ///
    /// Supports negative indices: -1 is last element, -2 is second to last, etc.
    pub fn get(&self, index: i64) -> Option<&Bytes> {
        let idx = self.normalize_index(index)?;
        self.inner.get(idx)
    }

    /// Set element at index (LSET).
    ///
    /// Returns false if index is out of range.
    pub fn set(&mut self, index: i64, value: Bytes) -> bool {
        if let Some(idx) = self.normalize_index(index) {
            if let Some(elem) = self.inner.get_mut(idx) {
                *elem = value;
                return true;
            }
        }
        false
    }

    /// Get a range of elements (LRANGE).
    ///
    /// Supports negative indices.
    pub fn range(&self, start: i64, stop: i64) -> Vec<&Bytes> {
        let len = self.inner.len() as i64;
        if len == 0 {
            return Vec::new();
        }

        // Normalize start index
        let start = if start < 0 {
            (len + start).max(0) as usize
        } else {
            (start as usize).min(self.inner.len())
        };

        // Normalize stop index (inclusive)
        let stop = if stop < 0 {
            (len + stop).max(0) as usize
        } else {
            (stop as usize).min(self.inner.len().saturating_sub(1))
        };

        if start > stop || start >= self.inner.len() {
            return Vec::new();
        }

        self.inner.range(start..=stop).collect()
    }

    /// Trim list to specified range (LTRIM).
    pub fn trim(&mut self, start: i64, stop: i64) {
        let len = self.inner.len() as i64;
        if len == 0 {
            return;
        }

        // Normalize indices
        let start = if start < 0 {
            (len + start).max(0) as usize
        } else {
            start as usize
        };

        let stop = if stop < 0 {
            (len + stop).max(-1) as usize
        } else {
            stop as usize
        };

        if start > stop || start >= self.inner.len() {
            self.inner.clear();
            return;
        }

        let stop = stop.min(self.inner.len() - 1);

        // Remove elements after stop
        self.inner.truncate(stop + 1);

        // Remove elements before start
        self.inner.drain(..start);
    }

    /// Insert element before or after pivot (LINSERT).
    ///
    /// Returns the new length, or -1 if pivot not found, or 0 if list is empty.
    pub fn insert(&mut self, pivot: &[u8], value: Bytes, before: bool) -> i64 {
        if self.inner.is_empty() {
            return 0;
        }

        for (i, elem) in self.inner.iter().enumerate() {
            if elem.as_ref() == pivot {
                let idx = if before { i } else { i + 1 };
                self.inner.insert(idx, value);
                return self.inner.len() as i64;
            }
        }

        -1
    }

    /// Remove count occurrences of value (LREM).
    ///
    /// - count > 0: Remove first count occurrences
    /// - count < 0: Remove last count occurrences
    /// - count = 0: Remove all occurrences
    ///
    /// Returns number of removed elements.
    pub fn remove(&mut self, count: i64, value: &[u8]) -> usize {
        let mut removed = 0;
        let target_count = count.unsigned_abs() as usize;

        if count >= 0 {
            // Remove from head
            let mut i = 0;
            while i < self.inner.len() {
                if self.inner[i].as_ref() == value {
                    self.inner.remove(i);
                    removed += 1;
                    if count != 0 && removed >= target_count {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
        } else {
            // Remove from tail
            let mut i = self.inner.len();
            while i > 0 {
                i -= 1;
                if self.inner[i].as_ref() == value {
                    self.inner.remove(i);
                    removed += 1;
                    if removed >= target_count {
                        break;
                    }
                }
            }
        }

        removed
    }

    /// Move element from one list to another (LMOVE).
    pub fn move_to(
        &mut self,
        dest: &mut ViatorList,
        from_left: bool,
        to_left: bool,
    ) -> Option<Bytes> {
        let value = if from_left {
            self.pop_front()?
        } else {
            self.pop_back()?
        };

        let result = value.clone();

        if to_left {
            dest.push_front(value);
        } else {
            dest.push_back(value);
        }

        Some(result)
    }

    /// Get position of element (LPOS).
    ///
    /// Returns first matching index, or None if not found.
    pub fn position(&self, value: &[u8], rank: i64, count: usize, max_len: usize) -> Vec<usize> {
        let mut matches = Vec::new();
        let max_len = if max_len == 0 {
            self.inner.len()
        } else {
            max_len.min(self.inner.len())
        };

        if rank >= 0 {
            // Search from head
            let mut skip = (rank as usize).saturating_sub(1);
            for (i, elem) in self.inner.iter().enumerate().take(max_len) {
                if elem.as_ref() == value {
                    if skip > 0 {
                        skip -= 1;
                    } else {
                        matches.push(i);
                        if count > 0 && matches.len() >= count {
                            break;
                        }
                    }
                }
            }
        } else {
            // Search from tail
            let mut skip = (rank.unsigned_abs() as usize).saturating_sub(1);
            let start = self.inner.len().saturating_sub(max_len);
            for (i, elem) in self.inner.iter().enumerate().skip(start).rev() {
                if elem.as_ref() == value {
                    if skip > 0 {
                        skip -= 1;
                    } else {
                        matches.push(i);
                        if count > 0 && matches.len() >= count {
                            break;
                        }
                    }
                }
            }
        }

        matches
    }

    /// Estimate memory usage in bytes.
    pub fn memory_usage(&self) -> usize {
        size_of::<Self>()
            + self.inner.capacity() * size_of::<Bytes>()
            + self.inner.iter().map(|b| b.len()).sum::<usize>()
    }

    /// Returns an iterator over the list elements.
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &Bytes> {
        self.inner.iter()
    }

    /// Clear all elements from the list.
    #[inline]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// Normalize a possibly-negative index to a positive index.
    fn normalize_index(&self, index: i64) -> Option<usize> {
        let len = self.inner.len() as i64;
        if len == 0 {
            return None;
        }

        let idx = if index < 0 { len + index } else { index };

        if idx < 0 || idx >= len {
            None
        } else {
            Some(idx as usize)
        }
    }
}

impl FromIterator<Bytes> for ViatorList {
    fn from_iter<T: IntoIterator<Item = Bytes>>(iter: T) -> Self {
        Self {
            inner: VecDeque::from_iter(iter),
        }
    }
}

impl IntoIterator for ViatorList {
    type Item = Bytes;
    type IntoIter = std::collections::vec_deque::IntoIter<Bytes>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_pop() {
        let mut list = ViatorList::new();
        list.push_back(Bytes::from("a"));
        list.push_back(Bytes::from("b"));
        list.push_front(Bytes::from("c"));

        assert_eq!(list.len(), 3);
        assert_eq!(list.pop_front(), Some(Bytes::from("c")));
        assert_eq!(list.pop_back(), Some(Bytes::from("b")));
        assert_eq!(list.pop_front(), Some(Bytes::from("a")));
        assert!(list.is_empty());
    }

    #[test]
    fn test_index() {
        let mut list = ViatorList::new();
        list.push_back(Bytes::from("a"));
        list.push_back(Bytes::from("b"));
        list.push_back(Bytes::from("c"));

        assert_eq!(list.get(0), Some(&Bytes::from("a")));
        assert_eq!(list.get(2), Some(&Bytes::from("c")));
        assert_eq!(list.get(-1), Some(&Bytes::from("c")));
        assert_eq!(list.get(-3), Some(&Bytes::from("a")));
        assert_eq!(list.get(3), None);
        assert_eq!(list.get(-4), None);
    }

    #[test]
    fn test_range() {
        let mut list = ViatorList::new();
        for c in b"abcde" {
            list.push_back(Bytes::from(vec![*c]));
        }

        let range: Vec<_> = list.range(0, 2).into_iter().map(|b| b.as_ref()).collect();
        assert_eq!(range, vec![b"a", b"b", b"c"]);

        let range: Vec<_> = list.range(-3, -1).into_iter().map(|b| b.as_ref()).collect();
        assert_eq!(range, vec![b"c", b"d", b"e"]);

        let range: Vec<_> = list.range(0, -1).into_iter().map(|b| b.as_ref()).collect();
        assert_eq!(range, vec![b"a", b"b", b"c", b"d", b"e"]);
    }

    #[test]
    fn test_trim() {
        let mut list = ViatorList::new();
        for c in b"abcde" {
            list.push_back(Bytes::from(vec![*c]));
        }

        list.trim(1, 3);
        assert_eq!(list.len(), 3);
        assert_eq!(list.get(0), Some(&Bytes::from("b")));
        assert_eq!(list.get(2), Some(&Bytes::from("d")));
    }

    #[test]
    fn test_remove() {
        let mut list = ViatorList::new();
        list.push_back(Bytes::from("a"));
        list.push_back(Bytes::from("b"));
        list.push_back(Bytes::from("a"));
        list.push_back(Bytes::from("c"));
        list.push_back(Bytes::from("a"));

        // Remove first 2 'a's
        assert_eq!(list.remove(2, b"a"), 2);
        assert_eq!(list.len(), 3);

        // Remaining: b, c, a
        assert_eq!(list.get(0), Some(&Bytes::from("b")));
        assert_eq!(list.get(2), Some(&Bytes::from("a")));
    }

    #[test]
    fn test_insert() {
        let mut list = ViatorList::new();
        list.push_back(Bytes::from("a"));
        list.push_back(Bytes::from("c"));

        assert_eq!(list.insert(b"c", Bytes::from("b"), true), 3);
        assert_eq!(list.get(1), Some(&Bytes::from("b")));

        assert_eq!(list.insert(b"x", Bytes::from("y"), true), -1);
    }
}
