//! Redis set implementation.
//!
//! Redis sets are unordered collections of unique binary-safe strings.

use bytes::Bytes;
use std::collections::HashSet;

/// A Redis set - unordered collection of unique elements.
///
/// # Implementation
///
/// Uses `HashSet<Bytes>` for O(1) average-case add/remove/contains operations.
/// For small sets, Redis uses an intset or listpack encoding - a future
/// optimization could implement this for memory efficiency.
#[derive(Debug, Clone, Default)]
pub struct ViatorSet {
    inner: HashSet<Bytes>,
}

impl ViatorSet {
    /// Create a new empty set.
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: HashSet::new(),
        }
    }

    /// Create a set with pre-allocated capacity.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: HashSet::with_capacity(capacity),
        }
    }

    /// Returns the number of elements (cardinality).
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if the set is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Add a member to the set (SADD).
    ///
    /// Returns true if the element was newly added.
    #[inline]
    pub fn add(&mut self, value: Bytes) -> bool {
        self.inner.insert(value)
    }

    /// Add multiple members to the set.
    ///
    /// Returns the number of elements actually added.
    pub fn add_multi(&mut self, values: impl IntoIterator<Item = Bytes>) -> usize {
        values
            .into_iter()
            .filter(|v| self.inner.insert(v.clone()))
            .count()
    }

    /// Remove a member from the set (SREM).
    ///
    /// Returns true if the element was present.
    #[inline]
    pub fn remove(&mut self, value: &[u8]) -> bool {
        self.inner.remove(value)
    }

    /// Remove multiple members from the set.
    ///
    /// Returns the number of elements actually removed.
    pub fn remove_multi<'a>(&mut self, values: impl IntoIterator<Item = &'a [u8]>) -> usize {
        values.into_iter().filter(|v| self.inner.remove(*v)).count()
    }

    /// Check if member exists (SISMEMBER).
    #[inline]
    pub fn contains(&self, value: &[u8]) -> bool {
        self.inner.contains(value)
    }

    /// Check multiple members (SMISMEMBER).
    pub fn contains_multi<'a>(&self, values: impl IntoIterator<Item = &'a [u8]>) -> Vec<bool> {
        values.into_iter().map(|v| self.inner.contains(v)).collect()
    }

    /// Get all members (SMEMBERS).
    pub fn members(&self) -> Vec<&Bytes> {
        self.inner.iter().collect()
    }

    /// Get a random member (SRANDMEMBER).
    pub fn random_member(&self) -> Option<&Bytes> {
        use rand::seq::IteratorRandom;
        let mut rng = rand::thread_rng();
        self.inner.iter().choose(&mut rng)
    }

    /// Get multiple random members.
    ///
    /// If count is positive, returns distinct elements.
    /// If count is negative, may return duplicates.
    pub fn random_members(&self, count: i64) -> Vec<&Bytes> {
        use rand::seq::IteratorRandom;
        let mut rng = rand::thread_rng();

        if count == 0 || self.inner.is_empty() {
            return Vec::new();
        }

        if count > 0 {
            // Distinct elements
            let count = (count as usize).min(self.inner.len());
            self.inner.iter().choose_multiple(&mut rng, count)
        } else {
            // May have duplicates
            let count = count.unsigned_abs() as usize;
            (0..count)
                .filter_map(|_| self.inner.iter().choose(&mut rng))
                .collect()
        }
    }

    /// Pop a random member (SPOP).
    pub fn pop(&mut self) -> Option<Bytes> {
        use rand::seq::IteratorRandom;
        let mut rng = rand::thread_rng();

        // Get a random element's clone first
        let elem = self.inner.iter().choose(&mut rng)?.clone();
        self.inner.remove(&elem);
        Some(elem)
    }

    /// Pop multiple random members.
    pub fn pop_multi(&mut self, count: usize) -> Vec<Bytes> {
        use rand::seq::IteratorRandom;
        let mut rng = rand::thread_rng();

        let count = count.min(self.inner.len());
        let elements: Vec<Bytes> = self
            .inner
            .iter()
            .choose_multiple(&mut rng, count)
            .into_iter()
            .cloned()
            .collect();

        for elem in &elements {
            self.inner.remove(elem);
        }

        elements
    }

    /// Move member from this set to another (SMOVE).
    pub fn move_to(&mut self, dest: &mut ViatorSet, member: &[u8]) -> bool {
        if let Some(elem) = self.inner.take(member) {
            dest.inner.insert(elem);
            true
        } else {
            false
        }
    }

    /// Set intersection (SINTER).
    pub fn intersection<'a>(&'a self, other: &'a ViatorSet) -> impl Iterator<Item = &'a Bytes> {
        self.inner.intersection(&other.inner)
    }

    /// Set union (SUNION).
    pub fn union<'a>(&'a self, other: &'a ViatorSet) -> impl Iterator<Item = &'a Bytes> {
        self.inner.union(&other.inner)
    }

    /// Set difference (SDIFF).
    pub fn difference<'a>(&'a self, other: &'a ViatorSet) -> impl Iterator<Item = &'a Bytes> {
        self.inner.difference(&other.inner)
    }

    /// Intersection with multiple sets.
    pub fn intersection_multi(sets: &[&ViatorSet]) -> HashSet<Bytes> {
        if sets.is_empty() {
            return HashSet::new();
        }

        // Start with smallest set for efficiency
        let (smallest_idx, _) = sets
            .iter()
            .enumerate()
            .min_by_key(|(_, s)| s.len())
            .unwrap();

        let mut result: HashSet<Bytes> = sets[smallest_idx].inner.clone();

        for (i, set) in sets.iter().enumerate() {
            if i != smallest_idx {
                result.retain(|elem| set.inner.contains(elem));
            }
        }

        result
    }

    /// Union with multiple sets.
    pub fn union_multi(sets: &[&ViatorSet]) -> HashSet<Bytes> {
        let mut result = HashSet::new();
        for set in sets {
            result.extend(set.inner.iter().cloned());
        }
        result
    }

    /// Difference with multiple sets (elements in first but not in others).
    pub fn difference_multi(sets: &[&ViatorSet]) -> HashSet<Bytes> {
        if sets.is_empty() {
            return HashSet::new();
        }

        let mut result: HashSet<Bytes> = sets[0].inner.clone();

        for set in sets.iter().skip(1) {
            result.retain(|elem| !set.inner.contains(elem));
        }

        result
    }

    /// Iterate over members matching a pattern (SSCAN).
    pub fn scan<'a>(
        &'a self,
        cursor: usize,
        pattern: Option<&'a [u8]>,
        count: usize,
    ) -> (usize, Vec<&'a Bytes>) {
        let mut results = Vec::new();
        let count = count.max(10);

        let members: Vec<_> = self.inner.iter().collect();
        let start = cursor.min(members.len());

        for member in members.iter().skip(start).take(count) {
            if let Some(pat) = pattern {
                if glob_match(pat, member) {
                    results.push(*member);
                }
            } else {
                results.push(*member);
            }
        }

        let next_cursor = if start + count >= members.len() {
            0
        } else {
            start + count
        };

        (next_cursor, results)
    }

    /// Returns an iterator over set members.
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &Bytes> {
        self.inner.iter()
    }

    /// Clear all members.
    #[inline]
    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

/// Simple glob pattern matching.
fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let mut px = 0;
    let mut tx = 0;
    let mut next_px = 0;
    let mut next_tx = 0;

    while tx < text.len() || px < pattern.len() {
        if px < pattern.len() {
            match pattern[px] {
                b'*' => {
                    next_px = px;
                    next_tx = tx + 1;
                    px += 1;
                    continue;
                }
                b'?' if tx < text.len() => {
                    px += 1;
                    tx += 1;
                    continue;
                }
                c if tx < text.len() && c == text[tx] => {
                    px += 1;
                    tx += 1;
                    continue;
                }
                _ => {}
            }
        }

        if next_tx > 0 && next_tx <= text.len() {
            px = next_px;
            tx = next_tx;
            next_tx += 1;
            continue;
        }

        return false;
    }

    true
}

impl FromIterator<Bytes> for ViatorSet {
    fn from_iter<T: IntoIterator<Item = Bytes>>(iter: T) -> Self {
        Self {
            inner: HashSet::from_iter(iter),
        }
    }
}

impl IntoIterator for ViatorSet {
    type Item = Bytes;
    type IntoIter = std::collections::hash_set::IntoIter<Bytes>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_remove() {
        let mut set = ViatorSet::new();
        assert!(set.add(Bytes::from("a")));
        assert!(!set.add(Bytes::from("a"))); // Duplicate
        assert!(set.add(Bytes::from("b")));

        assert_eq!(set.len(), 2);
        assert!(set.contains(b"a"));
        assert!(set.remove(b"a"));
        assert!(!set.contains(b"a"));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_intersection() {
        let mut s1 = ViatorSet::new();
        s1.add(Bytes::from("a"));
        s1.add(Bytes::from("b"));
        s1.add(Bytes::from("c"));

        let mut s2 = ViatorSet::new();
        s2.add(Bytes::from("b"));
        s2.add(Bytes::from("c"));
        s2.add(Bytes::from("d"));

        let inter: Vec<_> = s1.intersection(&s2).collect();
        assert_eq!(inter.len(), 2);
        assert!(inter.contains(&&Bytes::from("b")));
        assert!(inter.contains(&&Bytes::from("c")));
    }

    #[test]
    fn test_union() {
        let mut s1 = ViatorSet::new();
        s1.add(Bytes::from("a"));
        s1.add(Bytes::from("b"));

        let mut s2 = ViatorSet::new();
        s2.add(Bytes::from("b"));
        s2.add(Bytes::from("c"));

        let union: Vec<_> = s1.union(&s2).collect();
        assert_eq!(union.len(), 3);
    }

    #[test]
    fn test_difference() {
        let mut s1 = ViatorSet::new();
        s1.add(Bytes::from("a"));
        s1.add(Bytes::from("b"));
        s1.add(Bytes::from("c"));

        let mut s2 = ViatorSet::new();
        s2.add(Bytes::from("b"));

        let diff: Vec<_> = s1.difference(&s2).collect();
        assert_eq!(diff.len(), 2);
        assert!(diff.contains(&&Bytes::from("a")));
        assert!(diff.contains(&&Bytes::from("c")));
    }

    #[test]
    fn test_random() {
        let mut set = ViatorSet::new();
        set.add(Bytes::from("a"));
        set.add(Bytes::from("b"));
        set.add(Bytes::from("c"));

        // Random member should be one of the elements
        let member = set.random_member().unwrap();
        assert!(set.contains(member));

        // Pop should remove and return an element
        let popped = set.pop().unwrap();
        assert!(!set.contains(&popped));
        assert_eq!(set.len(), 2);
    }
}
