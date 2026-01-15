//! Redis sorted set implementation.
//!
//! Sorted sets maintain elements ordered by score, with O(log N) operations.

use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

/// Score type for sorted sets (IEEE 754 double).
pub type Score = f64;

/// An entry in a sorted set.
#[derive(Debug, Clone)]
pub struct SortedSetEntry {
    /// The member
    pub member: Bytes,
    /// The score
    pub score: Score,
}

impl SortedSetEntry {
    /// Create a new entry.
    pub fn new(member: Bytes, score: Score) -> Self {
        Self { member, score }
    }
}

/// Wrapper for score that handles NaN and ordering.
#[derive(Debug, Clone, Copy)]
struct OrderedScore(Score);

impl PartialEq for OrderedScore {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for OrderedScore {}

impl PartialOrd for OrderedScore {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedScore {
    fn cmp(&self, other: &Self) -> Ordering {
        // Handle special float values
        match (self.0.is_nan(), other.0.is_nan()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => self.0.partial_cmp(&other.0).unwrap_or(Ordering::Equal),
        }
    }
}

/// Key for the score-ordered BTreeMap: (score, member).
#[derive(Debug, Clone, Eq, PartialEq)]
struct ScoreKey {
    score: OrderedScore,
    member: Bytes,
}

impl Ord for ScoreKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .cmp(&other.score)
            .then_with(|| self.member.cmp(&other.member))
    }
}

impl PartialOrd for ScoreKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A Redis sorted set with O(log N) operations.
///
/// # Implementation
///
/// Uses two data structures for efficient operations:
/// - `HashMap<member, score>` for O(1) score lookup by member
/// - `BTreeMap<(score, member), ()>` for ordered iteration
///
/// This mirrors Redis's skiplist + hashtable implementation but uses
/// Rust's standard library types for safety and maintainability.
#[derive(Debug, Clone, Default)]
pub struct SortedSet {
    /// Member to score mapping
    scores: HashMap<Bytes, Score>,
    /// Score-ordered index
    by_score: BTreeMap<ScoreKey, ()>,
}

impl SortedSet {
    /// Create a new empty sorted set.
    pub fn new() -> Self {
        Self {
            scores: HashMap::new(),
            by_score: BTreeMap::new(),
        }
    }

    /// Returns the number of elements.
    #[inline]
    pub fn len(&self) -> usize {
        self.scores.len()
    }

    /// Returns true if the sorted set is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.scores.is_empty()
    }

    /// Add a member with score (ZADD).
    ///
    /// Returns true if newly added, false if updated.
    pub fn add(&mut self, member: Bytes, score: Score) -> bool {
        if let Some(&old_score) = self.scores.get(&member) {
            if (old_score - score).abs() < f64::EPSILON {
                return false; // Same score, no change
            }
            // Remove old entry from by_score
            self.by_score.remove(&ScoreKey {
                score: OrderedScore(old_score),
                member: member.clone(),
            });
        }

        let is_new = !self.scores.contains_key(&member);
        self.scores.insert(member.clone(), score);
        self.by_score.insert(
            ScoreKey {
                score: OrderedScore(score),
                member,
            },
            (),
        );

        is_new
    }

    /// Add with options (ZADD NX/XX/GT/LT).
    #[allow(clippy::fn_params_excessive_bools)]
    pub fn add_with_options(
        &mut self,
        member: Bytes,
        score: Score,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> (bool, bool) {
        // Returns (added, changed)
        let existing = self.scores.get(&member).copied();

        match existing {
            Some(old_score) if nx => (false, false), // NX: don't update existing
            None if xx => (false, false),            // XX: only update existing
            Some(old_score) => {
                // Check GT/LT conditions
                let should_update = match (gt, lt) {
                    (true, false) => score > old_score,
                    (false, true) => score < old_score,
                    _ => true,
                };

                if should_update && (old_score - score).abs() > f64::EPSILON {
                    self.by_score.remove(&ScoreKey {
                        score: OrderedScore(old_score),
                        member: member.clone(),
                    });
                    self.scores.insert(member.clone(), score);
                    self.by_score.insert(
                        ScoreKey {
                            score: OrderedScore(score),
                            member,
                        },
                        (),
                    );
                    (false, true)
                } else {
                    (false, false)
                }
            }
            None => {
                self.scores.insert(member.clone(), score);
                self.by_score.insert(
                    ScoreKey {
                        score: OrderedScore(score),
                        member,
                    },
                    (),
                );
                (true, false)
            }
        }
    }

    /// Remove a member (ZREM).
    pub fn remove(&mut self, member: &[u8]) -> bool {
        if let Some(score) = self.scores.remove(member) {
            self.by_score.remove(&ScoreKey {
                score: OrderedScore(score),
                member: Bytes::copy_from_slice(member),
            });
            true
        } else {
            false
        }
    }

    /// Remove multiple members.
    pub fn remove_multi<'a>(&mut self, members: impl IntoIterator<Item = &'a [u8]>) -> usize {
        members.into_iter().filter(|m| self.remove(m)).count()
    }

    /// Get the score of a member (ZSCORE).
    #[inline]
    pub fn score(&self, member: &[u8]) -> Option<Score> {
        self.scores.get(member).copied()
    }

    /// Get scores of multiple members (ZMSCORE).
    pub fn scores<'a>(&self, members: impl IntoIterator<Item = &'a [u8]>) -> Vec<Option<Score>> {
        members.into_iter().map(|m| self.score(m)).collect()
    }

    /// Increment score (ZINCRBY).
    pub fn incr(&mut self, member: Bytes, delta: Score) -> Score {
        let new_score = self.scores.get(&member).unwrap_or(&0.0) + delta;
        self.add(member, new_score);
        new_score
    }

    /// Get rank of member (ZRANK, 0-based).
    pub fn rank(&self, member: &[u8]) -> Option<usize> {
        let score = self.scores.get(member)?;
        let key = ScoreKey {
            score: OrderedScore(*score),
            member: Bytes::copy_from_slice(member),
        };

        Some(self.by_score.range(..&key).count())
    }

    /// Get reverse rank (ZREVRANK, 0-based).
    pub fn rev_rank(&self, member: &[u8]) -> Option<usize> {
        let rank = self.rank(member)?;
        Some(self.len() - 1 - rank)
    }

    /// Get range by rank (ZRANGE).
    pub fn range(&self, start: i64, stop: i64) -> Vec<SortedSetEntry> {
        let len = self.len() as i64;
        if len == 0 {
            return Vec::new();
        }

        // Normalize indices
        let start = if start < 0 {
            (len + start).max(0) as usize
        } else {
            (start as usize).min(self.len())
        };

        let stop = if stop < 0 {
            (len + stop).max(0) as usize
        } else {
            (stop as usize).min(self.len().saturating_sub(1))
        };

        if start > stop {
            return Vec::new();
        }

        self.by_score
            .keys()
            .skip(start)
            .take(stop - start + 1)
            .map(|k| SortedSetEntry::new(k.member.clone(), k.score.0))
            .collect()
    }

    /// Get reverse range by rank (ZREVRANGE).
    pub fn rev_range(&self, start: i64, stop: i64) -> Vec<SortedSetEntry> {
        let mut result = self.range(
            -(stop + 1),
            if start == 0 { -1 } else { -(start + 1) },
        );
        result.reverse();
        result
    }

    /// Get range by score (ZRANGEBYSCORE).
    pub fn range_by_score(
        &self,
        min: ScoreBound,
        max: ScoreBound,
        offset: usize,
        count: Option<usize>,
    ) -> Vec<SortedSetEntry> {
        let _min_score = min.value();
        let _max_score = max.value();

        let iter = self.by_score.keys().filter(|k| {
            let s = k.score.0;
            let above_min = match min {
                ScoreBound::Inclusive(v) => s >= v,
                ScoreBound::Exclusive(v) => s > v,
                ScoreBound::NegInf => true,
                ScoreBound::PosInf => false,
            };
            let below_max = match max {
                ScoreBound::Inclusive(v) => s <= v,
                ScoreBound::Exclusive(v) => s < v,
                ScoreBound::NegInf => false,
                ScoreBound::PosInf => true,
            };
            above_min && below_max
        });

        let iter = iter.skip(offset);

        let entries: Vec<_> = if let Some(count) = count {
            iter.take(count)
                .map(|k| SortedSetEntry::new(k.member.clone(), k.score.0))
                .collect()
        } else {
            iter.map(|k| SortedSetEntry::new(k.member.clone(), k.score.0))
                .collect()
        };

        entries
    }

    /// Count elements in score range (ZCOUNT).
    pub fn count_by_score(&self, min: ScoreBound, max: ScoreBound) -> usize {
        self.by_score
            .keys()
            .filter(|k| {
                let s = k.score.0;
                let above_min = match min {
                    ScoreBound::Inclusive(v) => s >= v,
                    ScoreBound::Exclusive(v) => s > v,
                    ScoreBound::NegInf => true,
                    ScoreBound::PosInf => false,
                };
                let below_max = match max {
                    ScoreBound::Inclusive(v) => s <= v,
                    ScoreBound::Exclusive(v) => s < v,
                    ScoreBound::NegInf => false,
                    ScoreBound::PosInf => true,
                };
                above_min && below_max
            })
            .count()
    }

    /// Remove elements by rank range (ZREMRANGEBYRANK).
    pub fn remove_range_by_rank(&mut self, start: i64, stop: i64) -> usize {
        let entries = self.range(start, stop);
        let count = entries.len();

        for entry in entries {
            self.remove(&entry.member);
        }

        count
    }

    /// Remove elements by score range (ZREMRANGEBYSCORE).
    pub fn remove_range_by_score(&mut self, min: ScoreBound, max: ScoreBound) -> usize {
        let entries = self.range_by_score(min, max, 0, None);
        let count = entries.len();

        for entry in entries {
            self.remove(&entry.member);
        }

        count
    }

    /// Pop minimum elements (ZPOPMIN).
    pub fn pop_min(&mut self, count: usize) -> Vec<SortedSetEntry> {
        let mut result = Vec::with_capacity(count.min(self.len()));

        for _ in 0..count {
            if let Some(key) = self.by_score.keys().next().cloned() {
                self.by_score.remove(&key);
                self.scores.remove(&key.member);
                result.push(SortedSetEntry::new(key.member, key.score.0));
            } else {
                break;
            }
        }

        result
    }

    /// Pop maximum elements (ZPOPMAX).
    pub fn pop_max(&mut self, count: usize) -> Vec<SortedSetEntry> {
        let mut result = Vec::with_capacity(count.min(self.len()));

        for _ in 0..count {
            if let Some(key) = self.by_score.keys().next_back().cloned() {
                self.by_score.remove(&key);
                self.scores.remove(&key.member);
                result.push(SortedSetEntry::new(key.member, key.score.0));
            } else {
                break;
            }
        }

        result
    }

    /// Get random members (ZRANDMEMBER).
    pub fn random_members(&self, count: i64, _with_scores: bool) -> Vec<SortedSetEntry> {
        use rand::seq::IteratorRandom;
        let mut rng = rand::thread_rng();

        if count == 0 || self.is_empty() {
            return Vec::new();
        }

        if count > 0 {
            let count = (count as usize).min(self.len());
            self.by_score
                .keys()
                .choose_multiple(&mut rng, count)
                .into_iter()
                .map(|k| SortedSetEntry::new(k.member.clone(), k.score.0))
                .collect()
        } else {
            let count = count.unsigned_abs() as usize;
            (0..count)
                .filter_map(|_| {
                    self.by_score
                        .keys()
                        .choose(&mut rng)
                        .map(|k| SortedSetEntry::new(k.member.clone(), k.score.0))
                })
                .collect()
        }
    }

    /// Estimate memory usage.
    pub fn memory_usage(&self) -> usize {
        let base = size_of::<Self>();
        let scores_overhead = self.scores.capacity() * (size_of::<Bytes>() + 8);
        let btree_overhead = self.by_score.len() * size_of::<ScoreKey>();
        let member_data: usize = self.scores.keys().map(|k| k.len()).sum();

        base + scores_overhead + btree_overhead + member_data * 2
    }

    /// Iterate over entries in score order.
    pub fn iter(&self) -> impl Iterator<Item = SortedSetEntry> + '_ {
        self.by_score
            .keys()
            .map(|k| SortedSetEntry::new(k.member.clone(), k.score.0))
    }

    /// Clear all entries.
    pub fn clear(&mut self) {
        self.scores.clear();
        self.by_score.clear();
    }
}

/// Score bound for range queries.
#[derive(Debug, Clone, Copy)]
pub enum ScoreBound {
    /// Inclusive bound
    Inclusive(Score),
    /// Exclusive bound
    Exclusive(Score),
    /// Negative infinity
    NegInf,
    /// Positive infinity
    PosInf,
}

impl ScoreBound {
    /// Get the score value.
    pub fn value(&self) -> Score {
        match self {
            Self::Inclusive(v) | Self::Exclusive(v) => *v,
            Self::NegInf => Score::NEG_INFINITY,
            Self::PosInf => Score::INFINITY,
        }
    }

    /// Parse Redis score bound syntax (e.g., "(1.5", "1.5", "-inf", "+inf").
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();

        if s.eq_ignore_ascii_case("-inf") {
            return Some(Self::NegInf);
        }
        if s.eq_ignore_ascii_case("+inf") || s.eq_ignore_ascii_case("inf") {
            return Some(Self::PosInf);
        }

        if let Some(rest) = s.strip_prefix('(') {
            rest.parse().ok().map(Self::Exclusive)
        } else {
            s.parse().ok().map(Self::Inclusive)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_score() {
        let mut zset = SortedSet::new();
        assert!(zset.add(Bytes::from("a"), 1.0));
        assert!(zset.add(Bytes::from("b"), 2.0));
        assert!(!zset.add(Bytes::from("a"), 1.5)); // Update

        assert_eq!(zset.len(), 2);
        assert_eq!(zset.score(b"a"), Some(1.5));
        assert_eq!(zset.score(b"b"), Some(2.0));
    }

    #[test]
    fn test_rank() {
        let mut zset = SortedSet::new();
        zset.add(Bytes::from("a"), 1.0);
        zset.add(Bytes::from("b"), 2.0);
        zset.add(Bytes::from("c"), 3.0);

        assert_eq!(zset.rank(b"a"), Some(0));
        assert_eq!(zset.rank(b"b"), Some(1));
        assert_eq!(zset.rank(b"c"), Some(2));
        assert_eq!(zset.rank(b"d"), None);

        assert_eq!(zset.rev_rank(b"a"), Some(2));
        assert_eq!(zset.rev_rank(b"c"), Some(0));
    }

    #[test]
    fn test_range() {
        let mut zset = SortedSet::new();
        zset.add(Bytes::from("a"), 1.0);
        zset.add(Bytes::from("b"), 2.0);
        zset.add(Bytes::from("c"), 3.0);

        let range = zset.range(0, 1);
        assert_eq!(range.len(), 2);
        assert_eq!(range[0].member, Bytes::from("a"));
        assert_eq!(range[1].member, Bytes::from("b"));

        let range = zset.range(-2, -1);
        assert_eq!(range.len(), 2);
        assert_eq!(range[0].member, Bytes::from("b"));
        assert_eq!(range[1].member, Bytes::from("c"));
    }

    #[test]
    fn test_range_by_score() {
        let mut zset = SortedSet::new();
        zset.add(Bytes::from("a"), 1.0);
        zset.add(Bytes::from("b"), 2.0);
        zset.add(Bytes::from("c"), 3.0);

        let range = zset.range_by_score(
            ScoreBound::Inclusive(1.5),
            ScoreBound::Inclusive(2.5),
            0,
            None,
        );
        assert_eq!(range.len(), 1);
        assert_eq!(range[0].member, Bytes::from("b"));

        let range =
            zset.range_by_score(ScoreBound::Exclusive(1.0), ScoreBound::PosInf, 0, None);
        assert_eq!(range.len(), 2);
    }

    #[test]
    fn test_incr() {
        let mut zset = SortedSet::new();
        assert_eq!(zset.incr(Bytes::from("a"), 5.0), 5.0);
        assert_eq!(zset.incr(Bytes::from("a"), 3.0), 8.0);
        assert_eq!(zset.incr(Bytes::from("a"), -2.0), 6.0);
    }

    #[test]
    fn test_pop() {
        let mut zset = SortedSet::new();
        zset.add(Bytes::from("a"), 1.0);
        zset.add(Bytes::from("b"), 2.0);
        zset.add(Bytes::from("c"), 3.0);

        let min = zset.pop_min(1);
        assert_eq!(min.len(), 1);
        assert_eq!(min[0].member, Bytes::from("a"));
        assert_eq!(zset.len(), 2);

        let max = zset.pop_max(1);
        assert_eq!(max.len(), 1);
        assert_eq!(max[0].member, Bytes::from("c"));
        assert_eq!(zset.len(), 1);
    }
}
