//! Sorted set command implementations.

use super::ParsedCommand;
use crate::Result;
use crate::error::CommandError;
use crate::protocol::Frame;
use crate::server::ClientState;
use crate::storage::Db;
use crate::types::{Key, ValueType, ViatorValue, sorted_set::ScoreBound};
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

fn get_or_create_zset(db: &Db, key: &Key) -> Result<ViatorValue> {
    match db.get(key) {
        Some(v) if v.is_zset() => Ok(v),
        Some(_) => Err(CommandError::WrongType.into()),
        None => Ok(ViatorValue::new_zset()),
    }
}

/// ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]
pub fn cmd_zadd(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());

        let mut nx = false;
        let mut xx = false;
        let mut gt = false;
        let mut lt = false;
        let mut ch = false;
        let mut incr = false;

        let mut i = 1;

        // Parse options
        while i < cmd.args.len() {
            let opt = std::str::from_utf8(&cmd.args[i])
                .map_err(|_| CommandError::SyntaxError)?
                .to_uppercase();

            match opt.as_str() {
                "NX" => nx = true,
                "XX" => xx = true,
                "GT" => gt = true,
                "LT" => lt = true,
                "CH" => ch = true,
                "INCR" => incr = true,
                _ => break,
            }
            i += 1;
        }

        // Remaining args are score-member pairs
        let pairs = &cmd.args[i..];
        if pairs.len() % 2 != 0 || pairs.is_empty() {
            return Err(CommandError::SyntaxError.into());
        }

        // INCR mode only accepts single pair
        if incr && pairs.len() > 2 {
            return Err(CommandError::SyntaxError.into());
        }

        let value = get_or_create_zset(&db, &key)?;
        let zset = value
            .as_zset()
            .expect("type guaranteed by get_or_create_zset");
        let mut zset = zset.write();

        let mut added = 0;
        let mut changed = 0;

        for chunk in pairs.chunks(2) {
            let score_str = std::str::from_utf8(&chunk[0]).map_err(|_| CommandError::NotFloat)?;
            let score: f64 = score_str.parse().map_err(|_| CommandError::NotFloat)?;
            let member = chunk[1].clone();

            if incr {
                let new_score = zset.incr(member, score);
                drop(zset);
                db.set(key, value);
                return Ok(Frame::Bulk(Bytes::from(new_score.to_string())));
            }

            let (is_added, is_changed) = zset.add_with_options(member, score, nx, xx, gt, lt);
            if is_added {
                added += 1;
            }
            if is_changed {
                changed += 1;
            }
        }

        drop(zset);
        db.set(key, value);

        Ok(Frame::Integer(if ch { added + changed } else { added }))
    })
}

/// ZREM key member [member ...]
pub fn cmd_zrem(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());

        let value = match db.get_typed(&key, ValueType::ZSet)? {
            Some(v) => v,
            None => return Ok(Frame::Integer(0)),
        };

        let zset = value
            .as_zset()
            .expect("type guaranteed by get_or_create_zset");
        let mut zset_guard = zset.write();
        let removed = zset_guard.remove_multi(cmd.args[1..].iter().map(|b| b.as_ref()));
        let is_empty = zset_guard.is_empty();
        drop(zset_guard);

        // Auto-delete empty zset (Redis behavior)
        if is_empty {
            db.delete(&key);
        }

        Ok(Frame::Integer(removed as i64))
    })
}

/// ZSCORE key member
pub fn cmd_zscore(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let member = &cmd.args[1];

        let score = match db.get_typed(&key, ValueType::ZSet)? {
            Some(v) => v
                .as_zset()
                .expect("type guaranteed by get_or_create_zset")
                .read()
                .score(member),
            None => None,
        };

        match score {
            Some(s) => Ok(Frame::Bulk(Bytes::from(s.to_string()))),
            None => Ok(Frame::Null),
        }
    })
}

/// ZRANK key member
pub fn cmd_zrank(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let member = &cmd.args[1];

        let rank = match db.get_typed(&key, ValueType::ZSet)? {
            Some(v) => v
                .as_zset()
                .expect("type guaranteed by get_or_create_zset")
                .read()
                .rank(member),
            None => None,
        };

        match rank {
            Some(r) => Ok(Frame::Integer(r as i64)),
            None => Ok(Frame::Null),
        }
    })
}

/// ZREVRANK key member
pub fn cmd_zrevrank(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let member = &cmd.args[1];

        let rank = match db.get_typed(&key, ValueType::ZSet)? {
            Some(v) => v
                .as_zset()
                .expect("type guaranteed by get_or_create_zset")
                .read()
                .rev_rank(member),
            None => None,
        };

        match rank {
            Some(r) => Ok(Frame::Integer(r as i64)),
            None => Ok(Frame::Null),
        }
    })
}

/// ZCARD key
pub fn cmd_zcard(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());

        let len = match db.get_typed(&key, ValueType::ZSet)? {
            Some(v) => v
                .as_zset()
                .expect("type guaranteed by get_or_create_zset")
                .read()
                .len(),
            None => 0,
        };

        Ok(Frame::Integer(len as i64))
    })
}

/// ZCOUNT key min max
pub fn cmd_zcount(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let min = ScoreBound::parse(cmd.get_str(1)?).ok_or(CommandError::SyntaxError)?;
        let max = ScoreBound::parse(cmd.get_str(2)?).ok_or(CommandError::SyntaxError)?;

        let count = match db.get_typed(&key, ValueType::ZSet)? {
            Some(v) => v
                .as_zset()
                .expect("type guaranteed by get_or_create_zset")
                .read()
                .count_by_score(min, max),
            None => 0,
        };

        Ok(Frame::Integer(count as i64))
    })
}

/// ZRANGE key start stop [WITHSCORES]
pub fn cmd_zrange(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let start = cmd.get_i64(1)?;
        let stop = cmd.get_i64(2)?;
        let with_scores = cmd
            .args
            .get(3)
            .map(|a| {
                std::str::from_utf8(a)
                    .map(|s| s.eq_ignore_ascii_case("WITHSCORES"))
                    .unwrap_or(false)
            })
            .unwrap_or(false);

        let entries = match db.get_typed(&key, ValueType::ZSet)? {
            Some(v) => v
                .as_zset()
                .expect("type guaranteed by get_or_create_zset")
                .read()
                .range(start, stop),
            None => vec![],
        };

        let frames = if with_scores {
            entries
                .into_iter()
                .flat_map(|e| {
                    vec![
                        Frame::Bulk(e.member),
                        Frame::Bulk(Bytes::from(e.score.to_string())),
                    ]
                })
                .collect()
        } else {
            entries.into_iter().map(|e| Frame::Bulk(e.member)).collect()
        };

        Ok(Frame::Array(frames))
    })
}

/// ZREVRANGE key start stop [WITHSCORES]
pub fn cmd_zrevrange(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let start = cmd.get_i64(1)?;
        let stop = cmd.get_i64(2)?;
        let with_scores = cmd
            .args
            .get(3)
            .map(|a| {
                std::str::from_utf8(a)
                    .map(|s| s.eq_ignore_ascii_case("WITHSCORES"))
                    .unwrap_or(false)
            })
            .unwrap_or(false);

        let entries = match db.get_typed(&key, ValueType::ZSet)? {
            Some(v) => v
                .as_zset()
                .expect("type guaranteed by get_or_create_zset")
                .read()
                .rev_range(start, stop),
            None => vec![],
        };

        let frames = if with_scores {
            entries
                .into_iter()
                .flat_map(|e| {
                    vec![
                        Frame::Bulk(e.member),
                        Frame::Bulk(Bytes::from(e.score.to_string())),
                    ]
                })
                .collect()
        } else {
            entries.into_iter().map(|e| Frame::Bulk(e.member)).collect()
        };

        Ok(Frame::Array(frames))
    })
}

/// ZINCRBY key increment member
pub fn cmd_zincrby(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let increment = cmd.get_f64(1)?;
        let member = cmd.args[2].clone();

        let value = get_or_create_zset(&db, &key)?;
        let zset = value
            .as_zset()
            .expect("type guaranteed by get_or_create_zset");
        let new_score = zset.write().incr(member, increment);

        db.set(key, value);
        Ok(Frame::Bulk(Bytes::from(new_score.to_string())))
    })
}

/// ZPOPMIN key [count]
pub fn cmd_zpopmin(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let count = if cmd.args.len() > 1 {
            cmd.get_u64(1)? as usize
        } else {
            1
        };

        let value = match db.get_typed(&key, ValueType::ZSet)? {
            Some(v) => v,
            None => return Ok(Frame::Array(vec![])),
        };

        let zset = value
            .as_zset()
            .expect("type guaranteed by get_or_create_zset");
        let mut zset_guard = zset.write();
        let entries = zset_guard.pop_min(count);
        let is_empty = zset_guard.is_empty();
        drop(zset_guard);

        // Auto-delete empty zset (Redis behavior)
        if is_empty {
            db.delete(&key);
        }

        let frames: Vec<Frame> = entries
            .into_iter()
            .flat_map(|e| {
                vec![
                    Frame::Bulk(e.member),
                    Frame::Bulk(Bytes::from(e.score.to_string())),
                ]
            })
            .collect();

        Ok(Frame::Array(frames))
    })
}

/// ZPOPMAX key [count]
pub fn cmd_zpopmax(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let count = if cmd.args.len() > 1 {
            cmd.get_u64(1)? as usize
        } else {
            1
        };

        let value = match db.get_typed(&key, ValueType::ZSet)? {
            Some(v) => v,
            None => return Ok(Frame::Array(vec![])),
        };

        let zset = value
            .as_zset()
            .expect("type guaranteed by get_or_create_zset");
        let mut zset_guard = zset.write();
        let entries = zset_guard.pop_max(count);
        let is_empty = zset_guard.is_empty();
        drop(zset_guard);

        // Auto-delete empty zset (Redis behavior)
        if is_empty {
            db.delete(&key);
        }

        let frames: Vec<Frame> = entries
            .into_iter()
            .flat_map(|e| {
                vec![
                    Frame::Bulk(e.member),
                    Frame::Bulk(Bytes::from(e.score.to_string())),
                ]
            })
            .collect();

        Ok(Frame::Array(frames))
    })
}

/// ZSCAN key cursor [MATCH pattern] [COUNT count] [NOSCORES]
pub fn cmd_zscan(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let cursor = cmd.get_u64(1)? as usize;

        let mut pattern: Option<Bytes> = None;
        let mut count: usize = 10;
        let mut no_scores = false;

        // Parse options
        let mut i = 2;
        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            match opt.as_str() {
                "MATCH" => {
                    i += 1;
                    pattern = Some(cmd.args.get(i).cloned().ok_or(CommandError::SyntaxError)?);
                }
                "COUNT" => {
                    i += 1;
                    count = cmd.get_u64(i)? as usize;
                }
                "NOSCORES" => {
                    no_scores = true;
                }
                _ => {
                    return Err(CommandError::SyntaxError.into());
                }
            }
            i += 1;
        }

        let value = match db.get_typed(&key, ValueType::ZSet)? {
            Some(v) => v,
            None => {
                return Ok(Frame::Array(vec![
                    Frame::Bulk(Bytes::from("0")),
                    Frame::Array(vec![]),
                ]));
            }
        };

        let zset = value
            .as_zset()
            .expect("type guaranteed by get_or_create_zset")
            .read();
        // Get all entries in sorted order
        let entries = zset.range(0, -1);

        // Simple scan implementation - just paginate through entries
        let total = entries.len();
        let start = cursor;
        let end = std::cmp::min(start + count, total);

        let mut result_entries = Vec::new();
        for i in start..end {
            let entry = &entries[i];
            // Apply pattern filter if specified (only on member names)
            let matches = pattern
                .as_ref()
                .map(|p| match_pattern(p, &entry.member))
                .unwrap_or(true);
            if matches {
                result_entries.push(Frame::Bulk(entry.member.clone()));
                if !no_scores {
                    result_entries.push(Frame::Bulk(Bytes::from(entry.score.to_string())));
                }
            }
        }

        let next_cursor = if end >= total { 0 } else { end };

        Ok(Frame::Array(vec![
            Frame::Bulk(Bytes::from(next_cursor.to_string())),
            Frame::Array(result_entries),
        ]))
    })
}

/// ZRANDMEMBER key [count [WITHSCORES]]
pub fn cmd_zrandmember(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        use rand::seq::SliceRandom;

        let key = Key::from(cmd.args[0].clone());

        let count = if cmd.args.len() > 1 {
            Some(cmd.get_i64(1)?)
        } else {
            None
        };

        let with_scores = if cmd.args.len() > 2 {
            let opt = cmd.get_str(2)?.to_uppercase();
            opt == "WITHSCORES"
        } else {
            false
        };

        let value = match db.get_typed(&key, ValueType::ZSet)? {
            Some(v) => v,
            None => {
                return Ok(if count.is_some() {
                    Frame::Array(vec![])
                } else {
                    Frame::Null
                });
            }
        };

        let zset = value
            .as_zset()
            .expect("type guaranteed by get_or_create_zset")
            .read();
        let entries = zset.range(0, -1);

        if entries.is_empty() {
            return Ok(if count.is_some() {
                Frame::Array(vec![])
            } else {
                Frame::Null
            });
        }

        let mut rng = rand::thread_rng();

        match count {
            Some(count) => {
                let allow_duplicates = count < 0;
                let count = count.unsigned_abs() as usize;

                let selected: Vec<_> = if allow_duplicates {
                    // With duplicates allowed (negative count)
                    (0..count)
                        .map(|_| entries.choose(&mut rng).unwrap().clone())
                        .collect()
                } else {
                    // Without duplicates
                    let take = std::cmp::min(count, entries.len());
                    let mut shuffled = entries.clone();
                    shuffled.shuffle(&mut rng);
                    shuffled.into_iter().take(take).collect()
                };

                if with_scores {
                    let mut result = Vec::new();
                    for entry in selected {
                        result.push(Frame::Bulk(entry.member));
                        result.push(Frame::Bulk(Bytes::from(entry.score.to_string())));
                    }
                    Ok(Frame::Array(result))
                } else {
                    Ok(Frame::Array(
                        selected
                            .into_iter()
                            .map(|e| Frame::Bulk(e.member))
                            .collect(),
                    ))
                }
            }
            None => match entries.choose(&mut rng) {
                Some(entry) => Ok(Frame::Bulk(entry.member.clone())),
                None => Ok(Frame::Null),
            },
        }
    })
}

/// Simple glob pattern matching for SCAN commands
fn match_pattern(pattern: &Bytes, text: &Bytes) -> bool {
    let pattern = pattern.as_ref();
    let text = text.as_ref();

    let mut p = 0;
    let mut t = 0;
    let mut star_p = None;
    let mut star_t = 0;

    while t < text.len() {
        if p < pattern.len() && (pattern[p] == b'?' || pattern[p] == text[t]) {
            p += 1;
            t += 1;
        } else if p < pattern.len() && pattern[p] == b'*' {
            star_p = Some(p);
            star_t = t;
            p += 1;
        } else if let Some(sp) = star_p {
            p = sp + 1;
            star_t += 1;
            t = star_t;
        } else {
            return false;
        }
    }

    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }

    p == pattern.len()
}

/// ZRANGESTORE dst src min max [BYSCORE | BYLEX] [REV] [LIMIT offset count]
pub fn cmd_zrangestore(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(4)?;

        let dest_key = Key::from(cmd.args[0].clone());
        let src_key = Key::from(cmd.args[1].clone());
        let start_str = cmd.get_str(2)?;
        let stop_str = cmd.get_str(3)?;

        // Parse options
        let mut by_score = false;
        let mut rev = false;
        let mut offset: usize = 0;
        let mut count: Option<usize> = None;

        let mut idx = 4;
        while idx < cmd.args.len() {
            let arg = cmd.get_str(idx)?.to_uppercase();
            match arg.as_str() {
                "BYSCORE" => by_score = true,
                "BYLEX" => (), // Just recognize but default behavior
                "REV" => rev = true,
                "LIMIT" => {
                    idx += 1;
                    offset = cmd.get_u64(idx)? as usize;
                    idx += 1;
                    count = Some(cmd.get_u64(idx)? as usize);
                }
                _ => return Err(CommandError::SyntaxError.into()),
            }
            idx += 1;
        }

        let src_value = match db.get_typed(&src_key, ValueType::ZSet)? {
            Some(v) => v,
            None => {
                db.delete(&dest_key);
                return Ok(Frame::Integer(0));
            }
        };

        let src_zset = src_value
            .as_zset()
            .expect("type guaranteed by get_or_create_zset")
            .read();

        let entries = if by_score {
            let min = ScoreBound::parse(start_str).ok_or(CommandError::NotFloat)?;
            let max = ScoreBound::parse(stop_str).ok_or(CommandError::NotFloat)?;
            let mut results = src_zset.range_by_score(min, max, offset, count);
            if rev {
                results.reverse();
            }
            results
        } else {
            let start: i64 = start_str.parse().map_err(|_| CommandError::NotInteger)?;
            let stop: i64 = stop_str.parse().map_err(|_| CommandError::NotInteger)?;
            let results = if rev {
                src_zset.rev_range(start, stop)
            } else {
                src_zset.range(start, stop)
            };
            results
                .into_iter()
                .skip(offset)
                .take(count.unwrap_or(usize::MAX))
                .collect()
        };

        let result_count = entries.len() as i64;

        if entries.is_empty() {
            db.delete(&dest_key);
        } else {
            let dest_value = ViatorValue::new_zset();
            let dest_zset = dest_value
                .as_zset()
                .expect("type guaranteed by get_or_create_zset");
            let mut guard = dest_zset.write();
            for entry in entries {
                guard.add(entry.member, entry.score);
            }
            drop(guard);
            db.set(dest_key, dest_value);
        }

        Ok(Frame::Integer(result_count))
    })
}

/// ZUNION numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
pub fn cmd_zunion(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        let numkeys: usize = cmd.get_u64(0)? as usize;

        if cmd.args.len() < 1 + numkeys {
            return Err(CommandError::WrongArity {
                command: "ZUNION".to_string(),
            }
            .into());
        }

        let keys: Vec<Key> = cmd.args[1..1 + numkeys]
            .iter()
            .map(|b| Key::from(b.clone()))
            .collect();

        // Parse options
        let mut weights: Vec<f64> = vec![1.0; numkeys];
        let mut aggregate = "SUM";
        let mut with_scores = false;

        let mut idx = 1 + numkeys;
        while idx < cmd.args.len() {
            let arg = cmd.get_str(idx)?.to_uppercase();
            match arg.as_str() {
                "WEIGHTS" => {
                    for i in 0..numkeys {
                        idx += 1;
                        weights[i] = cmd.get_f64(idx)?;
                    }
                }
                "AGGREGATE" => {
                    idx += 1;
                    aggregate = match cmd.get_str(idx)?.to_uppercase().as_str() {
                        "SUM" => "SUM",
                        "MIN" => "MIN",
                        "MAX" => "MAX",
                        _ => return Err(CommandError::SyntaxError.into()),
                    };
                }
                "WITHSCORES" => with_scores = true,
                _ => return Err(CommandError::SyntaxError.into()),
            }
            idx += 1;
        }

        // Collect all members with their weighted scores
        let mut result_map: std::collections::HashMap<Bytes, f64> =
            std::collections::HashMap::new();

        for (i, key) in keys.iter().enumerate() {
            if let Some(value) = db.get_typed(key, ValueType::ZSet)? {
                let zset = value
                    .as_zset()
                    .expect("type guaranteed by get_or_create_zset")
                    .read();
                for entry in zset.iter() {
                    let weighted_score = entry.score * weights[i];
                    result_map
                        .entry(entry.member.clone())
                        .and_modify(|existing| {
                            *existing = match aggregate {
                                "SUM" => *existing + weighted_score,
                                "MIN" => existing.min(weighted_score),
                                "MAX" => existing.max(weighted_score),
                                _ => *existing + weighted_score,
                            };
                        })
                        .or_insert(weighted_score);
                }
            }
        }

        // Sort by score then by member
        let mut entries: Vec<_> = result_map.into_iter().collect();
        entries.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });

        if with_scores {
            let mut result = Vec::new();
            for (member, score) in entries {
                result.push(Frame::Bulk(member));
                result.push(Frame::Bulk(Bytes::from(score.to_string())));
            }
            Ok(Frame::Array(result))
        } else {
            Ok(Frame::Array(
                entries.into_iter().map(|(m, _)| Frame::Bulk(m)).collect(),
            ))
        }
    })
}

/// ZINTER numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
pub fn cmd_zinter(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        let numkeys: usize = cmd.get_u64(0)? as usize;

        if cmd.args.len() < 1 + numkeys {
            return Err(CommandError::WrongArity {
                command: "ZINTER".to_string(),
            }
            .into());
        }

        let keys: Vec<Key> = cmd.args[1..1 + numkeys]
            .iter()
            .map(|b| Key::from(b.clone()))
            .collect();

        // Parse options
        let mut weights: Vec<f64> = vec![1.0; numkeys];
        let mut aggregate = "SUM";
        let mut with_scores = false;

        let mut idx = 1 + numkeys;
        while idx < cmd.args.len() {
            let arg = cmd.get_str(idx)?.to_uppercase();
            match arg.as_str() {
                "WEIGHTS" => {
                    for i in 0..numkeys {
                        idx += 1;
                        weights[i] = cmd.get_f64(idx)?;
                    }
                }
                "AGGREGATE" => {
                    idx += 1;
                    aggregate = match cmd.get_str(idx)?.to_uppercase().as_str() {
                        "SUM" => "SUM",
                        "MIN" => "MIN",
                        "MAX" => "MAX",
                        _ => return Err(CommandError::SyntaxError.into()),
                    };
                }
                "WITHSCORES" => with_scores = true,
                _ => return Err(CommandError::SyntaxError.into()),
            }
            idx += 1;
        }

        // Start with the first set
        let first_value = match db.get_typed(&keys[0], ValueType::ZSet)? {
            Some(v) => v,
            None => return Ok(Frame::Array(vec![])),
        };

        let first_zset = first_value
            .as_zset()
            .expect("type guaranteed by get_or_create_zset")
            .read();
        let mut result_map: std::collections::HashMap<Bytes, f64> =
            std::collections::HashMap::new();

        for entry in first_zset.iter() {
            result_map.insert(entry.member.clone(), entry.score * weights[0]);
        }
        drop(first_zset);

        // Intersect with remaining sets
        for (i, key) in keys.iter().enumerate().skip(1) {
            if let Some(value) = db.get_typed(key, ValueType::ZSet)? {
                let zset = value
                    .as_zset()
                    .expect("type guaranteed by get_or_create_zset")
                    .read();
                let members_in_current: std::collections::HashSet<_> =
                    zset.iter().map(|e| e.member.clone()).collect();

                // Remove members not in current set
                result_map.retain(|m, _| members_in_current.contains(m));

                // Update scores
                for entry in zset.iter() {
                    if let Some(existing) = result_map.get_mut(&entry.member) {
                        let weighted_score = entry.score * weights[i];
                        *existing = match aggregate {
                            "SUM" => *existing + weighted_score,
                            "MIN" => existing.min(weighted_score),
                            "MAX" => existing.max(weighted_score),
                            _ => *existing + weighted_score,
                        };
                    }
                }
            } else {
                return Ok(Frame::Array(vec![]));
            }
        }

        // Sort by score then by member
        let mut entries: Vec<_> = result_map.into_iter().collect();
        entries.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });

        if with_scores {
            let mut result = Vec::new();
            for (member, score) in entries {
                result.push(Frame::Bulk(member));
                result.push(Frame::Bulk(Bytes::from(score.to_string())));
            }
            Ok(Frame::Array(result))
        } else {
            Ok(Frame::Array(
                entries.into_iter().map(|(m, _)| Frame::Bulk(m)).collect(),
            ))
        }
    })
}

/// ZDIFF numkeys key [key ...] [WITHSCORES]
pub fn cmd_zdiff(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        let numkeys: usize = cmd.get_u64(0)? as usize;

        if cmd.args.len() < 1 + numkeys {
            return Err(CommandError::WrongArity {
                command: "ZDIFF".to_string(),
            }
            .into());
        }

        let keys: Vec<Key> = cmd.args[1..1 + numkeys]
            .iter()
            .map(|b| Key::from(b.clone()))
            .collect();

        let with_scores = cmd.args.len() > 1 + numkeys
            && cmd.get_str(1 + numkeys)?.to_uppercase() == "WITHSCORES";

        // Start with the first set
        let first_value = match db.get_typed(&keys[0], ValueType::ZSet)? {
            Some(v) => v,
            None => return Ok(Frame::Array(vec![])),
        };

        let first_zset = first_value
            .as_zset()
            .expect("type guaranteed by get_or_create_zset")
            .read();
        let mut result_map: std::collections::HashMap<Bytes, f64> =
            std::collections::HashMap::new();

        for entry in first_zset.iter() {
            result_map.insert(entry.member.clone(), entry.score);
        }
        drop(first_zset);

        // Remove members from other sets
        for key in keys.iter().skip(1) {
            if let Some(value) = db.get_typed(key, ValueType::ZSet)? {
                let zset = value
                    .as_zset()
                    .expect("type guaranteed by get_or_create_zset")
                    .read();
                for entry in zset.iter() {
                    result_map.remove(&entry.member);
                }
            }
        }

        // Sort by score then by member
        let mut entries: Vec<_> = result_map.into_iter().collect();
        entries.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });

        if with_scores {
            let mut result = Vec::new();
            for (member, score) in entries {
                result.push(Frame::Bulk(member));
                result.push(Frame::Bulk(Bytes::from(score.to_string())));
            }
            Ok(Frame::Array(result))
        } else {
            Ok(Frame::Array(
                entries.into_iter().map(|(m, _)| Frame::Bulk(m)).collect(),
            ))
        }
    })
}

/// ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
pub fn cmd_zmpop(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;

        let numkeys: usize = cmd.get_u64(0)? as usize;

        if cmd.args.len() < 2 + numkeys {
            return Err(CommandError::WrongArity {
                command: "ZMPOP".to_string(),
            }
            .into());
        }

        let keys: Vec<Key> = cmd.args[1..1 + numkeys]
            .iter()
            .map(|b| Key::from(b.clone()))
            .collect();

        let direction = cmd.get_str(1 + numkeys)?.to_uppercase();
        let mut count = 1usize;

        if cmd.args.len() > 2 + numkeys {
            let count_arg = cmd.get_str(2 + numkeys)?.to_uppercase();
            if count_arg == "COUNT" && cmd.args.len() > 3 + numkeys {
                count = cmd.get_u64(3 + numkeys)? as usize;
            }
        }

        for key in &keys {
            if let Some(value) = db.get(key) {
                if let Some(zset) = value.as_zset() {
                    let mut guard = zset.write();
                    if !guard.is_empty() {
                        let entries = match direction.as_str() {
                            "MIN" => guard.pop_min(count),
                            "MAX" => guard.pop_max(count),
                            _ => return Err(CommandError::SyntaxError.into()),
                        };

                        if !entries.is_empty() {
                            let elements: Vec<Frame> = entries
                                .into_iter()
                                .map(|entry| {
                                    Frame::Array(vec![
                                        Frame::Bulk(entry.member),
                                        Frame::Bulk(Bytes::from(entry.score.to_string())),
                                    ])
                                })
                                .collect();

                            let key_bytes = Bytes::copy_from_slice(key.as_bytes());
                            drop(guard);

                            if value
                                .as_zset()
                                .map(|z| z.read().is_empty())
                                .unwrap_or(false)
                            {
                                db.delete(key);
                            }

                            return Ok(Frame::Array(vec![
                                Frame::Bulk(key_bytes),
                                Frame::Array(elements),
                            ]));
                        }
                    }
                }
            }
        }

        Ok(Frame::Null)
    })
}

/// ZDIFFSTORE destination numkeys key [key ...]
pub fn cmd_zdiffstore(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;

        let dest_key = Key::from(cmd.args[0].clone());
        let numkeys: usize = cmd.get_u64(1)? as usize;

        if cmd.args.len() < 2 + numkeys {
            return Err(CommandError::WrongArity {
                command: "ZDIFFSTORE".to_string(),
            }
            .into());
        }

        let keys: Vec<Key> = cmd.args[2..2 + numkeys]
            .iter()
            .map(|b| Key::from(b.clone()))
            .collect();

        // Compute difference
        let first_value = match db.get_typed(&keys[0], ValueType::ZSet)? {
            Some(v) => v,
            None => {
                db.delete(&dest_key);
                return Ok(Frame::Integer(0));
            }
        };

        let first_zset = first_value
            .as_zset()
            .expect("type guaranteed by get_or_create_zset")
            .read();
        let mut result_map: std::collections::HashMap<Bytes, f64> =
            std::collections::HashMap::new();

        for entry in first_zset.iter() {
            result_map.insert(entry.member.clone(), entry.score);
        }
        drop(first_zset);

        for key in keys.iter().skip(1) {
            if let Some(value) = db.get_typed(key, ValueType::ZSet)? {
                let zset = value
                    .as_zset()
                    .expect("type guaranteed by get_or_create_zset")
                    .read();
                for entry in zset.iter() {
                    result_map.remove(&entry.member);
                }
            }
        }

        // Store result
        let count = result_map.len() as i64;
        if count == 0 {
            db.delete(&dest_key);
        } else {
            let new_zset = ViatorValue::new_zset();
            let zset = new_zset
                .as_zset()
                .expect("type guaranteed by get_or_create_zset");
            let mut guard = zset.write();
            for (member, score) in result_map {
                guard.add(member, score);
            }
            drop(guard);
            db.set(dest_key, new_zset);
        }

        Ok(Frame::Integer(count))
    })
}

/// ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
pub fn cmd_zinterstore(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;

        let dest_key = Key::from(cmd.args[0].clone());
        let numkeys: usize = cmd.get_u64(1)? as usize;

        if cmd.args.len() < 2 + numkeys {
            return Err(CommandError::WrongArity {
                command: "ZINTERSTORE".to_string(),
            }
            .into());
        }

        let keys: Vec<Key> = cmd.args[2..2 + numkeys]
            .iter()
            .map(|b| Key::from(b.clone()))
            .collect();

        // Parse WEIGHTS and AGGREGATE
        let mut weights: Vec<f64> = vec![1.0; numkeys];
        let mut aggregate = "SUM";
        let mut i = 2 + numkeys;
        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            match opt.as_str() {
                "WEIGHTS" => {
                    for j in 0..numkeys {
                        i += 1;
                        weights[j] = cmd.get_f64(i)?;
                    }
                }
                "AGGREGATE" => {
                    i += 1;
                    aggregate = match cmd.get_str(i)?.to_uppercase().as_str() {
                        "SUM" => "SUM",
                        "MIN" => "MIN",
                        "MAX" => "MAX",
                        _ => return Err(CommandError::SyntaxError.into()),
                    };
                }
                _ => {}
            }
            i += 1;
        }

        // Compute intersection
        let first_value = match db.get_typed(&keys[0], ValueType::ZSet)? {
            Some(v) => v,
            None => {
                db.delete(&dest_key);
                return Ok(Frame::Integer(0));
            }
        };

        let first_zset = first_value
            .as_zset()
            .expect("type guaranteed by get_or_create_zset")
            .read();
        let mut result: std::collections::HashMap<Bytes, f64> = std::collections::HashMap::new();

        'outer: for entry in first_zset.iter() {
            let mut scores = vec![entry.score * weights[0]];

            for (idx, key) in keys.iter().enumerate().skip(1) {
                if let Some(value) = db.get_typed(key, ValueType::ZSet)? {
                    let zset = value
                        .as_zset()
                        .expect("type guaranteed by get_or_create_zset")
                        .read();
                    if let Some(score) = zset.score(&entry.member) {
                        scores.push(score * weights[idx]);
                    } else {
                        continue 'outer;
                    }
                } else {
                    continue 'outer;
                }
            }

            let final_score = match aggregate {
                "SUM" => scores.iter().sum(),
                "MIN" => scores.iter().cloned().fold(f64::INFINITY, f64::min),
                "MAX" => scores.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
                _ => scores.iter().sum(),
            };

            result.insert(entry.member.clone(), final_score);
        }
        drop(first_zset);

        let count = result.len() as i64;
        if count == 0 {
            db.delete(&dest_key);
        } else {
            let new_zset = ViatorValue::new_zset();
            let zset = new_zset
                .as_zset()
                .expect("type guaranteed by get_or_create_zset");
            let mut guard = zset.write();
            for (member, score) in result {
                guard.add(member, score);
            }
            drop(guard);
            db.set(dest_key, new_zset);
        }

        Ok(Frame::Integer(count))
    })
}

/// ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
pub fn cmd_zunionstore(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;

        let dest_key = Key::from(cmd.args[0].clone());
        let numkeys: usize = cmd.get_u64(1)? as usize;

        if cmd.args.len() < 2 + numkeys {
            return Err(CommandError::WrongArity {
                command: "ZUNIONSTORE".to_string(),
            }
            .into());
        }

        let keys: Vec<Key> = cmd.args[2..2 + numkeys]
            .iter()
            .map(|b| Key::from(b.clone()))
            .collect();

        // Parse WEIGHTS and AGGREGATE
        let mut weights: Vec<f64> = vec![1.0; numkeys];
        let mut aggregate = "SUM";
        let mut i = 2 + numkeys;
        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            match opt.as_str() {
                "WEIGHTS" => {
                    for j in 0..numkeys {
                        i += 1;
                        weights[j] = cmd.get_f64(i)?;
                    }
                }
                "AGGREGATE" => {
                    i += 1;
                    aggregate = match cmd.get_str(i)?.to_uppercase().as_str() {
                        "SUM" => "SUM",
                        "MIN" => "MIN",
                        "MAX" => "MAX",
                        _ => return Err(CommandError::SyntaxError.into()),
                    };
                }
                _ => {}
            }
            i += 1;
        }

        // Compute union
        let mut result: std::collections::HashMap<Bytes, Vec<f64>> =
            std::collections::HashMap::new();

        for (idx, key) in keys.iter().enumerate() {
            if let Some(value) = db.get_typed(key, ValueType::ZSet)? {
                let zset = value
                    .as_zset()
                    .expect("type guaranteed by get_or_create_zset")
                    .read();
                for entry in zset.iter() {
                    result
                        .entry(entry.member.clone())
                        .or_default()
                        .push(entry.score * weights[idx]);
                }
            }
        }

        let count = result.len() as i64;
        if count == 0 {
            db.delete(&dest_key);
        } else {
            let new_zset = ViatorValue::new_zset();
            let zset = new_zset
                .as_zset()
                .expect("type guaranteed by get_or_create_zset");
            let mut guard = zset.write();
            for (member, scores) in result {
                let final_score = match aggregate {
                    "SUM" => scores.iter().sum(),
                    "MIN" => scores.iter().cloned().fold(f64::INFINITY, f64::min),
                    "MAX" => scores.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
                    _ => scores.iter().sum(),
                };
                guard.add(member, final_score);
            }
            drop(guard);
            db.set(dest_key, new_zset);
        }

        Ok(Frame::Integer(count))
    })
}

/// ZINTERCARD numkeys key [key ...] [LIMIT limit]
pub fn cmd_zintercard(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        let numkeys: usize = cmd.get_u64(0)? as usize;
        let keys: Vec<Key> = cmd.args[1..1 + numkeys]
            .iter()
            .map(|b| Key::from(b.clone()))
            .collect();

        let mut limit: usize = 0;
        if cmd.args.len() > 1 + numkeys && cmd.get_str(1 + numkeys)?.to_uppercase() == "LIMIT" {
            limit = cmd.get_u64(2 + numkeys)? as usize;
        }

        // Get first set
        let first_value = match db.get_typed(&keys[0], ValueType::ZSet)? {
            Some(v) => v,
            None => return Ok(Frame::Integer(0)),
        };

        let first_zset = first_value
            .as_zset()
            .expect("type guaranteed by get_or_create_zset")
            .read();
        let mut count = 0usize;

        'outer: for entry in first_zset.iter() {
            for key in keys.iter().skip(1) {
                if let Some(value) = db.get_typed(key, ValueType::ZSet)? {
                    let zset = value
                        .as_zset()
                        .expect("type guaranteed by get_or_create_zset")
                        .read();
                    if zset.score(&entry.member).is_none() {
                        continue 'outer;
                    }
                } else {
                    continue 'outer;
                }
            }
            count += 1;
            if limit > 0 && count >= limit {
                break;
            }
        }

        Ok(Frame::Integer(count as i64))
    })
}

/// ZMSCORE key member [member ...]
pub fn cmd_zmscore(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        let key = Key::from(cmd.args[0].clone());

        let value = db.get_typed(&key, ValueType::ZSet)?;

        let results: Vec<Frame> = cmd.args[1..]
            .iter()
            .map(|member| match &value {
                Some(v) => {
                    let zset = v
                        .as_zset()
                        .expect("type guaranteed by get_or_create_zset")
                        .read();
                    match zset.score(member) {
                        Some(score) => Frame::Bulk(Bytes::from(score.to_string())),
                        None => Frame::Null,
                    }
                }
                None => Frame::Null,
            })
            .collect();

        Ok(Frame::Array(results))
    })
}

/// ZLEXCOUNT key min max
pub fn cmd_zlexcount(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(3)?;

        let key = Key::from(cmd.args[0].clone());
        let min = cmd.get_str(1)?;
        let max = cmd.get_str(2)?;

        let value = match db.get_typed(&key, ValueType::ZSet)? {
            Some(v) => v,
            None => return Ok(Frame::Integer(0)),
        };

        let zset = value
            .as_zset()
            .expect("type guaranteed by get_or_create_zset")
            .read();
        let count = zset
            .iter()
            .filter(|entry| {
                let member_str = String::from_utf8_lossy(&entry.member);
                lex_in_range(&member_str, min, max)
            })
            .count();

        Ok(Frame::Integer(count as i64))
    })
}

/// ZRANGEBYLEX key min max [LIMIT offset count] (deprecated, use ZRANGE)
pub fn cmd_zrangebylex(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;

        let key = Key::from(cmd.args[0].clone());
        let min = cmd.get_str(1)?;
        let max = cmd.get_str(2)?;

        let mut offset = 0usize;
        let mut count = usize::MAX;

        if cmd.args.len() > 3 && cmd.get_str(3)?.to_uppercase() == "LIMIT" {
            offset = cmd.get_u64(4)? as usize;
            count = cmd.get_u64(5)? as usize;
        }

        let value = match db.get_typed(&key, ValueType::ZSet)? {
            Some(v) => v,
            None => return Ok(Frame::Array(vec![])),
        };

        let zset = value
            .as_zset()
            .expect("type guaranteed by get_or_create_zset")
            .read();
        let results: Vec<Frame> = zset
            .iter()
            .filter(|entry| {
                let member_str = String::from_utf8_lossy(&entry.member);
                lex_in_range(&member_str, min, max)
            })
            .skip(offset)
            .take(count)
            .map(|entry| Frame::Bulk(entry.member.clone()))
            .collect();

        Ok(Frame::Array(results))
    })
}

/// ZREVRANGEBYLEX key max min [LIMIT offset count] (deprecated, use ZRANGE)
pub fn cmd_zrevrangebylex(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;

        let key = Key::from(cmd.args[0].clone());
        let max = cmd.get_str(1)?;
        let min = cmd.get_str(2)?;

        let mut offset = 0usize;
        let mut count = usize::MAX;

        if cmd.args.len() > 3 && cmd.get_str(3)?.to_uppercase() == "LIMIT" {
            offset = cmd.get_u64(4)? as usize;
            count = cmd.get_u64(5)? as usize;
        }

        let value = match db.get_typed(&key, ValueType::ZSet)? {
            Some(v) => v,
            None => return Ok(Frame::Array(vec![])),
        };

        let zset = value
            .as_zset()
            .expect("type guaranteed by get_or_create_zset")
            .read();
        let mut entries: Vec<_> = zset
            .iter()
            .filter(|entry| {
                let member_str = String::from_utf8_lossy(&entry.member);
                lex_in_range(&member_str, min, max)
            })
            .collect();
        entries.reverse();

        let results: Vec<Frame> = entries
            .into_iter()
            .skip(offset)
            .take(count)
            .map(|entry| Frame::Bulk(entry.member.clone()))
            .collect();

        Ok(Frame::Array(results))
    })
}

/// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count] (deprecated, use ZRANGE)
pub fn cmd_zrangebyscore(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;

        let key = Key::from(cmd.args[0].clone());
        let min = parse_score_bound(cmd.get_str(1)?)?;
        let max = parse_score_bound(cmd.get_str(2)?)?;

        let mut with_scores = false;
        let mut offset = 0usize;
        let mut count = usize::MAX;

        let mut i = 3;
        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            match opt.as_str() {
                "WITHSCORES" => with_scores = true,
                "LIMIT" => {
                    offset = cmd.get_u64(i + 1)? as usize;
                    count = cmd.get_u64(i + 2)? as usize;
                    i += 2;
                }
                _ => {}
            }
            i += 1;
        }

        let value = match db.get_typed(&key, ValueType::ZSet)? {
            Some(v) => v,
            None => return Ok(Frame::Array(vec![])),
        };

        let zset = value
            .as_zset()
            .expect("type guaranteed by get_or_create_zset")
            .read();
        let entries: Vec<_> = zset
            .iter()
            .filter(|entry| score_in_range(entry.score, &min, &max))
            .skip(offset)
            .take(count)
            .collect();

        if with_scores {
            let mut result = Vec::new();
            for entry in entries {
                result.push(Frame::Bulk(entry.member.clone()));
                result.push(Frame::Bulk(Bytes::from(entry.score.to_string())));
            }
            Ok(Frame::Array(result))
        } else {
            Ok(Frame::Array(
                entries
                    .into_iter()
                    .map(|e| Frame::Bulk(e.member.clone()))
                    .collect(),
            ))
        }
    })
}

/// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count] (deprecated, use ZRANGE)
pub fn cmd_zrevrangebyscore(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;

        let key = Key::from(cmd.args[0].clone());
        let max = parse_score_bound(cmd.get_str(1)?)?;
        let min = parse_score_bound(cmd.get_str(2)?)?;

        let mut with_scores = false;
        let mut offset = 0usize;
        let mut count = usize::MAX;

        let mut i = 3;
        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            match opt.as_str() {
                "WITHSCORES" => with_scores = true,
                "LIMIT" => {
                    offset = cmd.get_u64(i + 1)? as usize;
                    count = cmd.get_u64(i + 2)? as usize;
                    i += 2;
                }
                _ => {}
            }
            i += 1;
        }

        let value = match db.get_typed(&key, ValueType::ZSet)? {
            Some(v) => v,
            None => return Ok(Frame::Array(vec![])),
        };

        let zset = value
            .as_zset()
            .expect("type guaranteed by get_or_create_zset")
            .read();
        let mut entries: Vec<_> = zset
            .iter()
            .filter(|entry| score_in_range(entry.score, &min, &max))
            .collect();
        entries.reverse();

        let entries: Vec<_> = entries.into_iter().skip(offset).take(count).collect();

        if with_scores {
            let mut result = Vec::new();
            for entry in entries {
                result.push(Frame::Bulk(entry.member.clone()));
                result.push(Frame::Bulk(Bytes::from(entry.score.to_string())));
            }
            Ok(Frame::Array(result))
        } else {
            Ok(Frame::Array(
                entries
                    .into_iter()
                    .map(|e| Frame::Bulk(e.member.clone()))
                    .collect(),
            ))
        }
    })
}

/// ZREMRANGEBYLEX key min max
pub fn cmd_zremrangebylex(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(3)?;

        let key = Key::from(cmd.args[0].clone());
        let min = cmd.get_str(1)?;
        let max = cmd.get_str(2)?;

        let value = match db.get_typed(&key, ValueType::ZSet)? {
            Some(v) => v,
            None => return Ok(Frame::Integer(0)),
        };

        let zset = value
            .as_zset()
            .expect("type guaranteed by get_or_create_zset");
        let mut guard = zset.write();

        let to_remove: Vec<Bytes> = guard
            .iter()
            .filter(|entry| {
                let member_str = String::from_utf8_lossy(&entry.member);
                lex_in_range(&member_str, min, max)
            })
            .map(|entry| entry.member.clone())
            .collect();

        let count = to_remove.len() as i64;
        for member in to_remove {
            guard.remove(&member);
        }

        let is_empty = guard.is_empty();
        drop(guard);

        if is_empty {
            db.delete(&key);
        }

        Ok(Frame::Integer(count))
    })
}

/// ZREMRANGEBYRANK key start stop
pub fn cmd_zremrangebyrank(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(3)?;

        let key = Key::from(cmd.args[0].clone());
        let start = cmd.get_i64(1)?;
        let stop = cmd.get_i64(2)?;

        let value = match db.get_typed(&key, ValueType::ZSet)? {
            Some(v) => v,
            None => return Ok(Frame::Integer(0)),
        };

        let zset = value
            .as_zset()
            .expect("type guaranteed by get_or_create_zset");
        let mut guard = zset.write();
        let len = guard.len() as i64;

        // Normalize indices
        let start_idx = if start < 0 {
            (len + start).max(0)
        } else {
            start.min(len)
        } as usize;
        let stop_idx = if stop < 0 {
            (len + stop).max(0)
        } else {
            stop.min(len - 1)
        } as usize;

        if start_idx > stop_idx {
            return Ok(Frame::Integer(0));
        }

        let entries: Vec<_> = guard.iter().collect();
        let to_remove: Vec<Bytes> = entries[start_idx..=stop_idx.min(entries.len() - 1)]
            .iter()
            .map(|e| e.member.clone())
            .collect();

        let count = to_remove.len() as i64;
        for member in to_remove {
            guard.remove(&member);
        }

        let is_empty = guard.is_empty();
        drop(guard);

        if is_empty {
            db.delete(&key);
        }

        Ok(Frame::Integer(count))
    })
}

/// ZREMRANGEBYSCORE key min max
pub fn cmd_zremrangebyscore(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(3)?;

        let key = Key::from(cmd.args[0].clone());
        let min = parse_score_bound(cmd.get_str(1)?)?;
        let max = parse_score_bound(cmd.get_str(2)?)?;

        let value = match db.get_typed(&key, ValueType::ZSet)? {
            Some(v) => v,
            None => return Ok(Frame::Integer(0)),
        };

        let zset = value
            .as_zset()
            .expect("type guaranteed by get_or_create_zset");
        let mut guard = zset.write();

        let to_remove: Vec<Bytes> = guard
            .iter()
            .filter(|entry| score_in_range(entry.score, &min, &max))
            .map(|entry| entry.member.clone())
            .collect();

        let count = to_remove.len() as i64;
        for member in to_remove {
            guard.remove(&member);
        }

        let is_empty = guard.is_empty();
        drop(guard);

        if is_empty {
            db.delete(&key);
        }

        Ok(Frame::Integer(count))
    })
}

/// Parse a score bound string, wrapping ScoreBound::parse() to return Result
fn parse_score_bound(s: &str) -> Result<ScoreBound> {
    ScoreBound::parse(s).ok_or_else(|| CommandError::NotFloat.into())
}

/// Check if a score is within the given bounds
fn score_in_range(score: f64, min: &ScoreBound, max: &ScoreBound) -> bool {
    let above_min = match min {
        ScoreBound::NegInf => true,
        ScoreBound::PosInf => false,
        ScoreBound::Inclusive(v) => score >= *v,
        ScoreBound::Exclusive(v) => score > *v,
    };

    let below_max = match max {
        ScoreBound::NegInf => false,
        ScoreBound::PosInf => true,
        ScoreBound::Inclusive(v) => score <= *v,
        ScoreBound::Exclusive(v) => score < *v,
    };

    above_min && below_max
}

fn lex_in_range(member: &str, min: &str, max: &str) -> bool {
    let above_min = match min.chars().next() {
        Some('-') => true,
        Some('+') => false,
        Some('[') => member >= &min[1..],
        Some('(') => member > &min[1..],
        _ => true,
    };

    let below_max = match max.chars().next() {
        Some('-') => false,
        Some('+') => true,
        Some('[') => member <= &max[1..],
        Some('(') => member < &max[1..],
        _ => true,
    };

    above_min && below_max
}
