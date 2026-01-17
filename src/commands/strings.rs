//! String command implementations.

use super::ParsedCommand;
use crate::Result;
use crate::error::CommandError;
use crate::protocol::Frame;
use crate::server::ClientState;
use crate::storage::Db;
use crate::types::{Expiry, Key, ViatorValue};
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// GET key
/// Fast path: skips LRU tracking for better performance.
pub fn cmd_get(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());

        // Use fast path that skips LRU tracking
        match db.get_string_fast(&key)? {
            Some(value) => Ok(Frame::Bulk(value)),
            None => Ok(Frame::Null),
        }
    })
}

/// Compare condition for SET command (Redis 8.4)
#[derive(Clone)]
enum SetCompareCondition {
    None,
    /// IFEQ: Set only if current value equals the specified value
    IfEq(Bytes),
    /// IFNE: Set only if current value does NOT equal the specified value
    IfNe(Bytes),
    /// IFDEQ: Set only if current value's XXH3 digest equals the specified digest
    IfDeq(u64),
    /// IFDNE: Set only if current value's XXH3 digest does NOT equal the specified digest
    IfDne(u64),
}

/// SET key value [NX | XX | IFEQ value | IFNE value | IFDEQ digest | IFDNE digest] [GET] [EX seconds | PX milliseconds | EXAT timestamp | PXAT timestamp | KEEPTTL]
/// Redis 8.4+ supports IFEQ, IFNE, IFDEQ, IFDNE for atomic compare-and-set operations.
pub fn cmd_set(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let value = cmd.args[1].clone();

        let mut expiry = Expiry::Never;
        let mut nx = false;
        let mut xx = false;
        let mut get = false;
        let mut keepttl = false;
        let mut compare_cond = SetCompareCondition::None;

        // Parse options
        let mut i = 2;
        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            match opt.as_str() {
                "EX" => {
                    i += 1;
                    let seconds = cmd.get_i64(i)?;
                    if seconds < 0 {
                        return Err(CommandError::InvalidExpireTime.into());
                    }
                    expiry = Expiry::from_seconds(seconds);
                }
                "PX" => {
                    i += 1;
                    let ms = cmd.get_i64(i)?;
                    if ms < 0 {
                        return Err(CommandError::InvalidExpireTime.into());
                    }
                    expiry = Expiry::from_millis(ms);
                }
                "EXAT" => {
                    i += 1;
                    let ts = cmd.get_i64(i)?;
                    expiry = Expiry::at_seconds(ts);
                }
                "PXAT" => {
                    i += 1;
                    let ts = cmd.get_i64(i)?;
                    expiry = Expiry::at_millis(ts);
                }
                "NX" => nx = true,
                "XX" => xx = true,
                "GET" => get = true,
                "KEEPTTL" => keepttl = true,
                "IFEQ" => {
                    i += 1;
                    compare_cond = SetCompareCondition::IfEq(cmd.args[i].clone());
                }
                "IFNE" => {
                    i += 1;
                    compare_cond = SetCompareCondition::IfNe(cmd.args[i].clone());
                }
                "IFDEQ" => {
                    i += 1;
                    let digest_str = cmd.get_str(i)?;
                    let digest = u64::from_str_radix(digest_str, 16)
                        .map_err(|_| CommandError::SyntaxError)?;
                    compare_cond = SetCompareCondition::IfDeq(digest);
                }
                "IFDNE" => {
                    i += 1;
                    let digest_str = cmd.get_str(i)?;
                    let digest = u64::from_str_radix(digest_str, 16)
                        .map_err(|_| CommandError::SyntaxError)?;
                    compare_cond = SetCompareCondition::IfDne(digest);
                }
                _ => {
                    return Err(CommandError::InvalidArgument {
                        command: "SET".to_string(),
                        arg: opt,
                    }
                    .into());
                }
            }
            i += 1;
        }

        // NX and XX are mutually exclusive
        if nx && xx {
            return Err(CommandError::SyntaxError.into());
        }

        // Compare conditions are mutually exclusive with NX/XX
        if !matches!(compare_cond, SetCompareCondition::None) && (nx || xx) {
            return Err(CommandError::SyntaxError.into());
        }

        // Handle KEEPTTL validation
        if keepttl && !matches!(expiry, Expiry::Never) {
            return Err(CommandError::SyntaxError.into());
        }

        // Fast path: basic SET without GET/KEEPTTL/compare options
        let needs_old_value = get || keepttl || !matches!(compare_cond, SetCompareCondition::None);

        if !needs_old_value && !nx && !xx {
            // Fastest path: SET key value [EX/PX]
            db.set_fast_with_expiry(key, ViatorValue::string(value), expiry);
            return Ok(Frame::ok());
        }

        // Slower path: need to check old value
        let old_value = db.get_string(&key)?;

        // Handle KEEPTTL
        if keepttl {
            if let Some(pttl) = db.pttl(&key) {
                if pttl > 0 {
                    expiry = Expiry::from_millis(pttl);
                }
            }
        }

        // Check compare condition (Redis 8.4 IFEQ/IFNE/IFDEQ/IFDNE)
        let compare_passed = match &compare_cond {
            SetCompareCondition::None => true,
            SetCompareCondition::IfEq(expected) => {
                match &old_value {
                    Some(current) => current.as_ref() == expected.as_ref(),
                    None => false, // Key must exist for IFEQ
                }
            }
            SetCompareCondition::IfNe(not_expected) => {
                match &old_value {
                    Some(current) => current.as_ref() != not_expected.as_ref(),
                    None => false, // Key must exist for IFNE
                }
            }
            SetCompareCondition::IfDeq(expected_digest) => {
                match &old_value {
                    Some(current) => {
                        let current_digest = xxhash_rust::xxh3::xxh3_64(current);
                        current_digest == *expected_digest
                    }
                    None => false, // Key must exist for IFDEQ
                }
            }
            SetCompareCondition::IfDne(not_expected_digest) => {
                match &old_value {
                    Some(current) => {
                        let current_digest = xxhash_rust::xxh3::xxh3_64(current);
                        current_digest != *not_expected_digest
                    }
                    None => false, // Key must exist for IFDNE
                }
            }
        };

        if !compare_passed {
            // Compare condition failed - return null (or old value if GET)
            return if get {
                Ok(match old_value {
                    Some(v) => Frame::Bulk(v),
                    None => Frame::Null,
                })
            } else {
                Ok(Frame::Null)
            };
        }

        // Perform the set operation
        let success = if nx {
            db.set_nx_with_expiry(key, ViatorValue::string(value), expiry)
        } else if xx {
            db.set_xx_with_expiry(key, ViatorValue::string(value), expiry)
        } else {
            db.set_fast_with_expiry(key, ViatorValue::string(value), expiry);
            true
        };

        // Return appropriate response
        if get {
            Ok(match old_value {
                Some(v) => Frame::Bulk(v),
                None => Frame::Null,
            })
        } else if success {
            Ok(Frame::ok())
        } else {
            Ok(Frame::Null)
        }
    })
}

/// SETNX key value
pub fn cmd_setnx(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let value = cmd.args[1].clone();

        if db.set_nx(key, ViatorValue::string(value)) {
            Ok(Frame::Integer(1))
        } else {
            Ok(Frame::Integer(0))
        }
    })
}

/// SETEX key seconds value
pub fn cmd_setex(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let seconds = cmd.get_i64(1)?;
        let value = cmd.args[2].clone();

        if seconds <= 0 {
            return Err(CommandError::InvalidExpireTime.into());
        }

        db.set_with_expiry(
            key,
            ViatorValue::string(value),
            Expiry::from_seconds(seconds),
        );
        Ok(Frame::ok())
    })
}

/// PSETEX key milliseconds value
pub fn cmd_psetex(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let ms = cmd.get_i64(1)?;
        let value = cmd.args[2].clone();

        if ms <= 0 {
            return Err(CommandError::InvalidExpireTime.into());
        }

        db.set_with_expiry(key, ViatorValue::string(value), Expiry::from_millis(ms));
        Ok(Frame::ok())
    })
}

/// MGET key [key ...]
/// Fast path: skips LRU tracking for better performance.
pub fn cmd_mget(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let values: Vec<Frame> = cmd
            .args
            .iter()
            .map(|arg| {
                let key = Key::from(arg.clone());
                // Use fast path that skips LRU tracking
                match db.get_string_fast(&key) {
                    Ok(Some(v)) => Frame::Bulk(v),
                    _ => Frame::Null,
                }
            })
            .collect();

        Ok(Frame::Array(values))
    })
}

/// MSET key value [key value ...]
pub fn cmd_mset(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        if cmd.args.len() % 2 != 0 {
            return Err(CommandError::WrongArity {
                command: "MSET".to_string(),
            }
            .into());
        }

        for chunk in cmd.args.chunks(2) {
            let key = Key::from(chunk[0].clone());
            let value = chunk[1].clone();
            db.set(key, ViatorValue::string(value));
        }

        Ok(Frame::ok())
    })
}

/// MSETNX key value [key value ...]
pub fn cmd_msetnx(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        if cmd.args.len() % 2 != 0 {
            return Err(CommandError::WrongArity {
                command: "MSETNX".to_string(),
            }
            .into());
        }

        // Check if any key exists
        for chunk in cmd.args.chunks(2) {
            let key = Key::from(chunk[0].clone());
            if db.exists(&key) {
                return Ok(Frame::Integer(0));
            }
        }

        // Set all keys
        for chunk in cmd.args.chunks(2) {
            let key = Key::from(chunk[0].clone());
            let value = chunk[1].clone();
            db.set(key, ViatorValue::string(value));
        }

        Ok(Frame::Integer(1))
    })
}

/// INCR key
pub fn cmd_incr(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move { incr_by(&db, &Key::from(cmd.args[0].clone()), 1) })
}

/// INCRBY key increment
pub fn cmd_incrby(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let increment = cmd.get_i64(1)?;
        incr_by(&db, &Key::from(cmd.args[0].clone()), increment)
    })
}

/// INCRBYFLOAT key increment
/// Fast path: skips LRU tracking for better performance.
pub fn cmd_incrbyfloat(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let increment = cmd.get_f64(1)?;

        // Use fast path for reads
        let current = match db.get_string_fast(&key)? {
            Some(v) => {
                let s = std::str::from_utf8(&v).map_err(|_| CommandError::NotFloat)?;
                s.parse::<f64>().map_err(|_| CommandError::NotFloat)?
            }
            None => 0.0,
        };

        let new_value = current + increment;

        // Check for infinity/NaN
        if new_value.is_infinite() || new_value.is_nan() {
            return Err(CommandError::OutOfRange.into());
        }

        let value_str = format_float(new_value);
        // Use fast path for writes
        db.set_fast(key, ViatorValue::string(value_str.clone()));
        Ok(Frame::Bulk(Bytes::from(value_str)))
    })
}

/// DECR key
pub fn cmd_decr(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move { incr_by(&db, &Key::from(cmd.args[0].clone()), -1) })
}

/// DECRBY key decrement
pub fn cmd_decrby(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let decrement = cmd.get_i64(1)?;
        incr_by(&db, &Key::from(cmd.args[0].clone()), -decrement)
    })
}

/// APPEND key value
pub fn cmd_append(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let append_value = &cmd.args[1];

        let new_len = match db.get_string(&key)? {
            Some(existing) => {
                let mut new_value = existing.to_vec();
                new_value.extend_from_slice(append_value);
                let len = new_value.len();
                db.set(key, ViatorValue::string(Bytes::from(new_value)));
                len
            }
            None => {
                let len = append_value.len();
                db.set(key, ViatorValue::string(append_value.clone()));
                len
            }
        };

        Ok(Frame::Integer(new_len as i64))
    })
}

/// STRLEN key
pub fn cmd_strlen(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());

        let len = match db.get_string(&key)? {
            Some(v) => v.len(),
            None => 0,
        };

        Ok(Frame::Integer(len as i64))
    })
}

/// GETRANGE key start end
pub fn cmd_getrange(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let start = cmd.get_i64(1)?;
        let end = cmd.get_i64(2)?;

        match db.get_string(&key)? {
            Some(value) => {
                let len = value.len() as i64;
                if len == 0 {
                    return Ok(Frame::Bulk(Bytes::new()));
                }

                // Normalize indices
                let start = if start < 0 {
                    (len + start).max(0) as usize
                } else {
                    (start as usize).min(value.len())
                };

                let end = if end < 0 {
                    (len + end).max(0) as usize
                } else {
                    (end as usize).min(value.len() - 1)
                };

                if start > end || start >= value.len() {
                    Ok(Frame::Bulk(Bytes::new()))
                } else {
                    Ok(Frame::Bulk(value.slice(start..=end)))
                }
            }
            None => Ok(Frame::Bulk(Bytes::new())),
        }
    })
}

/// SETRANGE key offset value
pub fn cmd_setrange(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let offset = cmd.get_i64(1)?;
        let value = &cmd.args[2];

        if offset < 0 {
            return Err(CommandError::OutOfRange.into());
        }

        let offset = offset as usize;

        let mut current = match db.get_string(&key)? {
            Some(v) => v.to_vec(),
            None => Vec::new(),
        };

        // Extend with zeros if needed
        if offset + value.len() > current.len() {
            current.resize(offset + value.len(), 0);
        }

        // Copy the value
        current[offset..offset + value.len()].copy_from_slice(value);

        let new_len = current.len();
        db.set(key, ViatorValue::string(Bytes::from(current)));

        Ok(Frame::Integer(new_len as i64))
    })
}

/// GETSET key value (deprecated, use SET with GET option)
pub fn cmd_getset(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let new_value = cmd.args[1].clone();

        let old_value = db.get_string(&key)?;
        db.set(key, ViatorValue::string(new_value));

        Ok(match old_value {
            Some(v) => Frame::Bulk(v),
            None => Frame::Null,
        })
    })
}

/// GETDEL key
pub fn cmd_getdel(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());

        let value = db.get_string(&key)?;
        if value.is_some() {
            db.delete(&key);
        }

        Ok(match value {
            Some(v) => Frame::Bulk(v),
            None => Frame::Null,
        })
    })
}

/// GETEX key [EXAT timestamp | PXAT timestamp | EX seconds | PX milliseconds | PERSIST]
pub fn cmd_getex(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());

        // Get current value
        let value = match db.get_string(&key)? {
            Some(v) => v,
            None => return Ok(Frame::Null),
        };

        // Parse options
        if cmd.args.len() > 1 {
            let opt = cmd.get_str(1)?.to_uppercase();
            match opt.as_str() {
                "EX" => {
                    let seconds = cmd.get_i64(2)?;
                    db.expire(&key, Expiry::from_seconds(seconds));
                }
                "PX" => {
                    let ms = cmd.get_i64(2)?;
                    db.expire(&key, Expiry::from_millis(ms));
                }
                "EXAT" => {
                    let ts = cmd.get_i64(2)?;
                    db.expire(&key, Expiry::at_seconds(ts));
                }
                "PXAT" => {
                    let ts = cmd.get_i64(2)?;
                    db.expire(&key, Expiry::at_millis(ts));
                }
                "PERSIST" => {
                    db.persist(&key);
                }
                _ => {
                    return Err(CommandError::InvalidArgument {
                        command: "GETEX".to_string(),
                        arg: opt,
                    }
                    .into());
                }
            }
        }

        Ok(Frame::Bulk(value))
    })
}

/// Helper function for INCR/INCRBY/DECR/DECRBY
/// Optimized: uses fast path that skips LRU tracking.
fn incr_by(db: &Db, key: &Key, delta: i64) -> Result<Frame> {
    // Use fast path for reads
    let current = match db.get_string_fast(key)? {
        Some(v) => {
            let s = std::str::from_utf8(&v).map_err(|_| CommandError::NotInteger)?;
            s.parse::<i64>().map_err(|_| CommandError::NotInteger)?
        }
        None => 0,
    };

    let new_value = current.checked_add(delta).ok_or(CommandError::OutOfRange)?;

    // Use fast path for writes
    db.set_fast(key.clone(), ViatorValue::string(new_value.to_string()));
    Ok(Frame::Integer(new_value))
}

/// Format a float for Redis output (remove trailing zeros).
fn format_float(n: f64) -> String {
    let s = format!("{n:.17}");
    // Trim trailing zeros after decimal point
    if s.contains('.') {
        let s = s.trim_end_matches('0');
        let s = s.trim_end_matches('.');
        s.to_string()
    } else {
        s
    }
}

/// SUBSTR key start end (deprecated alias for GETRANGE)
pub fn cmd_substr(
    cmd: ParsedCommand,
    db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    cmd_getrange(cmd, db, client)
}

/// LCS key1 key2 [LEN] [IDX] [MINMATCHLEN min-match-len] [WITHMATCHLEN]
pub fn cmd_lcs(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        let key1 = Key::from(cmd.args[0].clone());
        let key2 = Key::from(cmd.args[1].clone());

        let s1 = db.get_string(&key1)?.unwrap_or_default();
        let s2 = db.get_string(&key2)?.unwrap_or_default();

        // Parse options
        let mut len_only = false;
        let mut idx = false;
        let mut min_match_len: usize = 0;
        let mut with_match_len = false;

        let mut i = 2;
        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            match opt.as_str() {
                "LEN" => len_only = true,
                "IDX" => idx = true,
                "MINMATCHLEN" => {
                    i += 1;
                    min_match_len = cmd.get_u64(i)? as usize;
                }
                "WITHMATCHLEN" => with_match_len = true,
                _ => return Err(CommandError::SyntaxError.into()),
            }
            i += 1;
        }

        // Compute LCS using dynamic programming
        let (lcs_len, lcs_str, matches) = compute_lcs(&s1, &s2, min_match_len);

        if len_only {
            return Ok(Frame::Integer(lcs_len as i64));
        }

        if idx {
            // Return matches with indices
            let mut match_frames: Vec<Frame> = Vec::new();
            for (s1_start, s1_end, s2_start, s2_end, match_len) in matches {
                if match_len >= min_match_len {
                    let mut match_entry = vec![
                        Frame::Array(vec![
                            Frame::Integer(s1_start as i64),
                            Frame::Integer(s1_end as i64),
                        ]),
                        Frame::Array(vec![
                            Frame::Integer(s2_start as i64),
                            Frame::Integer(s2_end as i64),
                        ]),
                    ];
                    if with_match_len {
                        match_entry.push(Frame::Integer(match_len as i64));
                    }
                    match_frames.push(Frame::Array(match_entry));
                }
            }

            return Ok(Frame::Array(vec![
                Frame::bulk("matches"),
                Frame::Array(match_frames),
                Frame::bulk("len"),
                Frame::Integer(lcs_len as i64),
            ]));
        }

        Ok(Frame::Bulk(lcs_str))
    })
}

/// Compute LCS and return (length, lcs_string, matches)
fn compute_lcs(
    s1: &[u8],
    s2: &[u8],
    min_match_len: usize,
) -> (usize, Bytes, Vec<(usize, usize, usize, usize, usize)>) {
    let m = s1.len();
    let n = s2.len();

    if m == 0 || n == 0 {
        return (0, Bytes::new(), vec![]);
    }

    // Build LCS table
    let mut dp = vec![vec![0usize; n + 1]; m + 1];
    for i in 1..=m {
        for j in 1..=n {
            if s1[i - 1] == s2[j - 1] {
                dp[i][j] = dp[i - 1][j - 1] + 1;
            } else {
                dp[i][j] = dp[i - 1][j].max(dp[i][j - 1]);
            }
        }
    }

    // Backtrack to find LCS string
    let mut lcs = Vec::new();
    let mut i = m;
    let mut j = n;
    while i > 0 && j > 0 {
        if s1[i - 1] == s2[j - 1] {
            lcs.push(s1[i - 1]);
            i -= 1;
            j -= 1;
        } else if dp[i - 1][j] > dp[i][j - 1] {
            i -= 1;
        } else {
            j -= 1;
        }
    }
    lcs.reverse();

    // Find matches (contiguous matching regions)
    let mut matches = Vec::new();
    let mut i = m;
    let mut j = n;
    while i > 0 && j > 0 {
        if s1[i - 1] == s2[j - 1] {
            let end_i = i - 1;
            let end_j = j - 1;
            let mut len = 0;
            while i > 0 && j > 0 && s1[i - 1] == s2[j - 1] {
                i -= 1;
                j -= 1;
                len += 1;
            }
            if len >= min_match_len {
                matches.push((i, end_i, j, end_j, len));
            }
        } else if dp[i - 1][j] > dp[i][j - 1] {
            i -= 1;
        } else {
            j -= 1;
        }
    }

    (dp[m][n], Bytes::from(lcs), matches)
}

/// MSETEX numkeys key value [key value ...] [NX | XX] [EX seconds | PX milliseconds | EXAT timestamp | PXAT timestamp | KEEPTTL]
/// Atomically set multiple string keys with expiration (Redis 8.4+).
pub fn cmd_msetex(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?; // numkeys + at least one key-value pair

        let numkeys = cmd.get_u64(0)? as usize;
        if numkeys == 0 {
            return Err(CommandError::SyntaxError.into());
        }

        // Calculate where key-value pairs end
        let pairs_end = 1 + (numkeys * 2);
        if cmd.args.len() < pairs_end {
            return Err(CommandError::WrongArity {
                command: "MSETEX".to_string(),
            }
            .into());
        }

        // Parse key-value pairs
        let pairs = &cmd.args[1..pairs_end];
        if pairs.len() != numkeys * 2 {
            return Err(CommandError::WrongArity {
                command: "MSETEX".to_string(),
            }
            .into());
        }

        // Parse options
        let mut nx = false;
        let mut xx = false;
        let mut expiry = Expiry::Never;
        let mut keepttl = false;

        let mut i = pairs_end;
        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            match opt.as_str() {
                "NX" => {
                    nx = true;
                    i += 1;
                }
                "XX" => {
                    xx = true;
                    i += 1;
                }
                "EX" => {
                    i += 1;
                    let seconds = cmd.get_i64(i)?;
                    if seconds <= 0 {
                        return Err(CommandError::InvalidExpireTime.into());
                    }
                    expiry = Expiry::from_seconds(seconds);
                    i += 1;
                }
                "PX" => {
                    i += 1;
                    let ms = cmd.get_i64(i)?;
                    if ms <= 0 {
                        return Err(CommandError::InvalidExpireTime.into());
                    }
                    expiry = Expiry::from_millis(ms);
                    i += 1;
                }
                "EXAT" => {
                    i += 1;
                    let ts = cmd.get_i64(i)?;
                    expiry = Expiry::at_seconds(ts);
                    i += 1;
                }
                "PXAT" => {
                    i += 1;
                    let ts = cmd.get_i64(i)?;
                    expiry = Expiry::at_millis(ts);
                    i += 1;
                }
                "KEEPTTL" => {
                    keepttl = true;
                    i += 1;
                }
                _ => {
                    return Err(CommandError::SyntaxError.into());
                }
            }
        }

        // NX and XX are mutually exclusive
        if nx && xx {
            return Err(CommandError::SyntaxError.into());
        }

        // Check NX/XX conditions
        if nx {
            // All keys must not exist
            for chunk in pairs.chunks(2) {
                let key = Key::from(chunk[0].clone());
                if db.exists(&key) {
                    return Ok(Frame::Integer(0));
                }
            }
        }

        if xx {
            // All keys must exist
            for chunk in pairs.chunks(2) {
                let key = Key::from(chunk[0].clone());
                if !db.exists(&key) {
                    return Ok(Frame::Integer(0));
                }
            }
        }

        // Set all key-value pairs
        for chunk in pairs.chunks(2) {
            let key = Key::from(chunk[0].clone());
            let value = chunk[1].clone();

            let final_expiry = if keepttl {
                // Retain existing TTL
                if let Some(pttl) = db.pttl(&key) {
                    if pttl > 0 {
                        Expiry::from_millis(pttl)
                    } else {
                        expiry
                    }
                } else {
                    expiry
                }
            } else {
                expiry
            };

            db.set_with_expiry(key, ViatorValue::string(value), final_expiry);
        }

        Ok(Frame::Integer(1))
    })
}

/// DELEX key [IFEQ value | IFNE value | IFDEQ digest | IFDNE digest]
/// Conditionally delete a key based on value or digest comparison (Redis 8.4+).
pub fn cmd_delex(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;

        let key = Key::from(cmd.args[0].clone());

        // Parse condition
        enum DelexCondition {
            None,
            IfEq(Bytes),
            IfNe(Bytes),
            IfDeq(u64), // XXH3 digest
            IfDne(u64), // XXH3 digest
        }

        let condition = if cmd.args.len() >= 3 {
            let opt = cmd.get_str(1)?.to_uppercase();
            match opt.as_str() {
                "IFEQ" => DelexCondition::IfEq(cmd.args[2].clone()),
                "IFNE" => DelexCondition::IfNe(cmd.args[2].clone()),
                "IFDEQ" => {
                    // Parse hex digest
                    let digest_str = cmd.get_str(2)?;
                    let digest = u64::from_str_radix(digest_str, 16)
                        .map_err(|_| CommandError::SyntaxError)?;
                    DelexCondition::IfDeq(digest)
                }
                "IFDNE" => {
                    let digest_str = cmd.get_str(2)?;
                    let digest = u64::from_str_radix(digest_str, 16)
                        .map_err(|_| CommandError::SyntaxError)?;
                    DelexCondition::IfDne(digest)
                }
                _ => return Err(CommandError::SyntaxError.into()),
            }
        } else if cmd.args.len() == 1 {
            // No condition - delete unconditionally
            DelexCondition::None
        } else {
            return Err(CommandError::SyntaxError.into());
        };

        match condition {
            DelexCondition::None => {
                // Delete unconditionally
                if db.delete(&key) {
                    Ok(Frame::Integer(1))
                } else {
                    Ok(Frame::Integer(0))
                }
            }
            DelexCondition::IfEq(expected) => match db.get_string(&key)? {
                Some(current) if current.as_ref() == expected.as_ref() => {
                    db.delete(&key);
                    Ok(Frame::Integer(1))
                }
                Some(_) => Ok(Frame::Integer(0)),
                None => Ok(Frame::Integer(0)),
            },
            DelexCondition::IfNe(not_expected) => match db.get_string(&key)? {
                Some(current) if current.as_ref() != not_expected.as_ref() => {
                    db.delete(&key);
                    Ok(Frame::Integer(1))
                }
                Some(_) => Ok(Frame::Integer(0)),
                None => Ok(Frame::Integer(0)),
            },
            DelexCondition::IfDeq(expected_digest) => match db.get_string(&key)? {
                Some(current) => {
                    let current_digest = xxhash_rust::xxh3::xxh3_64(&current);
                    if current_digest == expected_digest {
                        db.delete(&key);
                        Ok(Frame::Integer(1))
                    } else {
                        Ok(Frame::Integer(0))
                    }
                }
                None => Ok(Frame::Integer(0)),
            },
            DelexCondition::IfDne(not_expected_digest) => match db.get_string(&key)? {
                Some(current) => {
                    let current_digest = xxhash_rust::xxh3::xxh3_64(&current);
                    if current_digest != not_expected_digest {
                        db.delete(&key);
                        Ok(Frame::Integer(1))
                    } else {
                        Ok(Frame::Integer(0))
                    }
                }
                None => Ok(Frame::Integer(0)),
            },
        }
    })
}

/// DIGEST key - Return XXH3 digest of value (Redis 8.4+)
/// Returns the hash digest as a hexadecimal string using XXH3 algorithm.
pub fn cmd_digest(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(1)?;

        let key = Key::from(cmd.args[0].clone());

        match db.get_string(&key)? {
            Some(value) => {
                // Redis 8.4 uses XXH3 algorithm
                let hash = xxhash_rust::xxh3::xxh3_64(&value);
                let hex = format!("{hash:016x}");
                Ok(Frame::Bulk(Bytes::from(hex)))
            }
            None => Ok(Frame::Null),
        }
    })
}
