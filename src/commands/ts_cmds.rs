//! TimeSeries command implementations.
//!
//! Redis Stack compatible TS.* commands.

use super::ParsedCommand;
use crate::Result;
use crate::error::CommandError;
use crate::protocol::Frame;
use crate::server::ClientState;
use crate::storage::Db;
use crate::types::{Aggregation, DuplicatePolicy, TimeSeries};
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Global storage for time series.
static TIMESERIES: std::sync::LazyLock<RwLock<HashMap<Vec<u8>, TimeSeries>>> =
    std::sync::LazyLock::new(|| RwLock::new(HashMap::new()));

/// Compaction rule definition.
#[derive(Debug, Clone)]
struct CompactionRule {
    /// Source key
    source_key: Vec<u8>,
    /// Destination key
    dest_key: Vec<u8>,
    /// Aggregation type
    aggregation: Aggregation,
    /// Bucket duration in milliseconds
    bucket_duration: u64,
    /// Alignment timestamp (optional)
    align_timestamp: u64,
}

/// Global storage for compaction rules.
static COMPACTION_RULES: std::sync::LazyLock<RwLock<Vec<CompactionRule>>> =
    std::sync::LazyLock::new(|| RwLock::new(Vec::new()));

/// TS.CREATE key [RETENTION retentionPeriod] [ENCODING enc] [CHUNK_SIZE size] [DUPLICATE_POLICY policy] [LABELS label value ...]
/// Create a time series.
pub fn cmd_ts_create(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;
        let key = cmd.args[0].to_vec();

        let mut retention = 0u64;
        let mut duplicate_policy = DuplicatePolicy::Block;
        let mut labels: HashMap<Bytes, Bytes> = HashMap::new();

        let mut i = 1;
        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            match opt.as_str() {
                "RETENTION" => {
                    i += 1;
                    retention = cmd.get_u64(i)?;
                }
                "ENCODING" | "CHUNK_SIZE" => {
                    i += 1; // Skip value, not used in this implementation
                }
                "DUPLICATE_POLICY" => {
                    i += 1;
                    let policy_str = cmd.get_str(i)?;
                    duplicate_policy =
                        DuplicatePolicy::from_str(policy_str).ok_or(CommandError::SyntaxError)?;
                }
                "LABELS" => {
                    i += 1;
                    while i + 1 < cmd.args.len() {
                        let label_key = cmd.args[i].clone();
                        let label_value = cmd.args[i + 1].clone();
                        labels.insert(label_key, label_value);
                        i += 2;
                    }
                }
                _ => {}
            }
            i += 1;
        }

        let mut series = TIMESERIES.write();
        if series.contains_key(&key) {
            return Err(CommandError::KeyExists.into());
        }

        let mut ts = TimeSeries::with_retention(retention);
        ts.set_duplicate_policy(duplicate_policy);
        ts.set_labels(labels);
        series.insert(key, ts);

        Ok(Frame::ok())
    })
}

/// TS.ADD key timestamp value [RETENTION retentionPeriod] [ENCODING enc] [CHUNK_SIZE size] [ON_DUPLICATE policy] [LABELS label value ...]
/// Add a sample to a time series.
pub fn cmd_ts_add(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;
        let key = cmd.args[0].to_vec();

        // Parse timestamp - can be * for auto
        let timestamp = if cmd.get_str(1)? == "*" {
            0 // Will be auto-generated
        } else {
            cmd.get_u64(1)?
        };

        let value: f64 = cmd
            .get_str(2)?
            .parse()
            .map_err(|_| CommandError::NotFloat)?;

        // Parse optional arguments
        let mut retention: Option<u64> = None;
        let mut on_duplicate: Option<DuplicatePolicy> = None;
        let mut labels: HashMap<Bytes, Bytes> = HashMap::new();

        let mut i = 3;
        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            match opt.as_str() {
                "RETENTION" => {
                    i += 1;
                    retention = Some(cmd.get_u64(i)?);
                }
                "ON_DUPLICATE" => {
                    i += 1;
                    let policy_str = cmd.get_str(i)?;
                    on_duplicate = DuplicatePolicy::from_str(policy_str);
                }
                "LABELS" => {
                    i += 1;
                    while i + 1 < cmd.args.len() {
                        let label_key = cmd.args[i].clone();
                        let label_value = cmd.args[i + 1].clone();
                        labels.insert(label_key, label_value);
                        i += 2;
                    }
                }
                _ => {}
            }
            i += 1;
        }

        let mut series = TIMESERIES.write();
        let ts = series.entry(key).or_default();

        if let Some(ret) = retention {
            ts.set_retention(ret);
        }
        if let Some(policy) = on_duplicate {
            ts.set_duplicate_policy(policy);
        }
        if !labels.is_empty() {
            ts.set_labels(labels);
        }

        match ts.add(timestamp, value) {
            Ok(actual_ts) => Ok(Frame::Integer(actual_ts as i64)),
            Err(_) => Err(CommandError::SyntaxError.into()),
        }
    })
}

/// TS.MADD key timestamp value [key timestamp value ...]
/// Add multiple samples to time series.
pub fn cmd_ts_madd(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;
        if cmd.args.len() % 3 != 0 {
            return Err(CommandError::WrongArity {
                command: "TS.MADD".to_string(),
            }
            .into());
        }

        let mut series = TIMESERIES.write();
        let mut results = Vec::new();

        let mut i = 0;
        while i < cmd.args.len() {
            let key = cmd.args[i].to_vec();
            let timestamp = if cmd.get_str(i + 1)? == "*" {
                0
            } else {
                cmd.get_u64(i + 1)?
            };
            let value: f64 = cmd
                .get_str(i + 2)?
                .parse()
                .map_err(|_| CommandError::NotFloat)?;

            let ts = series.entry(key).or_default();
            match ts.add(timestamp, value) {
                Ok(actual_ts) => results.push(Frame::Integer(actual_ts as i64)),
                Err(_) => results.push(Frame::error("TSDB: duplicate timestamp")),
            }

            i += 3;
        }

        Ok(Frame::Array(results))
    })
}

/// TS.GET key [LATEST]
/// Get the latest sample from a time series.
pub fn cmd_ts_get(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;
        let key = cmd.args[0].to_vec();

        let series = TIMESERIES.read();
        let ts = series.get(&key).ok_or(CommandError::NoSuchKey)?;

        match ts.get() {
            Some(sample) => Ok(Frame::Array(vec![
                Frame::Integer(sample.timestamp as i64),
                Frame::Bulk(format!("{}", sample.value).into()),
            ])),
            None => Ok(Frame::Array(vec![])),
        }
    })
}

/// TS.MGET [LATEST] [WITHLABELS | SELECTED_LABELS label...] FILTER filterExpr...
/// Get the latest samples from multiple time series.
pub fn cmd_ts_mget(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        let mut with_labels = false;
        let mut _filter_start = 0;

        for i in 0..cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            match opt.as_str() {
                "WITHLABELS" | "SELECTED_LABELS" => {
                    with_labels = true;
                }
                "FILTER" => {
                    _filter_start = i + 1;
                    break;
                }
                _ => {}
            }
        }

        // Simple filter implementation - just match all for now
        let series = TIMESERIES.read();
        let mut results = Vec::new();

        for (key, ts) in series.iter() {
            if let Some(sample) = ts.get() {
                let mut entry = vec![Frame::Bulk(Bytes::from(key.clone()))];

                if with_labels {
                    let labels: Vec<Frame> = ts
                        .labels()
                        .iter()
                        .flat_map(|(k, v)| vec![Frame::Bulk(k.clone()), Frame::Bulk(v.clone())])
                        .collect();
                    entry.push(Frame::Array(labels));
                } else {
                    entry.push(Frame::Array(vec![]));
                }

                entry.push(Frame::Array(vec![
                    Frame::Integer(sample.timestamp as i64),
                    Frame::Bulk(format!("{}", sample.value).into()),
                ]));

                results.push(Frame::Array(entry));
            }
        }

        Ok(Frame::Array(results))
    })
}

/// TS.RANGE key fromTimestamp toTimestamp [LATEST] [FILTER_BY_TS ts...] [FILTER_BY_VALUE min max] [COUNT count] [AGGREGATION aggregator bucketDuration]
/// Query a range of samples from a time series.
pub fn cmd_ts_range(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;
        let key = cmd.args[0].to_vec();

        let from = if cmd.get_str(1)? == "-" {
            0
        } else {
            cmd.get_u64(1)?
        };

        let to = if cmd.get_str(2)? == "+" {
            u64::MAX
        } else {
            cmd.get_u64(2)?
        };

        let mut count: Option<usize> = None;
        let mut aggregation: Option<Aggregation> = None;
        let mut bucket_duration = 0u64;

        let mut i = 3;
        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            match opt.as_str() {
                "COUNT" => {
                    i += 1;
                    count = Some(cmd.get_u64(i)? as usize);
                }
                "AGGREGATION" => {
                    i += 1;
                    let agg_str = cmd.get_str(i)?;
                    aggregation = Aggregation::from_str(agg_str);
                    i += 1;
                    bucket_duration = cmd.get_u64(i)?;
                }
                _ => {}
            }
            i += 1;
        }

        let series = TIMESERIES.read();
        let ts = series.get(&key).ok_or(CommandError::NoSuchKey)?;

        let samples = if let Some(agg) = aggregation {
            ts.range_aggregated(from, to, bucket_duration, agg)
        } else {
            ts.range(from, to, count)
        };

        let results: Vec<Frame> = samples
            .iter()
            .map(|s| {
                Frame::Array(vec![
                    Frame::Integer(s.timestamp as i64),
                    Frame::Bulk(format!("{}", s.value).into()),
                ])
            })
            .collect();

        Ok(Frame::Array(results))
    })
}

/// TS.REVRANGE key fromTimestamp toTimestamp [options...]
/// Query a range of samples in reverse order.
pub fn cmd_ts_revrange(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;
        let key = cmd.args[0].to_vec();

        let from = if cmd.get_str(1)? == "-" {
            0
        } else {
            cmd.get_u64(1)?
        };

        let to = if cmd.get_str(2)? == "+" {
            u64::MAX
        } else {
            cmd.get_u64(2)?
        };

        let mut count: Option<usize> = None;

        let mut i = 3;
        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            if opt == "COUNT" {
                i += 1;
                count = Some(cmd.get_u64(i)? as usize);
            }
            i += 1;
        }

        let series = TIMESERIES.read();
        let ts = series.get(&key).ok_or(CommandError::NoSuchKey)?;

        let samples = ts.rev_range(from, to, count);

        let results: Vec<Frame> = samples
            .iter()
            .map(|s| {
                Frame::Array(vec![
                    Frame::Integer(s.timestamp as i64),
                    Frame::Bulk(format!("{}", s.value).into()),
                ])
            })
            .collect();

        Ok(Frame::Array(results))
    })
}

/// TS.INCRBY key value [TIMESTAMP timestamp] [RETENTION retentionPeriod] [LABELS label value ...]
/// Increment the latest sample value.
pub fn cmd_ts_incrby(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;
        let key = cmd.args[0].to_vec();
        let value: f64 = cmd
            .get_str(1)?
            .parse()
            .map_err(|_| CommandError::NotFloat)?;

        let mut series = TIMESERIES.write();
        let ts = series.entry(key).or_default();

        match ts.incr(value) {
            Ok(timestamp) => Ok(Frame::Integer(timestamp as i64)),
            Err(_) => Err(CommandError::SyntaxError.into()),
        }
    })
}

/// TS.DECRBY key value [TIMESTAMP timestamp] [RETENTION retentionPeriod] [LABELS label value ...]
/// Decrement the latest sample value.
pub fn cmd_ts_decrby(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;
        let key = cmd.args[0].to_vec();
        let value: f64 = cmd
            .get_str(1)?
            .parse()
            .map_err(|_| CommandError::NotFloat)?;

        let mut series = TIMESERIES.write();
        let ts = series.entry(key).or_default();

        match ts.decr(value) {
            Ok(timestamp) => Ok(Frame::Integer(timestamp as i64)),
            Err(_) => Err(CommandError::SyntaxError.into()),
        }
    })
}

/// TS.DEL key fromTimestamp toTimestamp
/// Delete samples from a time series.
pub fn cmd_ts_del(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(3)?;
        let key = cmd.args[0].to_vec();
        let from = cmd.get_u64(1)?;
        let to = cmd.get_u64(2)?;

        let mut series = TIMESERIES.write();
        let ts = series.get_mut(&key).ok_or(CommandError::NoSuchKey)?;

        let deleted = ts.del(from, to);
        Ok(Frame::Integer(deleted as i64))
    })
}

/// TS.INFO key [DEBUG]
/// Get information about a time series.
pub fn cmd_ts_info(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;
        let key = cmd.args[0].to_vec();

        let series = TIMESERIES.read();
        let ts = series.get(&key).ok_or(CommandError::NoSuchKey)?;

        let info = ts.info();
        let labels: Vec<Frame> = info
            .labels
            .iter()
            .flat_map(|(k, v)| vec![Frame::Bulk(k.clone()), Frame::Bulk(v.clone())])
            .collect();

        let policy_str = match info.duplicate_policy {
            DuplicatePolicy::Block => "block",
            DuplicatePolicy::First => "first",
            DuplicatePolicy::Last => "last",
            DuplicatePolicy::Min => "min",
            DuplicatePolicy::Max => "max",
            DuplicatePolicy::Sum => "sum",
        };

        Ok(Frame::Array(vec![
            Frame::bulk("totalSamples"),
            Frame::Integer(info.total_samples as i64),
            Frame::bulk("memoryUsage"),
            Frame::Integer(info.memory_usage as i64),
            Frame::bulk("firstTimestamp"),
            Frame::Integer(info.first_timestamp as i64),
            Frame::bulk("lastTimestamp"),
            Frame::Integer(info.last_timestamp as i64),
            Frame::bulk("retentionTime"),
            Frame::Integer(info.retention as i64),
            Frame::bulk("chunkCount"),
            Frame::Integer(info.chunk_count as i64),
            Frame::bulk("chunkSize"),
            Frame::Integer(info.chunk_size as i64),
            Frame::bulk("labels"),
            Frame::Array(labels),
            Frame::bulk("duplicatePolicy"),
            Frame::bulk(policy_str),
        ]))
    })
}

/// TS.ALTER key [RETENTION retentionPeriod] [CHUNK_SIZE size] [DUPLICATE_POLICY policy] [LABELS label value ...]
/// Alter a time series configuration.
pub fn cmd_ts_alter(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;
        let key = cmd.args[0].to_vec();

        let mut series = TIMESERIES.write();
        let ts = series.get_mut(&key).ok_or(CommandError::NoSuchKey)?;

        let mut i = 1;
        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            match opt.as_str() {
                "RETENTION" => {
                    i += 1;
                    ts.set_retention(cmd.get_u64(i)?);
                }
                "DUPLICATE_POLICY" => {
                    i += 1;
                    if let Some(policy) = DuplicatePolicy::from_str(cmd.get_str(i)?) {
                        ts.set_duplicate_policy(policy);
                    }
                }
                "LABELS" => {
                    i += 1;
                    let mut labels = HashMap::new();
                    while i + 1 < cmd.args.len() {
                        let label_key = cmd.args[i].clone();
                        let label_value = cmd.args[i + 1].clone();
                        labels.insert(label_key, label_value);
                        i += 2;
                    }
                    ts.set_labels(labels);
                }
                _ => {}
            }
            i += 1;
        }

        Ok(Frame::ok())
    })
}

/// TS.CREATERULE sourceKey destKey AGGREGATION aggregationType bucketDuration [alignTimestamp]
/// Create a compaction rule.
pub fn cmd_ts_createrule(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(5)?;
        let source_key = cmd.args[0].to_vec();
        let dest_key = cmd.args[1].to_vec();

        // Expect AGGREGATION keyword
        if cmd.get_str(2)?.to_uppercase() != "AGGREGATION" {
            return Err(CommandError::SyntaxError.into());
        }

        let aggregation =
            Aggregation::from_str(cmd.get_str(3)?).ok_or(CommandError::SyntaxError)?;
        let bucket_duration = cmd.get_u64(4)?;

        let align_timestamp = if cmd.args.len() > 5 {
            cmd.get_u64(5)?
        } else {
            0
        };

        // Check that both source and dest exist
        {
            let series = TIMESERIES.read();
            if !series.contains_key(&source_key) {
                return Err(CommandError::NoSuchKey.into());
            }
            if !series.contains_key(&dest_key) {
                return Err(CommandError::NoSuchKey.into());
            }
        }

        // Check for duplicate rule
        {
            let rules = COMPACTION_RULES.read();
            for rule in rules.iter() {
                if rule.source_key == source_key && rule.dest_key == dest_key {
                    return Err(CommandError::KeyExists.into());
                }
            }
        }

        // Add the rule
        let rule = CompactionRule {
            source_key,
            dest_key,
            aggregation,
            bucket_duration,
            align_timestamp,
        };

        COMPACTION_RULES.write().push(rule);
        Ok(Frame::ok())
    })
}

/// TS.DELETERULE sourceKey destKey
/// Delete a compaction rule.
pub fn cmd_ts_deleterule(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(2)?;
        let source_key = cmd.args[0].to_vec();
        let dest_key = cmd.args[1].to_vec();

        let mut rules = COMPACTION_RULES.write();
        let initial_len = rules.len();
        rules.retain(|r| !(r.source_key == source_key && r.dest_key == dest_key));

        if rules.len() == initial_len {
            return Err(CommandError::NoSuchKey.into());
        }

        Ok(Frame::ok())
    })
}

/// TS.MRANGE fromTimestamp toTimestamp [LATEST] [FILTER_BY_TS ts...] [FILTER_BY_VALUE min max]
///   [WITHLABELS | SELECTED_LABELS label...] [COUNT count] [AGGREGATION aggregator bucketDuration]
///   FILTER filterExpr...
/// Query a range from multiple time series.
pub fn cmd_ts_mrange(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;

        let from = if cmd.get_str(0)? == "-" {
            0
        } else {
            cmd.get_u64(0)?
        };

        let to = if cmd.get_str(1)? == "+" {
            u64::MAX
        } else {
            cmd.get_u64(1)?
        };

        let mut count: Option<usize> = None;
        let mut aggregation: Option<Aggregation> = None;
        let mut bucket_duration = 0u64;
        let mut with_labels = false;
        let mut filter_labels: Vec<(Bytes, Option<Bytes>)> = Vec::new();

        let mut i = 2;
        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            match opt.as_str() {
                "COUNT" => {
                    i += 1;
                    count = Some(cmd.get_u64(i)? as usize);
                }
                "AGGREGATION" => {
                    i += 1;
                    aggregation = Aggregation::from_str(cmd.get_str(i)?);
                    i += 1;
                    bucket_duration = cmd.get_u64(i)?;
                }
                "WITHLABELS" => {
                    with_labels = true;
                }
                "FILTER" => {
                    i += 1;
                    // Parse filter expressions (simplified: label=value or label!=value)
                    while i < cmd.args.len() {
                        let expr = cmd.get_str(i)?;
                        if let Some(eq_pos) = expr.find('=') {
                            let label = Bytes::copy_from_slice(expr[..eq_pos].as_bytes());
                            let value = if expr.chars().nth(eq_pos + 1) == Some('=') {
                                // label==value (exact match not supported, treat as =)
                                None
                            } else {
                                Some(Bytes::copy_from_slice(expr[eq_pos + 1..].as_bytes()))
                            };
                            filter_labels.push((label, value));
                        }
                        i += 1;
                    }
                }
                _ => {}
            }
            i += 1;
        }

        let series = TIMESERIES.read();
        let mut results = Vec::new();

        for (key, ts) in series.iter() {
            // Check filter
            let mut matches = filter_labels.is_empty();
            if !matches {
                for (label, value) in &filter_labels {
                    if let Some(ts_value) = ts.labels().get(label) {
                        if value.is_none() || value.as_ref() == Some(ts_value) {
                            matches = true;
                            break;
                        }
                    }
                }
            }

            if matches {
                let samples = if let Some(agg) = aggregation {
                    ts.range_aggregated(from, to, bucket_duration, agg)
                } else {
                    ts.range(from, to, count)
                };

                let mut entry = vec![Frame::Bulk(Bytes::from(key.clone()))];

                if with_labels {
                    let labels: Vec<Frame> = ts
                        .labels()
                        .iter()
                        .flat_map(|(k, v)| vec![Frame::Bulk(k.clone()), Frame::Bulk(v.clone())])
                        .collect();
                    entry.push(Frame::Array(labels));
                } else {
                    entry.push(Frame::Array(vec![]));
                }

                let sample_frames: Vec<Frame> = samples
                    .iter()
                    .map(|s| {
                        Frame::Array(vec![
                            Frame::Integer(s.timestamp as i64),
                            Frame::Bulk(format!("{}", s.value).into()),
                        ])
                    })
                    .collect();
                entry.push(Frame::Array(sample_frames));

                results.push(Frame::Array(entry));
            }
        }

        Ok(Frame::Array(results))
    })
}

/// TS.MREVRANGE fromTimestamp toTimestamp [options...] FILTER filterExpr...
/// Query a range from multiple time series in reverse order.
pub fn cmd_ts_mrevrange(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;

        let from = if cmd.get_str(0)? == "-" {
            0
        } else {
            cmd.get_u64(0)?
        };

        let to = if cmd.get_str(1)? == "+" {
            u64::MAX
        } else {
            cmd.get_u64(1)?
        };

        let mut count: Option<usize> = None;
        let mut with_labels = false;
        let mut filter_labels: Vec<(Bytes, Option<Bytes>)> = Vec::new();

        let mut i = 2;
        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            match opt.as_str() {
                "COUNT" => {
                    i += 1;
                    count = Some(cmd.get_u64(i)? as usize);
                }
                "WITHLABELS" => {
                    with_labels = true;
                }
                "FILTER" => {
                    i += 1;
                    while i < cmd.args.len() {
                        let expr = cmd.get_str(i)?;
                        if let Some(eq_pos) = expr.find('=') {
                            let label = Bytes::copy_from_slice(expr[..eq_pos].as_bytes());
                            let value = Some(Bytes::copy_from_slice(expr[eq_pos + 1..].as_bytes()));
                            filter_labels.push((label, value));
                        }
                        i += 1;
                    }
                }
                _ => {}
            }
            i += 1;
        }

        let series = TIMESERIES.read();
        let mut results = Vec::new();

        for (key, ts) in series.iter() {
            // Check filter
            let mut matches = filter_labels.is_empty();
            if !matches {
                for (label, value) in &filter_labels {
                    if let Some(ts_value) = ts.labels().get(label) {
                        if value.is_none() || value.as_ref() == Some(ts_value) {
                            matches = true;
                            break;
                        }
                    }
                }
            }

            if matches {
                let samples = ts.rev_range(from, to, count);

                let mut entry = vec![Frame::Bulk(Bytes::from(key.clone()))];

                if with_labels {
                    let labels: Vec<Frame> = ts
                        .labels()
                        .iter()
                        .flat_map(|(k, v)| vec![Frame::Bulk(k.clone()), Frame::Bulk(v.clone())])
                        .collect();
                    entry.push(Frame::Array(labels));
                } else {
                    entry.push(Frame::Array(vec![]));
                }

                let sample_frames: Vec<Frame> = samples
                    .iter()
                    .map(|s| {
                        Frame::Array(vec![
                            Frame::Integer(s.timestamp as i64),
                            Frame::Bulk(format!("{}", s.value).into()),
                        ])
                    })
                    .collect();
                entry.push(Frame::Array(sample_frames));

                results.push(Frame::Array(entry));
            }
        }

        Ok(Frame::Array(results))
    })
}

/// TS.QUERYINDEX filterExpr...
/// Query time series keys by filter expressions.
pub fn cmd_ts_queryindex(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;

        // Parse filter expressions
        let mut filter_labels: Vec<(Bytes, Option<Bytes>, bool)> = Vec::new(); // (label, value, is_not_equal)

        for i in 0..cmd.args.len() {
            let expr = cmd.get_str(i)?;

            if let Some(neq_pos) = expr.find("!=") {
                let label = Bytes::copy_from_slice(expr[..neq_pos].as_bytes());
                let value = Some(Bytes::copy_from_slice(expr[neq_pos + 2..].as_bytes()));
                filter_labels.push((label, value, true)); // not equal
            } else if let Some(eq_pos) = expr.find('=') {
                let label = Bytes::copy_from_slice(expr[..eq_pos].as_bytes());
                let value = Some(Bytes::copy_from_slice(expr[eq_pos + 1..].as_bytes()));
                filter_labels.push((label, value, false)); // equal
            }
        }

        let series = TIMESERIES.read();
        let mut results = Vec::new();

        for (key, ts) in series.iter() {
            let mut matches = true;

            for (label, value, is_not_equal) in &filter_labels {
                let ts_value = ts.labels().get(label);

                if *is_not_equal {
                    // != : should NOT have this label=value
                    if let Some(tv) = ts_value {
                        if value.as_ref() == Some(tv) {
                            matches = false;
                            break;
                        }
                    }
                } else {
                    // = : should have this label=value
                    match (ts_value, value) {
                        (Some(tv), Some(v)) if tv == v => {}
                        (Some(_), None) => {} // Just checking label exists
                        _ => {
                            matches = false;
                            break;
                        }
                    }
                }
            }

            if matches {
                results.push(Frame::Bulk(Bytes::from(key.clone())));
            }
        }

        Ok(Frame::Array(results))
    })
}
