//! T-Digest command implementations.
//!
//! Redis Stack compatible TDIGEST.* commands.

use super::ParsedCommand;
use crate::Result;
use crate::error::CommandError;
use crate::protocol::Frame;
use crate::server::ClientState;
use crate::storage::Db;
use crate::types::TDigest;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Global storage for T-Digests.
static TDIGESTS: std::sync::LazyLock<RwLock<HashMap<Vec<u8>, TDigest>>> =
    std::sync::LazyLock::new(|| RwLock::new(HashMap::new()));

/// TDIGEST.CREATE key [COMPRESSION compression]
/// Create a T-Digest structure.
pub fn cmd_tdigest_create(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;
        let key = cmd.args[0].to_vec();

        let mut compression = 100.0;

        let mut i = 1;
        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            if opt == "COMPRESSION" {
                i += 1;
                compression = cmd
                    .get_str(i)?
                    .parse()
                    .map_err(|_| CommandError::NotFloat)?;
            }
            i += 1;
        }

        let mut digests = TDIGESTS.write();
        if digests.contains_key(&key) {
            return Err(CommandError::KeyExists.into());
        }

        digests.insert(key, TDigest::new(compression));
        Ok(Frame::ok())
    })
}

/// TDIGEST.RESET key
/// Reset a T-Digest structure.
pub fn cmd_tdigest_reset(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(1)?;
        let key = cmd.args[0].to_vec();

        let mut digests = TDIGESTS.write();
        let digest = digests.get_mut(&key).ok_or(CommandError::NoSuchKey)?;
        digest.reset();

        Ok(Frame::ok())
    })
}

/// TDIGEST.ADD key value [value ...]
/// Add values to a T-Digest.
pub fn cmd_tdigest_add(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;
        let key = cmd.args[0].to_vec();

        let mut digests = TDIGESTS.write();
        let digest = digests
            .entry(key)
            .or_insert_with(TDigest::default_compression);

        for i in 1..cmd.args.len() {
            let value: f64 = cmd
                .get_str(i)?
                .parse()
                .map_err(|_| CommandError::NotFloat)?;
            digest.add(value);
        }

        Ok(Frame::ok())
    })
}

/// TDIGEST.MERGE destkey [COMPRESSION compression] OVERRIDE numkeys key [key ...]
/// Merge multiple T-Digests.
pub fn cmd_tdigest_merge(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;
        let dest_key = cmd.args[0].to_vec();

        let mut compression: Option<f64> = None;
        let mut num_keys = 0usize;
        let mut source_start = 1;

        let mut i = 1;
        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            match opt.as_str() {
                "COMPRESSION" => {
                    i += 1;
                    compression = Some(
                        cmd.get_str(i)?
                            .parse()
                            .map_err(|_| CommandError::NotFloat)?,
                    );
                }
                "OVERRIDE" => {
                    // OVERRIDE flag (ignored - we always override)
                }
                _ => {
                    // First number is numkeys
                    if let Ok(n) = cmd.get_u64(i) {
                        num_keys = n as usize;
                        source_start = i + 1;
                        break;
                    }
                }
            }
            i += 1;
        }

        if num_keys == 0 || source_start + num_keys > cmd.args.len() {
            return Err(CommandError::WrongArity {
                command: "TDIGEST.MERGE".to_string(),
            }
            .into());
        }

        let source_keys: Vec<Vec<u8>> = cmd.args[source_start..source_start + num_keys]
            .iter()
            .map(|b| b.to_vec())
            .collect();

        let mut digests = TDIGESTS.write();

        // Clone sources first to avoid borrow conflicts
        let sources: Vec<TDigest> = source_keys
            .iter()
            .filter_map(|key| digests.get(key).cloned())
            .collect();

        // Create destination with specified compression or default
        let comp = compression.unwrap_or(100.0);
        let dest = digests
            .entry(dest_key)
            .or_insert_with(|| TDigest::new(comp));

        // Merge the cloned sources
        for source in &sources {
            dest.merge(source);
        }

        Ok(Frame::ok())
    })
}

/// TDIGEST.QUANTILE key quantile [quantile ...]
/// Get quantile values from a T-Digest.
pub fn cmd_tdigest_quantile(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;
        let key = cmd.args[0].to_vec();

        let digests = TDIGESTS.read();
        let digest = digests.get(&key).ok_or(CommandError::NoSuchKey)?;

        let results: Vec<Frame> = cmd.args[1..]
            .iter()
            .map(|arg| {
                let q: f64 = std::str::from_utf8(arg)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                let value = digest.quantile(q);
                Frame::Bulk(format!("{}", value).into())
            })
            .collect();

        Ok(Frame::Array(results))
    })
}

/// TDIGEST.CDF key value [value ...]
/// Get CDF (cumulative distribution function) values.
pub fn cmd_tdigest_cdf(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;
        let key = cmd.args[0].to_vec();

        let digests = TDIGESTS.read();
        let digest = digests.get(&key).ok_or(CommandError::NoSuchKey)?;

        let results: Vec<Frame> = cmd.args[1..]
            .iter()
            .map(|arg| {
                let v: f64 = std::str::from_utf8(arg)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                let value = digest.cdf(v);
                Frame::Bulk(format!("{}", value).into())
            })
            .collect();

        Ok(Frame::Array(results))
    })
}

/// TDIGEST.MIN key
/// Get minimum value in a T-Digest.
pub fn cmd_tdigest_min(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(1)?;
        let key = cmd.args[0].to_vec();

        let digests = TDIGESTS.read();
        let digest = digests.get(&key).ok_or(CommandError::NoSuchKey)?;

        let min = digest.min();
        Ok(Frame::Bulk(format!("{}", min).into()))
    })
}

/// TDIGEST.MAX key
/// Get maximum value in a T-Digest.
pub fn cmd_tdigest_max(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(1)?;
        let key = cmd.args[0].to_vec();

        let digests = TDIGESTS.read();
        let digest = digests.get(&key).ok_or(CommandError::NoSuchKey)?;

        let max = digest.max();
        Ok(Frame::Bulk(format!("{}", max).into()))
    })
}

/// TDIGEST.TRIMMED_MEAN key low_percentile high_percentile
/// Get trimmed mean from a T-Digest.
pub fn cmd_tdigest_trimmed_mean(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(3)?;
        let key = cmd.args[0].to_vec();
        let low: f64 = cmd
            .get_str(1)?
            .parse()
            .map_err(|_| CommandError::NotFloat)?;
        let high: f64 = cmd
            .get_str(2)?
            .parse()
            .map_err(|_| CommandError::NotFloat)?;

        let digests = TDIGESTS.read();
        let digest = digests.get(&key).ok_or(CommandError::NoSuchKey)?;

        let mean = digest.trimmed_mean(low, high);
        Ok(Frame::Bulk(format!("{}", mean).into()))
    })
}

/// TDIGEST.RANK key value [value ...]
/// Get rank of values (equivalent to CDF).
pub fn cmd_tdigest_rank(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;
        let key = cmd.args[0].to_vec();

        let digests = TDIGESTS.read();
        let digest = digests.get(&key).ok_or(CommandError::NoSuchKey)?;

        let total = digest.count();
        let results: Vec<Frame> = cmd.args[1..]
            .iter()
            .map(|arg| {
                let v: f64 = std::str::from_utf8(arg)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                let rank = (digest.cdf(v) * total).round() as i64;
                Frame::Integer(rank)
            })
            .collect();

        Ok(Frame::Array(results))
    })
}

/// TDIGEST.REVRANK key value [value ...]
/// Get reverse rank of values.
pub fn cmd_tdigest_revrank(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;
        let key = cmd.args[0].to_vec();

        let digests = TDIGESTS.read();
        let digest = digests.get(&key).ok_or(CommandError::NoSuchKey)?;

        let total = digest.count();
        let results: Vec<Frame> = cmd.args[1..]
            .iter()
            .map(|arg| {
                let v: f64 = std::str::from_utf8(arg)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                let rank = ((1.0 - digest.cdf(v)) * total).round() as i64;
                Frame::Integer(rank)
            })
            .collect();

        Ok(Frame::Array(results))
    })
}

/// TDIGEST.BYRANK key rank [rank ...]
/// Get values by rank.
pub fn cmd_tdigest_byrank(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;
        let key = cmd.args[0].to_vec();

        let digests = TDIGESTS.read();
        let digest = digests.get(&key).ok_or(CommandError::NoSuchKey)?;

        let total = digest.count();
        let results: Vec<Frame> = cmd.args[1..]
            .iter()
            .map(|arg| {
                let rank: f64 = std::str::from_utf8(arg)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                let q = if total > 0.0 { rank / total } else { 0.0 };
                let value = digest.quantile(q);
                Frame::Bulk(format!("{}", value).into())
            })
            .collect();

        Ok(Frame::Array(results))
    })
}

/// TDIGEST.BYREVRANK key rank [rank ...]
/// Get values by reverse rank.
pub fn cmd_tdigest_byrevrank(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;
        let key = cmd.args[0].to_vec();

        let digests = TDIGESTS.read();
        let digest = digests.get(&key).ok_or(CommandError::NoSuchKey)?;

        let total = digest.count();
        let results: Vec<Frame> = cmd.args[1..]
            .iter()
            .map(|arg| {
                let rank: f64 = std::str::from_utf8(arg)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                let q = if total > 0.0 {
                    1.0 - (rank / total)
                } else {
                    1.0
                };
                let value = digest.quantile(q);
                Frame::Bulk(format!("{}", value).into())
            })
            .collect();

        Ok(Frame::Array(results))
    })
}

/// TDIGEST.INFO key
/// Get information about a T-Digest.
pub fn cmd_tdigest_info(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(1)?;
        let key = cmd.args[0].to_vec();

        let digests = TDIGESTS.read();
        let digest = digests.get(&key).ok_or(CommandError::NoSuchKey)?;

        let info = digest.info();
        Ok(Frame::Array(vec![
            Frame::bulk("Compression"),
            Frame::Bulk(format!("{}", info.compression).into()),
            Frame::bulk("Capacity"),
            Frame::Integer(info.capacity as i64),
            Frame::bulk("Merged nodes"),
            Frame::Integer(info.merged_nodes as i64),
            Frame::bulk("Unmerged nodes"),
            Frame::Integer(info.unmerged_nodes as i64),
            Frame::bulk("Merged weight"),
            Frame::Bulk(format!("{}", info.merged_weight).into()),
            Frame::bulk("Unmerged weight"),
            Frame::Bulk(format!("{}", info.unmerged_weight).into()),
            Frame::bulk("Total compressions"),
            Frame::Integer(info.total_compressions as i64),
            Frame::bulk("Memory usage"),
            Frame::Integer(info.memory_usage as i64),
        ]))
    })
}
