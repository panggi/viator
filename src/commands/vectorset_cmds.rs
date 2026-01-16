//! Vector Set command implementations (Redis 8.0+).
//!
//! Commands for vector similarity search operations.

use super::ParsedCommand;
use crate::Result;
use crate::error::CommandError;
use crate::protocol::Frame;
use crate::server::ClientState;
use crate::storage::Db;
use crate::types::{Key, ViatorValue};
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Quantization mode for vector storage
#[derive(Debug, Clone, Copy, Default)]
enum VectorQuantization {
    /// No quantization (full FP32)
    #[default]
    NoQuant,
    /// 8-bit quantization
    Q8,
    /// Binary quantization
    Bin,
}

/// VADD key [REDUCE dim] (FP32 | VALUES num) element vector [element vector ...]
///      [NOQUANT | Q8 | BIN] [EF build-exploration-factor] [M numlinks]
///      [CAS] [NOCREATE] [SETATTR json-object]
/// Add elements with vectors to a vector set.
///
/// Redis 8.4+ supports:
/// - FP32: binary vector blob
/// - VALUES num: num floats follow
/// - Quantization: NOQUANT (default), Q8 (8-bit), BIN (binary)
/// - EF: build-time exploration factor for HNSW
/// - M: max neighbors per node in HNSW graph
/// - CAS: compare-and-swap (don't overwrite existing)
/// - NOCREATE: fail if key doesn't exist
/// - SETATTR: set JSON attributes atomically
pub fn cmd_vadd(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;

        let key = Key::from(cmd.args[0].clone());
        let mut i = 1;
        let mut reduce_dim: Option<usize> = None;
        let mut cas = false;
        let mut nocreate = false;
        let mut setattr_json: Option<Bytes> = None;
        let mut _quantization = VectorQuantization::NoQuant;
        let mut _ef: Option<usize> = None;
        let mut _m: Option<usize> = None;
        let mut values_mode: Option<usize> = None;
        let mut fp32_mode = false;

        // Pre-scan for global options
        let mut scan_i = 1;
        while scan_i < cmd.args.len() {
            if let Ok(opt) = cmd.get_str(scan_i) {
                match opt.to_uppercase().as_str() {
                    "REDUCE" => {
                        if scan_i + 1 < cmd.args.len() {
                            reduce_dim = Some(cmd.get_u64(scan_i + 1)? as usize);
                            i = scan_i + 2;
                            scan_i += 1;
                        }
                    }
                    "FP32" => {
                        fp32_mode = true;
                        i = scan_i + 1;
                    }
                    "VALUES" => {
                        if scan_i + 1 < cmd.args.len() {
                            values_mode = Some(cmd.get_u64(scan_i + 1)? as usize);
                            i = scan_i + 2;
                            scan_i += 1;
                        }
                    }
                    "NOQUANT" => _quantization = VectorQuantization::NoQuant,
                    "Q8" => _quantization = VectorQuantization::Q8,
                    "BIN" => _quantization = VectorQuantization::Bin,
                    "EF" => {
                        if scan_i + 1 < cmd.args.len() {
                            _ef = Some(cmd.get_u64(scan_i + 1)? as usize);
                            scan_i += 1;
                        }
                    }
                    "M" => {
                        if scan_i + 1 < cmd.args.len() {
                            _m = Some(cmd.get_u64(scan_i + 1)? as usize);
                            scan_i += 1;
                        }
                    }
                    "CAS" => cas = true,
                    "NOCREATE" => nocreate = true,
                    "SETATTR" => {
                        if scan_i + 1 < cmd.args.len() {
                            setattr_json = Some(cmd.args[scan_i + 1].clone());
                            scan_i += 1;
                        }
                    }
                    _ => {}
                }
            }
            scan_i += 1;
        }

        // Get or create vector set
        let vs = match db.get(&key) {
            Some(v) => {
                if let Some(vs) = v.as_vectorset() {
                    vs.clone()
                } else {
                    return Err(CommandError::WrongType.into());
                }
            }
            None => {
                if nocreate {
                    return Err(CommandError::WrongType.into());
                }
                let new_vs = ViatorValue::new_vectorset();
                db.set(key.clone(), new_vs.clone());
                new_vs.as_vectorset().unwrap().clone()
            }
        };

        let mut added = 0;

        // Parse element-vector pairs
        while i + 1 < cmd.args.len() {
            // Check for option keywords that end element parsing
            if let Ok(maybe_keyword) = cmd.get_str(i) {
                let upper = maybe_keyword.to_uppercase();
                if matches!(
                    upper.as_str(),
                    "SETATTR" | "CAS" | "NOCREATE" | "NOQUANT" | "Q8" | "BIN" | "EF" | "M"
                ) {
                    break;
                }
            }

            let element_name = cmd.args[i].clone();

            // Parse vector based on mode
            let vector = if fp32_mode {
                // Binary FP32 blob
                let blob = &cmd.args[i + 1];
                parse_fp32_blob(blob)?
            } else if let Some(num_values) = values_mode {
                // VALUES num: next num args are float values
                let mut vals = Vec::with_capacity(num_values);
                for j in 0..num_values {
                    if i + 1 + j >= cmd.args.len() {
                        return Err(CommandError::SyntaxError.into());
                    }
                    let val_str = cmd.get_str(i + 1 + j)?;
                    vals.push(
                        val_str
                            .parse::<f32>()
                            .map_err(|_| CommandError::SyntaxError)?,
                    );
                }
                i += num_values - 1; // Adjust for extra args consumed
                if let Some(dim) = reduce_dim {
                    vals.truncate(dim);
                }
                vals
            } else {
                // String format: comma or space separated
                let vector_str = cmd.get_str(i + 1)?;
                parse_vector(vector_str, reduce_dim)?
            };

            let mut guard = vs.write();

            // CAS: check if element already exists
            if cas && guard.contains(&element_name) {
                i += 2;
                continue;
            }

            if guard
                .add(element_name.clone(), vector)
                .map_err(|_| CommandError::SyntaxError)?
            {
                added += 1;
            }

            // Apply SETATTR if provided
            if let Some(ref json) = setattr_json {
                // Parse JSON and set attributes
                if let Ok(json_str) = std::str::from_utf8(json) {
                    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(json_str) {
                        if let Some(obj) = parsed.as_object() {
                            for (k, v) in obj {
                                let value_bytes = Bytes::from(v.to_string());
                                guard.set_attr(&element_name, Bytes::from(k.clone()), value_bytes);
                            }
                        }
                    }
                }
            }

            i += 2;
        }

        Ok(Frame::Integer(added))
    })
}

/// Parse FP32 binary blob into vector
fn parse_fp32_blob(blob: &Bytes) -> Result<Vec<f32>> {
    if blob.len() % 4 != 0 {
        return Err(CommandError::SyntaxError.into());
    }
    let mut vector = Vec::with_capacity(blob.len() / 4);
    for chunk in blob.chunks(4) {
        let bytes: [u8; 4] = chunk.try_into().map_err(|_| CommandError::SyntaxError)?;
        vector.push(f32::from_le_bytes(bytes));
    }
    Ok(vector)
}

/// VCARD key
/// Get the cardinality of a vector set.
pub fn cmd_vcard(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(1)?;

        let key = Key::from(cmd.args[0].clone());

        match db.get(&key) {
            Some(v) => {
                if let Some(vs) = v.as_vectorset() {
                    Ok(Frame::Integer(vs.read().card() as i64))
                } else {
                    Err(CommandError::WrongType.into())
                }
            }
            None => Ok(Frame::Integer(0)),
        }
    })
}

/// VDIM key
/// Get the dimensionality of vectors in the set.
pub fn cmd_vdim(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(1)?;

        let key = Key::from(cmd.args[0].clone());

        match db.get(&key) {
            Some(v) => {
                if let Some(vs) = v.as_vectorset() {
                    let dim = vs.read().dim().unwrap_or(0);
                    Ok(Frame::Integer(dim as i64))
                } else {
                    Err(CommandError::WrongType.into())
                }
            }
            None => Ok(Frame::Integer(0)),
        }
    })
}

/// VEMB key element [RAW]
/// Get the embedding vector for an element.
pub fn cmd_vemb(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        let key = Key::from(cmd.args[0].clone());
        let element = cmd.args[1].clone();
        let raw = cmd.args.len() > 2 && cmd.get_str(2)?.to_uppercase() == "RAW";

        match db.get(&key) {
            Some(v) => {
                if let Some(vs) = v.as_vectorset() {
                    let guard = vs.read();
                    match guard.get_embedding(&element) {
                        Some(vec) => {
                            if raw {
                                // Return as binary blob
                                let bytes: Vec<u8> =
                                    vec.iter().flat_map(|f| f.to_le_bytes()).collect();
                                Ok(Frame::Bulk(Bytes::from(bytes)))
                            } else {
                                // Return as array of bulk strings
                                let frames: Vec<Frame> = vec
                                    .iter()
                                    .map(|f| Frame::Bulk(Bytes::from(f.to_string())))
                                    .collect();
                                Ok(Frame::Array(frames))
                            }
                        }
                        None => Ok(Frame::Null),
                    }
                } else {
                    Err(CommandError::WrongType.into())
                }
            }
            None => Ok(Frame::Null),
        }
    })
}

/// Filter expression for VSIM command
#[derive(Debug, Clone)]
enum VsimFilter {
    /// No filter
    None,
    /// IN filter: @attr IN {val1, val2, ...}
    In { attr: String, values: Vec<Bytes> },
    /// NOT IN filter: @attr NOT IN {val1, val2, ...}
    NotIn { attr: String, values: Vec<Bytes> },
    /// Equality: @attr == value
    Eq { attr: String, value: Bytes },
    /// Not equal: @attr != value
    Ne { attr: String, value: Bytes },
}

/// Parse a filter expression for VSIM (Redis 8.2+)
/// Supports: @attr IN {val1, val2}, @attr NOT IN {val1, val2}, @attr == val, @attr != val
fn parse_vsim_filter(expr: &str) -> VsimFilter {
    let expr = expr.trim();

    // Parse @attr IN {val1, val2, ...}
    if let Some(pos) = expr.find(" IN ") {
        let attr_part = expr[..pos].trim();
        let values_part = expr[pos + 4..].trim();

        if attr_part.starts_with('@') {
            let attr = attr_part[1..].to_string();
            let values = parse_filter_values(values_part);
            return VsimFilter::In { attr, values };
        }
    }

    // Parse @attr NOT IN {val1, val2, ...}
    if let Some(pos) = expr.find(" NOT IN ") {
        let attr_part = expr[..pos].trim();
        let values_part = expr[pos + 8..].trim();

        if attr_part.starts_with('@') {
            let attr = attr_part[1..].to_string();
            let values = parse_filter_values(values_part);
            return VsimFilter::NotIn { attr, values };
        }
    }

    // Parse @attr == value
    if let Some(pos) = expr.find("==") {
        let attr_part = expr[..pos].trim();
        let value_part = expr[pos + 2..].trim();

        if attr_part.starts_with('@') {
            let attr = attr_part[1..].to_string();
            let value = Bytes::from(value_part.trim_matches('"').to_string());
            return VsimFilter::Eq { attr, value };
        }
    }

    // Parse @attr != value
    if let Some(pos) = expr.find("!=") {
        let attr_part = expr[..pos].trim();
        let value_part = expr[pos + 2..].trim();

        if attr_part.starts_with('@') {
            let attr = attr_part[1..].to_string();
            let value = Bytes::from(value_part.trim_matches('"').to_string());
            return VsimFilter::Ne { attr, value };
        }
    }

    VsimFilter::None
}

/// Parse values from {val1, val2, ...} format
fn parse_filter_values(s: &str) -> Vec<Bytes> {
    let s = s.trim_start_matches('{').trim_end_matches('}');
    s.split(',')
        .map(|v| Bytes::from(v.trim().trim_matches('"').to_string()))
        .collect()
}

/// VSIM key (ELE element | FP32 blob | VALUES num v1 v2 ...) [COUNT count] [EF ef]
///      [FILTER expr] [FILTER-EF ef] [TRUTH] [NOTHREAD]
///      [WITHSCORES] [WITHATTRIBS] [EPSILON eps]
/// Find similar elements.
///
/// Redis 8.4+ supports:
/// - VALUES num: inline vector as num separate float arguments
/// - WITHSCORES: include similarity scores in output
/// - WITHATTRIBS: include element attributes in output
/// - EPSILON: approximation factor for faster search
/// - FILTER with IN operator: @attr IN {val1, val2, ...}
pub fn cmd_vsim(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;

        let key = Key::from(cmd.args[0].clone());
        let mode = cmd.get_str(1)?.to_uppercase();

        let mut count = 10usize;
        let mut filter = VsimFilter::None;
        let mut with_scores = false;
        let mut with_attribs = false;
        let mut _epsilon: Option<f32> = None;
        let mut _ef: Option<usize> = None;
        let mut query_vector: Option<Vec<f32>> = None;
        let mut query_element: Option<Bytes> = None;
        let mut i = 2;

        // Parse mode-specific query
        match mode.as_str() {
            "ELE" => {
                query_element = Some(cmd.args[i].clone());
                i += 1;
            }
            "FP32" => {
                // Binary blob
                let blob = &cmd.args[i];
                query_vector = Some(parse_fp32_blob(blob)?);
                i += 1;
            }
            "VALUES" => {
                // VALUES num v1 v2 ...
                let num = cmd.get_u64(i)? as usize;
                i += 1;
                let mut vals = Vec::with_capacity(num);
                for _ in 0..num {
                    if i >= cmd.args.len() {
                        return Err(CommandError::SyntaxError.into());
                    }
                    let val_str = cmd.get_str(i)?;
                    vals.push(
                        val_str
                            .parse::<f32>()
                            .map_err(|_| CommandError::SyntaxError)?,
                    );
                    i += 1;
                }
                query_vector = Some(vals);
            }
            _ => return Err(CommandError::SyntaxError.into()),
        }

        // Parse options
        while i < cmd.args.len() {
            if let Ok(opt) = cmd.get_str(i) {
                match opt.to_uppercase().as_str() {
                    "COUNT" => {
                        i += 1;
                        count = cmd.get_u64(i)? as usize;
                    }
                    "FILTER" => {
                        i += 1;
                        if i < cmd.args.len() {
                            let filter_expr = cmd.get_str(i)?;
                            filter = parse_vsim_filter(filter_expr);
                        }
                    }
                    "EF" | "FILTER-EF" => {
                        i += 1;
                        if i < cmd.args.len() {
                            _ef = Some(cmd.get_u64(i)? as usize);
                        }
                    }
                    "EPSILON" => {
                        i += 1;
                        if i < cmd.args.len() {
                            let eps_str = cmd.get_str(i)?;
                            _epsilon = Some(
                                eps_str
                                    .parse::<f32>()
                                    .map_err(|_| CommandError::SyntaxError)?,
                            );
                        }
                    }
                    "WITHSCORES" => with_scores = true,
                    "WITHATTRIBS" => with_attribs = true,
                    "TRUTH" | "NOTHREAD" => {} // Recognized but no-op
                    _ => {}
                }
            }
            i += 1;
        }

        match db.get(&key) {
            Some(v) => {
                if let Some(vs) = v.as_vectorset() {
                    let guard = vs.read();

                    let results = if let Some(ref element) = query_element {
                        guard.search_by_element(element, count * 10)
                    } else if let Some(ref vector) = query_vector {
                        guard.search(vector, count * 10)
                    } else {
                        return Err(CommandError::SyntaxError.into());
                    };

                    // Apply filter if present
                    let filtered_results: Vec<(Bytes, f32)> = match &filter {
                        VsimFilter::None => results.into_iter().take(count).collect(),
                        VsimFilter::In { attr, values } => results
                            .into_iter()
                            .filter(|(name, _)| {
                                if let Some(val) = guard.get_attr(name, &Bytes::from(attr.clone()))
                                {
                                    values.iter().any(|v| v == val)
                                } else {
                                    false
                                }
                            })
                            .take(count)
                            .collect(),
                        VsimFilter::NotIn { attr, values } => results
                            .into_iter()
                            .filter(|(name, _)| {
                                if let Some(val) = guard.get_attr(name, &Bytes::from(attr.clone()))
                                {
                                    !values.iter().any(|v| v == val)
                                } else {
                                    true
                                }
                            })
                            .take(count)
                            .collect(),
                        VsimFilter::Eq { attr, value } => results
                            .into_iter()
                            .filter(|(name, _)| {
                                guard
                                    .get_attr(name, &Bytes::from(attr.clone()))
                                    .is_some_and(|v| v == value)
                            })
                            .take(count)
                            .collect(),
                        VsimFilter::Ne { attr, value } => results
                            .into_iter()
                            .filter(|(name, _)| {
                                guard
                                    .get_attr(name, &Bytes::from(attr.clone()))
                                    .map(|v| v != value)
                                    .unwrap_or(true)
                            })
                            .take(count)
                            .collect(),
                    };

                    // Build response based on options
                    let frames: Vec<Frame> = if with_attribs {
                        // Return [element, score, {attrs}] or [element, {attrs}]
                        filtered_results
                            .into_iter()
                            .flat_map(|(name, score)| {
                                let mut result = vec![Frame::Bulk(name.clone())];
                                if with_scores {
                                    result.push(Frame::Bulk(Bytes::from(score.to_string())));
                                }
                                // Get all attributes as JSON object
                                let attrs = guard.get_all_attrs(&name);
                                let attrs_json = serde_json::to_string(&attrs)
                                    .unwrap_or_else(|_| "{}".to_string());
                                result.push(Frame::Bulk(Bytes::from(attrs_json)));
                                result
                            })
                            .collect()
                    } else if with_scores {
                        // Return [element, score] pairs
                        filtered_results
                            .into_iter()
                            .flat_map(|(name, score)| {
                                vec![
                                    Frame::Bulk(name),
                                    Frame::Bulk(Bytes::from(score.to_string())),
                                ]
                            })
                            .collect()
                    } else {
                        // Return just element names
                        filtered_results
                            .into_iter()
                            .map(|(name, _)| Frame::Bulk(name))
                            .collect()
                    };

                    Ok(Frame::Array(frames))
                } else {
                    Err(CommandError::WrongType.into())
                }
            }
            None => Ok(Frame::Array(vec![])),
        }
    })
}

/// VREM key element [element ...]
/// Remove elements from a vector set.
pub fn cmd_vrem(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        let key = Key::from(cmd.args[0].clone());

        match db.get(&key) {
            Some(v) => {
                if let Some(vs) = v.as_vectorset() {
                    let mut guard = vs.write();
                    let mut removed = 0;

                    for element in &cmd.args[1..] {
                        if guard.remove(element) {
                            removed += 1;
                        }
                    }

                    Ok(Frame::Integer(removed))
                } else {
                    Err(CommandError::WrongType.into())
                }
            }
            None => Ok(Frame::Integer(0)),
        }
    })
}

/// VGETATTR key element [attr]
/// Get an attribute or all attributes from an element.
///
/// Redis 8.4+: When attr is omitted, returns all attributes as a JSON object.
pub fn cmd_vgetattr(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        let key = Key::from(cmd.args[0].clone());
        let element = cmd.args[1].clone();
        let attr = if cmd.args.len() > 2 {
            Some(cmd.args[2].clone())
        } else {
            None
        };

        match db.get(&key) {
            Some(v) => {
                if let Some(vs) = v.as_vectorset() {
                    let guard = vs.read();

                    // Check if element exists first
                    if !guard.contains(&element) {
                        return Ok(Frame::Null);
                    }

                    if let Some(attr_key) = attr {
                        // Get specific attribute
                        match guard.get_attr(&element, &attr_key) {
                            Some(value) => Ok(Frame::Bulk(value.clone())),
                            None => Ok(Frame::Null),
                        }
                    } else {
                        // Get all attributes as JSON
                        let attrs = guard.get_all_attrs(&element);
                        let json =
                            serde_json::to_string(&attrs).unwrap_or_else(|_| "{}".to_string());
                        Ok(Frame::Bulk(Bytes::from(json)))
                    }
                } else {
                    Err(CommandError::WrongType.into())
                }
            }
            None => Ok(Frame::Null),
        }
    })
}

/// VSETATTR key element json-object
/// Set attributes on an element from a JSON object.
///
/// Redis 8.4+: Accepts a JSON object where each key-value pair becomes an attribute.
/// Example: VSETATTR myvec elem1 '{"color":"red","size":"10"}'
pub fn cmd_vsetattr(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(3)?;

        let key = Key::from(cmd.args[0].clone());
        let element = cmd.args[1].clone();
        let json_str = cmd.get_str(2)?;

        // Parse JSON object
        let parsed: serde_json::Value =
            serde_json::from_str(json_str).map_err(|_| CommandError::SyntaxError)?;

        let obj = parsed.as_object().ok_or(CommandError::SyntaxError)?;

        match db.get(&key) {
            Some(v) => {
                if let Some(vs) = v.as_vectorset() {
                    let mut guard = vs.write();

                    // Check if element exists
                    if !guard.contains(&element) {
                        return Ok(Frame::Null);
                    }

                    // Set each attribute from the JSON object
                    for (k, v) in obj {
                        let value_str = match v {
                            serde_json::Value::String(s) => s.clone(),
                            _ => v.to_string(),
                        };
                        guard.set_attr(&element, Bytes::from(k.clone()), Bytes::from(value_str));
                    }

                    Ok(Frame::ok())
                } else {
                    Err(CommandError::WrongType.into())
                }
            }
            None => Ok(Frame::Null),
        }
    })
}

/// VINFO key
/// Get information about a vector set.
pub fn cmd_vinfo(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(1)?;

        let key = Key::from(cmd.args[0].clone());

        match db.get(&key) {
            Some(v) => {
                if let Some(vs) = v.as_vectorset() {
                    let guard = vs.read();
                    let info = guard.info();

                    Ok(Frame::Array(vec![
                        Frame::bulk("num-elements"),
                        Frame::Integer(info.num_elements as i64),
                        Frame::bulk("dimension"),
                        Frame::Integer(info.dim as i64),
                        Frame::bulk("distance-metric"),
                        Frame::Bulk(Bytes::from(info.metric.as_str())),
                        Frame::bulk("quantization"),
                        Frame::Bulk(Bytes::from(info.quantization)),
                    ]))
                } else {
                    Err(CommandError::WrongType.into())
                }
            }
            None => Ok(Frame::Null),
        }
    })
}

/// VLINKS key element [WITHSCORES]
/// Get the HNSW graph links for an element.
///
/// Redis 8.4+: Returns the element's neighbors in the HNSW graph.
/// WITHSCORES includes similarity scores with each neighbor.
pub fn cmd_vlinks(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        let key = Key::from(cmd.args[0].clone());
        let element = cmd.args[1].clone();
        let with_scores = cmd.args.len() > 2
            && cmd
                .get_str(2)
                .is_ok_and(|s| s.to_uppercase() == "WITHSCORES");

        match db.get(&key) {
            Some(v) => {
                if let Some(vs) = v.as_vectorset() {
                    let guard = vs.read();

                    // Check if element exists
                    if !guard.contains(&element) {
                        return Ok(Frame::Null);
                    }

                    // Get graph neighbors (simulated via similarity search for now)
                    // In a real HNSW implementation, this would return actual graph links
                    let results = guard.search_by_element(&element, 16); // M parameter typically 16

                    let frames: Vec<Frame> = if with_scores {
                        results
                            .into_iter()
                            .flat_map(|(name, score)| {
                                vec![
                                    Frame::Bulk(name),
                                    Frame::Bulk(Bytes::from(score.to_string())),
                                ]
                            })
                            .collect()
                    } else {
                        results
                            .into_iter()
                            .map(|(name, _)| Frame::Bulk(name))
                            .collect()
                    };

                    Ok(Frame::Array(frames))
                } else {
                    Err(CommandError::WrongType.into())
                }
            }
            None => Ok(Frame::Null),
        }
    })
}

/// VRANDMEMBER key [count]
/// Get random members from a vector set.
pub fn cmd_vrandmember(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;

        let key = Key::from(cmd.args[0].clone());
        let (count, allow_duplicates) = if cmd.args.len() > 1 {
            let c = cmd.get_i64(1)?;
            if c < 0 {
                ((-c) as usize, true)
            } else {
                (c as usize, false)
            }
        } else {
            (1, false)
        };

        match db.get(&key) {
            Some(v) => {
                if let Some(vs) = v.as_vectorset() {
                    let guard = vs.read();
                    let members = guard.random_members(count, allow_duplicates);

                    if cmd.args.len() == 1 {
                        // Single element mode
                        if let Some(m) = members.into_iter().next() {
                            Ok(Frame::Bulk(m))
                        } else {
                            Ok(Frame::Null)
                        }
                    } else {
                        let frames: Vec<Frame> = members.into_iter().map(Frame::Bulk).collect();
                        Ok(Frame::Array(frames))
                    }
                } else {
                    Err(CommandError::WrongType.into())
                }
            }
            None => {
                if cmd.args.len() == 1 {
                    Ok(Frame::Null)
                } else {
                    Ok(Frame::Array(vec![]))
                }
            }
        }
    })
}

/// VISMEMBER key element
/// Check if an element exists in a vector set.
///
/// Redis 8.0+: Returns 1 if element exists, 0 otherwise.
pub fn cmd_vismember(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(2)?;

        let key = Key::from(cmd.args[0].clone());
        let element = cmd.args[1].clone();

        match db.get(&key) {
            Some(v) => {
                if let Some(vs) = v.as_vectorset() {
                    let guard = vs.read();
                    if guard.contains(&element) {
                        Ok(Frame::Integer(1))
                    } else {
                        Ok(Frame::Integer(0))
                    }
                } else {
                    Err(CommandError::WrongType.into())
                }
            }
            None => Ok(Frame::Integer(0)),
        }
    })
}

/// VRANGE key start end [count]
/// Return elements in a lexicographical range.
///
/// Redis 8.4+: Returns elements sorted lexicographically.
/// - start/end: [str for inclusive, (str for exclusive, - for min, + for max
/// - count: max elements to return (-1 for all)
pub fn cmd_vrange(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;

        let key = Key::from(cmd.args[0].clone());
        let start_spec = cmd.get_str(1)?;
        let end_spec = cmd.get_str(2)?;
        let count = if cmd.args.len() > 3 {
            cmd.get_i64(3)?
        } else {
            -1 // Default: return all
        };

        match db.get(&key) {
            Some(v) => {
                if let Some(vs) = v.as_vectorset() {
                    let guard = vs.read();

                    // Get all member names and sort lexicographically
                    let mut members: Vec<Bytes> = guard.members().into_iter().cloned().collect();
                    members.sort();

                    // Parse range specifications
                    let (start_inclusive, start_val) = parse_range_spec(start_spec);
                    let (end_inclusive, end_val) = parse_range_spec(end_spec);

                    // Filter by range
                    let filtered: Vec<Bytes> = members
                        .into_iter()
                        .filter(|m| {
                            let m_str = std::str::from_utf8(m).unwrap_or("");

                            // Check start bound
                            let after_start = match &start_val {
                                None => true, // "-" means minimum
                                Some(s) => {
                                    if start_inclusive {
                                        m_str >= s.as_str()
                                    } else {
                                        m_str > s.as_str()
                                    }
                                }
                            };

                            // Check end bound
                            let before_end = match &end_val {
                                None => true, // "+" means maximum
                                Some(e) => {
                                    if end_inclusive {
                                        m_str <= e.as_str()
                                    } else {
                                        m_str < e.as_str()
                                    }
                                }
                            };

                            after_start && before_end
                        })
                        .collect();

                    // Apply count limit
                    let result: Vec<Frame> = if count < 0 {
                        filtered.into_iter().map(Frame::Bulk).collect()
                    } else {
                        filtered
                            .into_iter()
                            .take(count as usize)
                            .map(Frame::Bulk)
                            .collect()
                    };

                    Ok(Frame::Array(result))
                } else {
                    Err(CommandError::WrongType.into())
                }
            }
            None => Ok(Frame::Array(vec![])),
        }
    })
}

/// Parse range specification for VRANGE
/// Returns (is_inclusive, Option<value>)
/// "-" returns (true, None) for minimum
/// "+" returns (true, None) for maximum
/// "[foo" returns (true, Some("foo"))
/// "(foo" returns (false, Some("foo"))
fn parse_range_spec(spec: &str) -> (bool, Option<String>) {
    if spec == "-" || spec == "+" {
        (true, None)
    } else if let Some(stripped) = spec.strip_prefix('[') {
        (true, Some(stripped.to_string()))
    } else if let Some(stripped) = spec.strip_prefix('(') {
        (false, Some(stripped.to_string()))
    } else {
        // Default: treat as inclusive
        (true, Some(spec.to_string()))
    }
}

/// Parse a vector from string representation.
/// Supports formats: "1.0,2.0,3.0" or space-separated "1.0 2.0 3.0"
fn parse_vector(s: &str, reduce_dim: Option<usize>) -> Result<Vec<f32>> {
    let values: Vec<f32> = if s.contains(',') {
        s.split(',')
            .map(|v| v.trim().parse::<f32>())
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|_| CommandError::SyntaxError)?
    } else {
        s.split_whitespace()
            .map(|v| v.parse::<f32>())
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|_| CommandError::SyntaxError)?
    };

    if values.is_empty() {
        return Err(CommandError::SyntaxError.into());
    }

    // Apply dimensionality reduction if requested
    if let Some(dim) = reduce_dim {
        if dim < values.len() {
            // Simple truncation for now (proper reduction would use PCA or similar)
            Ok(values.into_iter().take(dim).collect())
        } else {
            Ok(values)
        }
    } else {
        Ok(values)
    }
}
