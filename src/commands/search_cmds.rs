//! RediSearch (FT.*) command implementations.
//!
//! Full-text and vector search commands.

use super::ParsedCommand;
use crate::Result;
use crate::error::CommandError;
use crate::protocol::Frame;
use crate::server::ClientState;
use crate::storage::Db;
use crate::types::search::{
    FieldDefinition, FieldType, FieldValue, IndexOptions, IndexSchema, SearchManager, SearchQuery,
    parse_query,
};
use bytes::Bytes;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Get the global search manager from database
fn get_search_manager(db: &Db) -> Arc<SearchManager> {
    db.search_manager().clone()
}

/// FT.CREATE index [ON HASH|JSON] [PREFIX count prefix [prefix ...]] SCHEMA field_name [TEXT|NUMERIC|TAG|GEO|VECTOR] [options] ...
pub fn cmd_ft_create(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(4)?; // index SCHEMA field type

        let index_name = cmd.get_str(0)?;
        let manager = get_search_manager(&db);

        let mut idx = 1;
        let mut options = IndexOptions::default();
        let mut fields = Vec::new();

        // Parse options until SCHEMA
        while idx < cmd.args.len() {
            let opt = cmd.get_str(idx)?.to_uppercase();
            match opt.as_str() {
                "ON" => {
                    idx += 1;
                    let on_type = cmd.get_str(idx)?.to_uppercase();
                    options.on_type = match on_type.as_str() {
                        "JSON" => crate::types::search::IndexOnType::Json,
                        _ => crate::types::search::IndexOnType::Hash,
                    };
                    idx += 1;
                }
                "PREFIX" => {
                    idx += 1;
                    let count = cmd.get_u64(idx)? as usize;
                    idx += 1;
                    for _ in 0..count {
                        options.prefixes.push(cmd.get_str(idx)?.to_string());
                        idx += 1;
                    }
                }
                "LANGUAGE" => {
                    idx += 1;
                    options.language = cmd.get_str(idx)?.to_string();
                    idx += 1;
                }
                "STOPWORDS" => {
                    idx += 1;
                    let count = cmd.get_u64(idx)? as usize;
                    idx += 1;
                    options.stopwords.clear();
                    for _ in 0..count {
                        options.stopwords.insert(cmd.get_str(idx)?.to_string());
                        idx += 1;
                    }
                }
                "SCHEMA" => {
                    idx += 1;
                    break;
                }
                _ => {
                    idx += 1;
                }
            }
        }

        // Parse schema
        while idx < cmd.args.len() {
            let field_name = cmd.get_str(idx)?.to_string();
            idx += 1;

            if idx >= cmd.args.len() {
                return Err(CommandError::SyntaxError.into());
            }

            let type_str = cmd.get_str(idx)?.to_uppercase();
            let field_type = match type_str.as_str() {
                "TEXT" => FieldType::Text,
                "NUMERIC" => FieldType::Numeric,
                "TAG" => FieldType::Tag,
                "GEO" => FieldType::Geo,
                "VECTOR" => FieldType::Vector,
                _ => return Err(CommandError::SyntaxError.into()),
            };
            idx += 1;

            let mut weight = 1.0;
            let mut sortable = false;
            let mut no_index = false;

            // Parse field options
            while idx < cmd.args.len() {
                if let Ok(opt) = cmd.get_str(idx) {
                    let opt_upper = opt.to_uppercase();
                    match opt_upper.as_str() {
                        "WEIGHT" => {
                            idx += 1;
                            weight = cmd.get_f64(idx)?;
                            idx += 1;
                        }
                        "SORTABLE" => {
                            sortable = true;
                            idx += 1;
                        }
                        "NOINDEX" => {
                            no_index = true;
                            idx += 1;
                        }
                        "TEXT" | "NUMERIC" | "TAG" | "GEO" | "VECTOR" => {
                            // Next field
                            break;
                        }
                        _ => {
                            // Check if it's a new field name by looking ahead
                            if idx + 1 < cmd.args.len() {
                                let next = cmd.get_str(idx + 1)?.to_uppercase();
                                if matches!(
                                    next.as_str(),
                                    "TEXT" | "NUMERIC" | "TAG" | "GEO" | "VECTOR"
                                ) {
                                    break;
                                }
                            }
                            idx += 1;
                        }
                    }
                } else {
                    break;
                }
            }

            fields.push(FieldDefinition {
                name: field_name,
                field_type,
                weight,
                sortable,
                no_index,
                phonetic: None,
            });
        }

        let schema = IndexSchema::new(fields);
        manager
            .create_index(index_name, schema, options)
            .map_err(|_| CommandError::SyntaxError)?;

        Ok(Frame::ok())
    })
}

/// FT.DROPINDEX index [DD]
pub fn cmd_ft_dropindex(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;

        let index_name = cmd.get_str(0)?;
        let manager = get_search_manager(&db);

        manager
            .drop_index(index_name)
            .map_err(|_| CommandError::NoSuchKey)?;

        Ok(Frame::ok())
    })
}

/// FT.ADD index docId score FIELDS field value ...
pub fn cmd_ft_add(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(5)?; // index docId score FIELDS field value

        let index_name = cmd.get_str(0)?;
        let doc_id = cmd.get_str(1)?;
        let score = cmd.get_f64(2)?;

        let manager = get_search_manager(&db);
        let index = manager
            .get_index(index_name)
            .ok_or(CommandError::NoSuchKey)?;

        // Find FIELDS keyword
        let mut idx = 3;
        while idx < cmd.args.len() {
            if cmd.get_str(idx)?.to_uppercase() == "FIELDS" {
                idx += 1;
                break;
            }
            idx += 1;
        }

        // Parse field-value pairs
        let mut fields = HashMap::new();
        while idx + 1 < cmd.args.len() {
            let field_name = cmd.get_str(idx)?;
            let value = cmd.get_str(idx + 1)?;

            // Determine field type from schema
            if let Some(field_def) = index.schema.get_field(field_name) {
                let field_value = match field_def.field_type {
                    FieldType::Text => FieldValue::Text(value.to_string()),
                    FieldType::Numeric => {
                        let n: f64 = value.parse().unwrap_or(0.0);
                        FieldValue::Numeric(n)
                    }
                    FieldType::Tag => {
                        let tags: Vec<String> =
                            value.split(',').map(|s| s.trim().to_string()).collect();
                        FieldValue::Tag(tags)
                    }
                    FieldType::Geo => {
                        let parts: Vec<&str> = value.split(',').collect();
                        if parts.len() >= 2 {
                            let lat: f64 = parts[0].parse().unwrap_or(0.0);
                            let lon: f64 = parts[1].parse().unwrap_or(0.0);
                            FieldValue::Geo(lat, lon)
                        } else {
                            FieldValue::Geo(0.0, 0.0)
                        }
                    }
                    FieldType::Vector => {
                        // Parse vector from comma-separated values
                        let vec: Vec<f32> = value
                            .split(',')
                            .filter_map(|s| s.trim().parse().ok())
                            .collect();
                        FieldValue::Vector(vec)
                    }
                };
                fields.insert(field_name.to_string(), field_value);
            }

            idx += 2;
        }

        index.add_document(doc_id, score, fields);

        Ok(Frame::ok())
    })
}

/// FT.SEARCH index query [RETURN count field ...] [LIMIT offset num] [WITHSCORES]
pub fn cmd_ft_search(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        let index_name = cmd.get_str(0)?;
        let query_str = cmd.get_str(1)?;

        let manager = get_search_manager(&db);
        let index = manager
            .get_index(index_name)
            .ok_or(CommandError::NoSuchKey)?;

        let mut return_fields = None;
        let mut limit = (0usize, 10usize);
        let mut with_scores = false;

        // Parse options
        let mut idx = 2;
        while idx < cmd.args.len() {
            let opt = cmd.get_str(idx)?.to_uppercase();
            match opt.as_str() {
                "RETURN" => {
                    idx += 1;
                    let count = cmd.get_u64(idx)? as usize;
                    idx += 1;
                    let mut fields = Vec::with_capacity(count);
                    for _ in 0..count {
                        fields.push(cmd.get_str(idx)?.to_string());
                        idx += 1;
                    }
                    return_fields = Some(fields);
                }
                "LIMIT" => {
                    idx += 1;
                    let offset = cmd.get_u64(idx)? as usize;
                    idx += 1;
                    let num = cmd.get_u64(idx)? as usize;
                    idx += 1;
                    limit = (offset, num);
                }
                "WITHSCORES" => {
                    with_scores = true;
                    idx += 1;
                }
                _ => {
                    idx += 1;
                }
            }
        }

        let query = SearchQuery {
            query: query_str.to_string(),
            tree: parse_query(query_str),
            return_fields,
            highlight: None,
            sort_by: None,
            limit,
            with_scores,
            with_payloads: false,
            language: "english".to_string(),
            verbatim: false,
            no_stopwords: false,
            slop: 0,
            in_order: true,
        };

        let results = index.search(&query);

        // Build response
        let mut response = vec![Frame::Integer(results.total as i64)];

        for doc in results.docs {
            response.push(Frame::Bulk(Bytes::from(doc.id)));

            if with_scores {
                response.push(Frame::Bulk(Bytes::from(doc.score.to_string())));
            }

            // Field values as array
            let mut field_frames = Vec::new();
            for (k, v) in doc.fields {
                field_frames.push(Frame::Bulk(Bytes::from(k)));
                field_frames.push(Frame::Bulk(Bytes::from(v)));
            }
            response.push(Frame::Array(field_frames));
        }

        Ok(Frame::Array(response))
    })
}

/// FT.INFO index
pub fn cmd_ft_info(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;

        let index_name = cmd.get_str(0)?;
        let manager = get_search_manager(&db);
        let index = manager
            .get_index(index_name)
            .ok_or(CommandError::NoSuchKey)?;

        let info = index.info();

        Ok(Frame::Array(vec![
            Frame::bulk("index_name"),
            Frame::Bulk(Bytes::from(info.name)),
            Frame::bulk("num_docs"),
            Frame::Integer(info.num_docs as i64),
            Frame::bulk("num_terms"),
            Frame::Integer(info.num_terms as i64),
            Frame::bulk("num_records"),
            Frame::Integer(info.num_records as i64),
            Frame::bulk("avg_doc_length"),
            Frame::Bulk(Bytes::from(info.avg_doc_length.to_string())),
        ]))
    })
}

/// FT._LIST - List all indexes
pub fn cmd_ft_list(
    _cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let manager = get_search_manager(&db);
        let indexes = manager.list_indexes();

        let frames: Vec<Frame> = indexes
            .into_iter()
            .map(|name| Frame::Bulk(Bytes::from(name)))
            .collect();

        Ok(Frame::Array(frames))
    })
}

/// FT.HYBRID index SEARCH query VSIM @field $param [KNN count K k [EF_RUNTIME ef]] [RANGE count RADIUS r]
///     [COMBINE RRF count [CONSTANT c] [WINDOW w] | COMBINE LINEAR count [ALPHA a] [BETA b] [WINDOW w]]
///     [FILTER expr] [LIMIT offset num] [SORTBY field [ASC|DESC]] [LOAD count field ...]
///     [PARAMS nargs name value ...]
/// Hybrid search combining text and vector similarity (Redis 8.4+).
pub fn cmd_ft_hybrid(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(4)?; // index SEARCH query VSIM...

        let index_name = cmd.get_str(0)?;
        let manager = get_search_manager(&db);
        let index = manager
            .get_index(index_name)
            .ok_or(CommandError::NoSuchKey)?;

        // Parsing state
        let mut search_query: Option<String> = None;
        let mut vsim_field: Option<String> = None;
        let mut vsim_param: Option<String> = None;
        let mut knn_k: usize = 10;
        let mut _ef_runtime: usize = 10;
        let mut _range_radius: Option<f64> = None;
        let mut limit = (0usize, 10usize);
        let mut _filter: Option<String> = None;
        let mut _sortby: Option<(String, bool)> = None; // (field, is_asc)
        let mut _load_fields: Vec<String> = Vec::new();
        let mut params: HashMap<String, Bytes> = HashMap::new();

        // Combine method: RRF or LINEAR
        enum CombineMethod {
            Rrf {
                constant: f64,
                window: usize,
            },
            Linear {
                alpha: f64,
                beta: f64,
                window: usize,
            },
        }
        let mut combine = CombineMethod::Rrf {
            constant: 60.0,
            window: 20,
        };

        // Parse options
        let mut idx = 1;
        while idx < cmd.args.len() {
            let opt = cmd.get_str(idx)?.to_uppercase();
            match opt.as_str() {
                "SEARCH" => {
                    idx += 1;
                    search_query = Some(cmd.get_str(idx)?.to_string());
                    idx += 1;
                }
                "VSIM" => {
                    idx += 1;
                    // Parse @field $param
                    let field_str = cmd.get_str(idx)?;
                    vsim_field = Some(field_str.trim_start_matches('@').to_string());
                    idx += 1;
                    let param_str = cmd.get_str(idx)?;
                    vsim_param = Some(param_str.trim_start_matches('$').to_string());
                    idx += 1;

                    // Check for KNN or RANGE
                    while idx < cmd.args.len() {
                        let sub_opt = cmd.get_str(idx)?.to_uppercase();
                        match sub_opt.as_str() {
                            "KNN" => {
                                idx += 1;
                                let _count = cmd.get_u64(idx)? as usize;
                                idx += 1;
                                // Parse K and EF_RUNTIME
                                while idx < cmd.args.len() {
                                    let knn_opt = cmd.get_str(idx)?.to_uppercase();
                                    match knn_opt.as_str() {
                                        "K" => {
                                            idx += 1;
                                            knn_k = cmd.get_u64(idx)? as usize;
                                            idx += 1;
                                        }
                                        "EF_RUNTIME" => {
                                            idx += 1;
                                            _ef_runtime = cmd.get_u64(idx)? as usize;
                                            idx += 1;
                                        }
                                        _ => break,
                                    }
                                }
                            }
                            "RANGE" => {
                                idx += 1;
                                let _count = cmd.get_u64(idx)? as usize;
                                idx += 1;
                                if idx < cmd.args.len()
                                    && cmd.get_str(idx)?.to_uppercase() == "RADIUS"
                                {
                                    idx += 1;
                                    _range_radius = Some(cmd.get_f64(idx)?);
                                    idx += 1;
                                }
                            }
                            _ => break,
                        }
                    }
                }
                "COMBINE" => {
                    idx += 1;
                    let method = cmd.get_str(idx)?.to_uppercase();
                    idx += 1;
                    let _count = cmd.get_u64(idx)? as usize;
                    idx += 1;

                    match method.as_str() {
                        "RRF" => {
                            let mut constant = 60.0;
                            let mut window = 20usize;
                            while idx < cmd.args.len() {
                                let rrf_opt = cmd.get_str(idx)?.to_uppercase();
                                match rrf_opt.as_str() {
                                    "CONSTANT" => {
                                        idx += 1;
                                        constant = cmd.get_f64(idx)?;
                                        idx += 1;
                                    }
                                    "WINDOW" => {
                                        idx += 1;
                                        window = cmd.get_u64(idx)? as usize;
                                        idx += 1;
                                    }
                                    _ => break,
                                }
                            }
                            combine = CombineMethod::Rrf { constant, window };
                        }
                        "LINEAR" => {
                            let mut alpha = 0.5;
                            let mut beta = 0.5;
                            let mut window = 20usize;
                            while idx < cmd.args.len() {
                                let lin_opt = cmd.get_str(idx)?.to_uppercase();
                                match lin_opt.as_str() {
                                    "ALPHA" => {
                                        idx += 1;
                                        alpha = cmd.get_f64(idx)?;
                                        idx += 1;
                                    }
                                    "BETA" => {
                                        idx += 1;
                                        beta = cmd.get_f64(idx)?;
                                        idx += 1;
                                    }
                                    "WINDOW" => {
                                        idx += 1;
                                        window = cmd.get_u64(idx)? as usize;
                                        idx += 1;
                                    }
                                    _ => break,
                                }
                            }
                            combine = CombineMethod::Linear {
                                alpha,
                                beta,
                                window,
                            };
                        }
                        _ => {}
                    }
                }
                "FILTER" => {
                    idx += 1;
                    _filter = Some(cmd.get_str(idx)?.to_string());
                    idx += 1;
                }
                "LIMIT" => {
                    idx += 1;
                    let offset = cmd.get_u64(idx)? as usize;
                    idx += 1;
                    let num = cmd.get_u64(idx)? as usize;
                    idx += 1;
                    limit = (offset, num);
                }
                "SORTBY" => {
                    idx += 1;
                    let _count = cmd.get_u64(idx)? as usize;
                    idx += 1;
                    let field = cmd.get_str(idx)?.to_string();
                    idx += 1;
                    let is_asc = if idx < cmd.args.len() {
                        let dir = cmd.get_str(idx)?.to_uppercase();
                        if dir == "ASC" || dir == "DESC" {
                            idx += 1;
                            dir == "ASC"
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    _sortby = Some((field, is_asc));
                }
                "LOAD" => {
                    idx += 1;
                    let count = cmd.get_u64(idx)? as usize;
                    idx += 1;
                    for _ in 0..count {
                        if idx < cmd.args.len() {
                            _load_fields.push(cmd.get_str(idx)?.to_string());
                            idx += 1;
                        }
                    }
                }
                "PARAMS" => {
                    idx += 1;
                    let nargs = cmd.get_u64(idx)? as usize;
                    idx += 1;
                    for _ in 0..(nargs / 2) {
                        if idx + 1 < cmd.args.len() {
                            let name = cmd.get_str(idx)?.to_string();
                            idx += 1;
                            let value = cmd.args[idx].clone();
                            idx += 1;
                            params.insert(name, value);
                        }
                    }
                }
                _ => {
                    idx += 1;
                }
            }
        }

        // Get vector from params if specified
        let vector_query: Option<Vec<f32>> = vsim_param.as_ref().and_then(|param_name| {
            params.get(param_name).and_then(|blob| {
                // Parse as f32 array from binary blob (little-endian)
                if blob.len() % 4 == 0 {
                    Some(
                        blob.chunks(4)
                            .map(|chunk| {
                                f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]])
                            })
                            .collect(),
                    )
                } else {
                    // Try parsing as comma-separated string
                    std::str::from_utf8(blob).ok().map(parse_vector_query)
                }
            })
        });

        // Perform hybrid search
        let mut text_results: Vec<(String, f64, usize)> = Vec::new(); // (doc_id, score, rank)
        let vector_results: Vec<(String, f64, usize)> = Vec::new();

        // Text search component
        if let Some(ref query_text) = search_query {
            let query = SearchQuery {
                query: query_text.clone(),
                tree: parse_query(query_text),
                return_fields: None,
                highlight: None,
                sort_by: None,
                limit: (0, 10000),
                with_scores: true,
                with_payloads: false,
                language: "english".to_string(),
                verbatim: false,
                no_stopwords: false,
                slop: 0,
                in_order: true,
            };

            let results = index.search(&query);
            for (rank, doc) in results.docs.into_iter().enumerate() {
                text_results.push((doc.id, doc.score, rank + 1));
            }
        }

        // Vector search component
        if let (Some(_field_name), Some(_query_vec)) = (&vsim_field, &vector_query) {
            // Stub: In a full implementation, this would query vector indices
            // For now, vector_results remains empty
        }

        // Combine results using RRF or LINEAR
        let mut combined_scores: HashMap<String, f64> = HashMap::new();

        match combine {
            CombineMethod::Rrf { constant, window } => {
                // Reciprocal Rank Fusion: score = sum(1 / (constant + rank))
                for (doc_id, _score, rank) in text_results.iter().take(window) {
                    let rrf_score = 1.0 / (constant + *rank as f64);
                    *combined_scores.entry(doc_id.clone()).or_insert(0.0) += rrf_score;
                }
                for (doc_id, _score, rank) in vector_results.iter().take(window) {
                    let rrf_score = 1.0 / (constant + *rank as f64);
                    *combined_scores.entry(doc_id.clone()).or_insert(0.0) += rrf_score;
                }
            }
            CombineMethod::Linear {
                alpha,
                beta,
                window,
            } => {
                // Linear combination: score = alpha * text_score + beta * vector_score
                let max_text = text_results
                    .iter()
                    .map(|(_, s, _)| *s)
                    .fold(0.0f64, f64::max);
                let max_vec = vector_results
                    .iter()
                    .map(|(_, s, _)| *s)
                    .fold(0.0f64, f64::max);

                for (doc_id, score, _) in text_results.iter().take(window) {
                    let norm_score = if max_text > 0.0 {
                        score / max_text
                    } else {
                        0.0
                    };
                    *combined_scores.entry(doc_id.clone()).or_insert(0.0) += alpha * norm_score;
                }
                for (doc_id, score, _) in vector_results.iter().take(window) {
                    let norm_score = if max_vec > 0.0 { score / max_vec } else { 0.0 };
                    *combined_scores.entry(doc_id.clone()).or_insert(0.0) += beta * norm_score;
                }
            }
        }

        // Sort by combined score
        let mut results: Vec<(String, f64)> = combined_scores.into_iter().collect();
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Apply limit
        let (offset, count) = limit;
        let results: Vec<_> = results
            .into_iter()
            .skip(offset)
            .take(count.min(knn_k))
            .collect();

        // Build response
        let mut response = vec![Frame::Integer(results.len() as i64)];

        for (doc_id, score) in results {
            response.push(Frame::Bulk(Bytes::from(doc_id)));
            response.push(Frame::Bulk(Bytes::from(score.to_string())));
        }

        Ok(Frame::Array(response))
    })
}

/// Parse a vector from comma or space separated string
fn parse_vector_query(s: &str) -> Vec<f32> {
    if s.contains(',') {
        s.split(',')
            .filter_map(|v| v.trim().parse::<f32>().ok())
            .collect()
    } else {
        s.split_whitespace()
            .filter_map(|v| v.parse::<f32>().ok())
            .collect()
    }
}
