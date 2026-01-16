//! Full-text search implementation (ViatorSearch).
//!
//! Provides full-text search capabilities similar to RediSearch:
//! - Inverted index for fast text search
//! - Multiple field types (TEXT, NUMERIC, TAG, GEO)
//! - Query parsing with boolean operators
//! - BM25 ranking
//! - Aggregations
//!
//! ## Example
//!
//! ```ignore
//! FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA title TEXT WEIGHT 5.0 body TEXT
//! FT.ADD idx doc:1 1.0 FIELDS title "Hello World" body "This is a test"
//! FT.SEARCH idx "hello world"
//! ```

use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// A search index.
#[derive(Debug)]
pub struct SearchIndex {
    /// Index name
    pub name: String,
    /// Index schema
    pub schema: IndexSchema,
    /// Inverted index for text fields
    inverted_index: DashMap<String, PostingList>,
    /// Numeric index for range queries
    numeric_index: DashMap<String, BTreeMap<i64, HashSet<String>>>,
    /// Tag index for exact match
    tag_index: DashMap<String, DashMap<String, HashSet<String>>>,
    /// Document store
    documents: DashMap<String, Document>,
    /// Document count
    doc_count: AtomicU64,
    /// Total terms in index (for BM25)
    total_terms: AtomicU64,
    /// Average document length (for BM25)
    avg_doc_length: RwLock<f64>,
    /// Index options
    pub options: IndexOptions,
}

/// Index schema definition.
#[derive(Debug, Clone)]
pub struct IndexSchema {
    /// Fields in the schema
    pub fields: Vec<FieldDefinition>,
    /// Field name to index mapping
    field_map: HashMap<String, usize>,
}

/// Field definition in schema.
#[derive(Debug, Clone)]
pub struct FieldDefinition {
    /// Field name
    pub name: String,
    /// Field type
    pub field_type: FieldType,
    /// Field weight for scoring
    pub weight: f64,
    /// Is field sortable
    pub sortable: bool,
    /// Is field not indexed (stored only)
    pub no_index: bool,
    /// Phonetic matching algorithm
    pub phonetic: Option<String>,
}

/// Field types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldType {
    /// Full-text searchable field
    Text,
    /// Numeric field for range queries
    Numeric,
    /// Tag field for exact matching
    Tag,
    /// Geo field for location queries
    Geo,
    /// Vector field for similarity search
    Vector,
}

/// Index options.
#[derive(Debug, Clone)]
pub struct IndexOptions {
    /// Key prefixes to index
    pub prefixes: Vec<String>,
    /// Index on HASH or JSON
    pub on_type: IndexOnType,
    /// Default language for stemming
    pub language: String,
    /// Stop words
    pub stopwords: HashSet<String>,
    /// Temporary index (not persisted)
    pub temporary: bool,
    /// No highlighting
    pub no_highlight: bool,
    /// No field flags
    pub no_fields: bool,
    /// No frequencies
    pub no_freqs: bool,
    /// No offsets
    pub no_offsets: bool,
    /// Maximum text fields
    pub max_text_fields: usize,
}

impl Default for IndexOptions {
    fn default() -> Self {
        Self {
            prefixes: Vec::new(),
            on_type: IndexOnType::Hash,
            language: "english".to_string(),
            stopwords: default_stopwords(),
            temporary: false,
            no_highlight: false,
            no_fields: false,
            no_freqs: false,
            no_offsets: false,
            max_text_fields: 32,
        }
    }
}

/// Type of data to index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IndexOnType {
    /// Index HASH keys
    #[default]
    Hash,
    /// Index JSON keys
    Json,
}

/// A posting list entry.
#[derive(Debug, Clone)]
pub struct PostingList {
    /// Document IDs containing this term
    docs: HashSet<String>,
    /// Term frequency per document
    term_freq: HashMap<String, u32>,
    /// Field positions per document (for phrase queries)
    positions: HashMap<String, Vec<u32>>,
}

impl PostingList {
    fn new() -> Self {
        Self {
            docs: HashSet::new(),
            term_freq: HashMap::new(),
            positions: HashMap::new(),
        }
    }

    fn add(&mut self, doc_id: &str, positions: Vec<u32>) {
        self.docs.insert(doc_id.to_string());
        self.term_freq
            .insert(doc_id.to_string(), positions.len() as u32);
        self.positions.insert(doc_id.to_string(), positions);
    }

    fn remove(&mut self, doc_id: &str) {
        self.docs.remove(doc_id);
        self.term_freq.remove(doc_id);
        self.positions.remove(doc_id);
    }

    fn document_count(&self) -> usize {
        self.docs.len()
    }
}

/// A document in the index.
#[derive(Debug, Clone)]
pub struct Document {
    /// Document ID (key name)
    pub id: String,
    /// Document score (user-provided)
    pub score: f64,
    /// Field values
    pub fields: HashMap<String, FieldValue>,
    /// Document length in terms
    pub length: u32,
}

/// A field value.
#[derive(Debug, Clone)]
pub enum FieldValue {
    /// Text value
    Text(String),
    /// Numeric value
    Numeric(f64),
    /// Tag value(s)
    Tag(Vec<String>),
    /// Geo value (lat, lon)
    Geo(f64, f64),
    /// Vector value
    Vector(Vec<f32>),
}

/// Search query.
#[derive(Debug, Clone)]
pub struct SearchQuery {
    /// Query text
    pub query: String,
    /// Parsed query tree
    pub tree: QueryNode,
    /// Fields to return
    pub return_fields: Option<Vec<String>>,
    /// Highlight options
    pub highlight: Option<HighlightOptions>,
    /// Sort by field
    pub sort_by: Option<(String, bool)>, // (field, ascending)
    /// Limit results
    pub limit: (usize, usize), // (offset, count)
    /// Include scores
    pub with_scores: bool,
    /// Include payloads
    pub with_payloads: bool,
    /// Language for stemming
    pub language: String,
    /// Verbatim (no stemming)
    pub verbatim: bool,
    /// No stopwords
    pub no_stopwords: bool,
    /// Slop for phrase queries
    pub slop: u32,
    /// In-order phrase matching
    pub in_order: bool,
}

/// Query node for parsed queries.
#[derive(Debug, Clone)]
pub enum QueryNode {
    /// Single term
    Term(String),
    /// Phrase (multiple terms in order)
    Phrase(Vec<String>),
    /// Prefix match
    Prefix(String),
    /// Wildcard match
    Wildcard(String),
    /// Fuzzy match
    Fuzzy(String, u8),
    /// Numeric range
    NumericRange(String, Option<f64>, Option<f64>),
    /// Tag match
    Tag(String, Vec<String>),
    /// Geo radius
    GeoRadius(String, f64, f64, f64, GeoUnit),
    /// Boolean AND
    And(Vec<QueryNode>),
    /// Boolean OR
    Or(Vec<QueryNode>),
    /// Boolean NOT
    Not(Box<QueryNode>),
    /// Optional (boost if present)
    Optional(Box<QueryNode>),
    /// Field-specific query
    Field(String, Box<QueryNode>),
}

/// Geo distance units.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeoUnit {
    Meters,
    Kilometers,
    Miles,
    Feet,
}

/// Highlight options.
#[derive(Debug, Clone)]
pub struct HighlightOptions {
    /// Fields to highlight
    pub fields: Option<Vec<String>>,
    /// Open tag
    pub open_tag: String,
    /// Close tag
    pub close_tag: String,
}

impl Default for HighlightOptions {
    fn default() -> Self {
        Self {
            fields: None,
            open_tag: "<b>".to_string(),
            close_tag: "</b>".to_string(),
        }
    }
}

/// Search result.
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// Total number of results
    pub total: u64,
    /// Result documents
    pub docs: Vec<SearchResultDoc>,
}

/// A document in search results.
#[derive(Debug, Clone)]
pub struct SearchResultDoc {
    /// Document ID
    pub id: String,
    /// BM25 score
    pub score: f64,
    /// Field values
    pub fields: HashMap<String, String>,
    /// Highlighted snippets
    pub highlights: HashMap<String, String>,
}

impl SearchIndex {
    /// Create a new search index.
    pub fn new(name: String, schema: IndexSchema, options: IndexOptions) -> Self {
        Self {
            name,
            schema,
            inverted_index: DashMap::new(),
            numeric_index: DashMap::new(),
            tag_index: DashMap::new(),
            documents: DashMap::new(),
            doc_count: AtomicU64::new(0),
            total_terms: AtomicU64::new(0),
            avg_doc_length: RwLock::new(0.0),
            options,
        }
    }

    /// Add a document to the index.
    pub fn add_document(&self, doc_id: &str, score: f64, fields: HashMap<String, FieldValue>) {
        // Remove existing document if present
        self.remove_document(doc_id);

        let mut doc_length = 0u32;

        // Index each field
        for (field_name, value) in &fields {
            if let Some(field_def) = self.schema.get_field(field_name) {
                if field_def.no_index {
                    continue;
                }

                match (&field_def.field_type, value) {
                    (FieldType::Text, FieldValue::Text(text)) => {
                        let terms = tokenize(text, &self.options.stopwords);
                        doc_length += terms.len() as u32;

                        // Build term positions map
                        let mut term_positions: HashMap<String, Vec<u32>> = HashMap::new();
                        for (pos, term) in terms.iter().enumerate() {
                            term_positions
                                .entry(term.clone())
                                .or_default()
                                .push(pos as u32);
                        }

                        // Add to inverted index
                        for (term, positions) in term_positions {
                            let key = format!("{}:{}", field_name, term);
                            self.inverted_index
                                .entry(key)
                                .or_insert_with(PostingList::new)
                                .add(doc_id, positions);
                        }

                        self.total_terms
                            .fetch_add(terms.len() as u64, Ordering::Relaxed);
                    }
                    (FieldType::Numeric, FieldValue::Numeric(n)) => {
                        let key = field_name.clone();
                        self.numeric_index
                            .entry(key)
                            .or_default()
                            .entry(*n as i64)
                            .or_default()
                            .insert(doc_id.to_string());
                    }
                    (FieldType::Tag, FieldValue::Tag(tags)) => {
                        let field_tags = self.tag_index.entry(field_name.clone()).or_default();
                        for tag in tags {
                            field_tags
                                .entry(tag.to_lowercase())
                                .or_default()
                                .insert(doc_id.to_string());
                        }
                    }
                    _ => {}
                }
            }
        }

        // Store document
        let document = Document {
            id: doc_id.to_string(),
            score,
            fields,
            length: doc_length,
        };
        self.documents.insert(doc_id.to_string(), document);

        // Update stats
        let count = self.doc_count.fetch_add(1, Ordering::Relaxed) + 1;
        let total = self.total_terms.load(Ordering::Relaxed);
        *self.avg_doc_length.write() = total as f64 / count as f64;
    }

    /// Remove a document from the index.
    pub fn remove_document(&self, doc_id: &str) -> bool {
        if let Some((_, doc)) = self.documents.remove(doc_id) {
            // Remove from inverted index
            for (field_name, value) in &doc.fields {
                if let FieldValue::Text(text) = value {
                    let terms = tokenize(text, &self.options.stopwords);
                    for term in terms {
                        let key = format!("{}:{}", field_name, term);
                        if let Some(mut posting) = self.inverted_index.get_mut(&key) {
                            posting.remove(doc_id);
                            if posting.document_count() == 0 {
                                drop(posting);
                                self.inverted_index.remove(&key);
                            }
                        }
                    }
                }
            }

            self.doc_count.fetch_sub(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Search the index.
    pub fn search(&self, query: &SearchQuery) -> SearchResult {
        // Execute query and get matching doc IDs with scores
        let matches = self.execute_query(&query.tree, &query.language);

        // Sort by score or specified field
        let mut results: Vec<(String, f64)> = matches.into_iter().collect();

        if let Some((field, ascending)) = &query.sort_by {
            results.sort_by(|a, b| {
                let va = self.documents.get(&a.0).and_then(|d| {
                    d.fields.get(field).and_then(|v| match v {
                        FieldValue::Numeric(n) => Some(*n),
                        _ => None,
                    })
                });
                let vb = self.documents.get(&b.0).and_then(|d| {
                    d.fields.get(field).and_then(|v| match v {
                        FieldValue::Numeric(n) => Some(*n),
                        _ => None,
                    })
                });
                let cmp = va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal);
                if *ascending { cmp } else { cmp.reverse() }
            });
        } else {
            // Sort by score descending
            results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        }

        let total = results.len() as u64;

        // Apply limit
        let (offset, count) = query.limit;
        let results: Vec<_> = results.into_iter().skip(offset).take(count).collect();

        // Build result documents
        let docs: Vec<SearchResultDoc> = results
            .into_iter()
            .filter_map(|(doc_id, score)| {
                self.documents.get(&doc_id).map(|doc| {
                    let fields: HashMap<String, String> = doc
                        .fields
                        .iter()
                        .filter(|(name, _)| {
                            query
                                .return_fields
                                .as_ref()
                                .map(|rf| rf.contains(*name))
                                .unwrap_or(true)
                        })
                        .map(|(k, v)| {
                            let s = match v {
                                FieldValue::Text(t) => t.clone(),
                                FieldValue::Numeric(n) => n.to_string(),
                                FieldValue::Tag(tags) => tags.join(","),
                                FieldValue::Geo(lat, lon) => format!("{},{}", lat, lon),
                                FieldValue::Vector(_) => "[vector]".to_string(),
                            };
                            (k.clone(), s)
                        })
                        .collect();

                    SearchResultDoc {
                        id: doc_id,
                        score,
                        fields,
                        highlights: HashMap::new(), // TODO: implement highlighting
                    }
                })
            })
            .collect();

        SearchResult { total, docs }
    }

    /// Execute a query node and return matching documents with scores.
    fn execute_query(&self, node: &QueryNode, _language: &str) -> HashMap<String, f64> {
        match node {
            QueryNode::Term(term) => self.search_term(term, None),
            QueryNode::Phrase(terms) => self.search_phrase(terms, None),
            QueryNode::Prefix(prefix) => self.search_prefix(prefix, None),
            QueryNode::And(nodes) => {
                let mut result: Option<HashMap<String, f64>> = None;
                for node in nodes {
                    let matches = self.execute_query(node, _language);
                    result = Some(match result {
                        None => matches,
                        Some(existing) => {
                            // Intersection with score addition
                            existing
                                .into_iter()
                                .filter_map(|(id, score)| matches.get(&id).map(|s| (id, score + s)))
                                .collect()
                        }
                    });
                }
                result.unwrap_or_default()
            }
            QueryNode::Or(nodes) => {
                let mut result: HashMap<String, f64> = HashMap::new();
                for node in nodes {
                    for (id, score) in self.execute_query(node, _language) {
                        *result.entry(id).or_insert(0.0) += score;
                    }
                }
                result
            }
            QueryNode::Not(inner) => {
                let excluded = self.execute_query(inner, _language);
                // Return all documents NOT in excluded set (with score 1.0)
                self.documents
                    .iter()
                    .filter(|e| !excluded.contains_key(e.key()))
                    .map(|e| (e.key().clone(), 1.0))
                    .collect()
            }
            QueryNode::Field(field, inner) => {
                // Execute query but restrict to specific field
                match inner.as_ref() {
                    QueryNode::Term(term) => self.search_term(term, Some(field)),
                    QueryNode::Prefix(prefix) => self.search_prefix(prefix, Some(field)),
                    _ => self.execute_query(inner, _language),
                }
            }
            QueryNode::NumericRange(field, min, max) => {
                self.search_numeric_range(field, *min, *max)
            }
            QueryNode::Tag(field, tags) => self.search_tags(field, tags),
            _ => HashMap::new(),
        }
    }

    /// Search for a single term.
    fn search_term(&self, term: &str, field: Option<&str>) -> HashMap<String, f64> {
        let term_lower = term.to_lowercase();
        let mut results = HashMap::new();

        // Search in specified field or all text fields
        let fields: Vec<&str> = if let Some(f) = field {
            vec![f]
        } else {
            self.schema
                .fields
                .iter()
                .filter(|f| f.field_type == FieldType::Text)
                .map(|f| f.name.as_str())
                .collect()
        };

        for field_name in fields {
            let key = format!("{}:{}", field_name, term_lower);
            if let Some(posting) = self.inverted_index.get(&key) {
                let weight = self
                    .schema
                    .get_field(field_name)
                    .map(|f| f.weight)
                    .unwrap_or(1.0);

                for doc_id in posting.docs.iter() {
                    let tf = posting.term_freq.get(doc_id).copied().unwrap_or(1) as f64;
                    let doc_len = self
                        .documents
                        .get(doc_id)
                        .map(|d| d.length as f64)
                        .unwrap_or(1.0);
                    let avg_len = *self.avg_doc_length.read();
                    let doc_count = self.doc_count.load(Ordering::Relaxed) as f64;
                    let df = posting.document_count() as f64;

                    // BM25 scoring
                    let k1 = 1.2;
                    let b = 0.75;
                    let idf = ((doc_count - df + 0.5) / (df + 0.5) + 1.0).ln();
                    let tf_norm =
                        (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * doc_len / avg_len.max(1.0)));
                    let score = idf * tf_norm * weight;

                    *results.entry(doc_id.clone()).or_insert(0.0) += score;
                }
            }
        }

        results
    }

    /// Search for a phrase.
    fn search_phrase(&self, terms: &[String], field: Option<&str>) -> HashMap<String, f64> {
        if terms.is_empty() {
            return HashMap::new();
        }

        // Start with first term
        let mut candidates = self.search_term(&terms[0], field);

        // Filter by subsequent terms and positions
        for (_i, term) in terms.iter().enumerate().skip(1) {
            let term_matches = self.search_term(term, field);

            // Keep only documents that have this term at the right position
            candidates.retain(|doc_id, _| {
                if !term_matches.contains_key(doc_id) {
                    return false;
                }

                // Check positions (simplified - would need more complex logic for proper phrase matching)
                true
            });
        }

        candidates
    }

    /// Search for terms with a prefix.
    fn search_prefix(&self, prefix: &str, field: Option<&str>) -> HashMap<String, f64> {
        let prefix_lower = prefix.to_lowercase();
        let mut results = HashMap::new();

        // Find all matching terms
        for entry in self.inverted_index.iter() {
            let key = entry.key();
            let parts: Vec<&str> = key.splitn(2, ':').collect();
            if parts.len() != 2 {
                continue;
            }

            let (field_name, term) = (parts[0], parts[1]);

            // Check field filter
            if let Some(f) = field {
                if field_name != f {
                    continue;
                }
            }

            if term.starts_with(&prefix_lower) {
                let posting = entry.value();
                for doc_id in posting.docs.iter() {
                    *results.entry(doc_id.clone()).or_insert(0.0) += 1.0;
                }
            }
        }

        results
    }

    /// Search numeric range.
    fn search_numeric_range(
        &self,
        field: &str,
        min: Option<f64>,
        max: Option<f64>,
    ) -> HashMap<String, f64> {
        let mut results = HashMap::new();

        if let Some(index) = self.numeric_index.get(field) {
            let min_i = min.map(|m| m as i64).unwrap_or(i64::MIN);
            let max_i = max.map(|m| m as i64).unwrap_or(i64::MAX);

            for (_, doc_ids) in index.range(min_i..=max_i) {
                for doc_id in doc_ids {
                    results.insert(doc_id.clone(), 1.0);
                }
            }
        }

        results
    }

    /// Search for tags.
    fn search_tags(&self, field: &str, tags: &[String]) -> HashMap<String, f64> {
        let mut results = HashMap::new();

        if let Some(field_tags) = self.tag_index.get(field) {
            for tag in tags {
                if let Some(doc_ids) = field_tags.get(&tag.to_lowercase()) {
                    for doc_id in doc_ids.iter() {
                        *results.entry(doc_id.clone()).or_insert(0.0) += 1.0;
                    }
                }
            }
        }

        results
    }

    /// Get document count.
    #[must_use]
    pub fn doc_count(&self) -> u64 {
        self.doc_count.load(Ordering::Relaxed)
    }

    /// Get index info.
    #[must_use]
    pub fn info(&self) -> IndexInfo {
        IndexInfo {
            name: self.name.clone(),
            num_docs: self.doc_count.load(Ordering::Relaxed),
            num_terms: self.inverted_index.len() as u64,
            num_records: self.total_terms.load(Ordering::Relaxed),
            avg_doc_length: *self.avg_doc_length.read(),
            schema: self.schema.clone(),
        }
    }
}

impl IndexSchema {
    /// Create a new schema.
    pub fn new(fields: Vec<FieldDefinition>) -> Self {
        let field_map: HashMap<String, usize> = fields
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name.clone(), i))
            .collect();

        Self { fields, field_map }
    }

    /// Get a field definition by name.
    #[must_use]
    pub fn get_field(&self, name: &str) -> Option<&FieldDefinition> {
        self.field_map.get(name).map(|&i| &self.fields[i])
    }
}

/// Index information.
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// Index name
    pub name: String,
    /// Number of documents
    pub num_docs: u64,
    /// Number of unique terms
    pub num_terms: u64,
    /// Total term occurrences
    pub num_records: u64,
    /// Average document length
    pub avg_doc_length: f64,
    /// Schema
    pub schema: IndexSchema,
}

/// Tokenize text into terms.
fn tokenize(text: &str, stopwords: &HashSet<String>) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty() && s.len() > 1 && !stopwords.contains(*s))
        .map(String::from)
        .collect()
}

/// Default English stopwords.
fn default_stopwords() -> HashSet<String> {
    [
        "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is",
        "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there",
        "these", "they", "this", "to", "was", "will", "with",
    ]
    .iter()
    .map(|&s| s.to_string())
    .collect()
}

/// Parse a simple query string into a query tree.
pub fn parse_query(query: &str) -> QueryNode {
    let terms: Vec<String> = query.split_whitespace().map(|s| s.to_lowercase()).collect();

    if terms.is_empty() {
        return QueryNode::Term(String::new());
    }

    if terms.len() == 1 {
        let term = &terms[0];
        if term.ends_with('*') {
            return QueryNode::Prefix(term[..term.len() - 1].to_string());
        }
        return QueryNode::Term(term.clone());
    }

    // Multiple terms - treat as AND query
    QueryNode::And(
        terms
            .into_iter()
            .map(|t| {
                if t.ends_with('*') {
                    QueryNode::Prefix(t[..t.len() - 1].to_string())
                } else {
                    QueryNode::Term(t)
                }
            })
            .collect(),
    )
}

/// Global search index manager.
#[derive(Debug, Default)]
pub struct SearchManager {
    /// All indexes
    indexes: DashMap<String, Arc<SearchIndex>>,
}

impl SearchManager {
    /// Create a new search manager.
    #[must_use]
    pub fn new() -> Self {
        Self {
            indexes: DashMap::new(),
        }
    }

    /// Create a new index.
    pub fn create_index(
        &self,
        name: &str,
        schema: IndexSchema,
        options: IndexOptions,
    ) -> Result<(), SearchError> {
        if self.indexes.contains_key(name) {
            return Err(SearchError::IndexExists(name.to_string()));
        }

        let index = Arc::new(SearchIndex::new(name.to_string(), schema, options));
        self.indexes.insert(name.to_string(), index);
        Ok(())
    }

    /// Drop an index.
    pub fn drop_index(&self, name: &str) -> Result<(), SearchError> {
        self.indexes
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| SearchError::IndexNotFound(name.to_string()))
    }

    /// Get an index.
    #[must_use]
    pub fn get_index(&self, name: &str) -> Option<Arc<SearchIndex>> {
        self.indexes.get(name).map(|e| e.clone())
    }

    /// List all indexes.
    #[must_use]
    pub fn list_indexes(&self) -> Vec<String> {
        self.indexes.iter().map(|e| e.key().clone()).collect()
    }
}

/// Search errors.
#[derive(Debug, Clone, thiserror::Error)]
pub enum SearchError {
    /// Index already exists
    #[error("index already exists: {0}")]
    IndexExists(String),
    /// Index not found
    #[error("index not found: {0}")]
    IndexNotFound(String),
    /// Invalid schema
    #[error("invalid schema: {0}")]
    InvalidSchema(String),
    /// Query parse error
    #[error("query parse error: {0}")]
    QueryParseError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize() {
        let stopwords = default_stopwords();
        let tokens = tokenize("Hello World! This is a test.", &stopwords);
        assert_eq!(tokens, vec!["hello", "world", "test"]);
    }

    #[test]
    fn test_search_index() {
        let schema = IndexSchema::new(vec![
            FieldDefinition {
                name: "title".to_string(),
                field_type: FieldType::Text,
                weight: 5.0,
                sortable: false,
                no_index: false,
                phonetic: None,
            },
            FieldDefinition {
                name: "body".to_string(),
                field_type: FieldType::Text,
                weight: 1.0,
                sortable: false,
                no_index: false,
                phonetic: None,
            },
        ]);

        let index = SearchIndex::new("test".to_string(), schema, IndexOptions::default());

        // Add documents
        let mut fields1 = HashMap::new();
        fields1.insert(
            "title".to_string(),
            FieldValue::Text("Hello World".to_string()),
        );
        fields1.insert(
            "body".to_string(),
            FieldValue::Text("This is a test document.".to_string()),
        );
        index.add_document("doc:1", 1.0, fields1);

        let mut fields2 = HashMap::new();
        fields2.insert(
            "title".to_string(),
            FieldValue::Text("Another Document".to_string()),
        );
        fields2.insert(
            "body".to_string(),
            FieldValue::Text("Hello from another doc.".to_string()),
        );
        index.add_document("doc:2", 1.0, fields2);

        assert_eq!(index.doc_count(), 2);

        // Search
        let query = SearchQuery {
            query: "hello".to_string(),
            tree: parse_query("hello"),
            return_fields: None,
            highlight: None,
            sort_by: None,
            limit: (0, 10),
            with_scores: true,
            with_payloads: false,
            language: "english".to_string(),
            verbatim: false,
            no_stopwords: false,
            slop: 0,
            in_order: true,
        };

        let results = index.search(&query);
        assert_eq!(results.total, 2);
    }

    #[test]
    fn test_parse_query() {
        let node = parse_query("hello world");
        assert!(matches!(node, QueryNode::And(_)));

        let node = parse_query("test*");
        assert!(matches!(node, QueryNode::Prefix(_)));
    }
}
