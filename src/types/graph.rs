//! Graph database implementation (ViatorGraph).
//!
//! Provides graph database capabilities:
//! - Property graph model (nodes, edges, properties)
//! - Cypher-like query language
//! - Path finding algorithms
//! - Graph traversals
//!
//! ## Example
//!
//! ```ignore
//! GRAPH.QUERY social "CREATE (n:Person {name: 'Alice'})"
//! GRAPH.QUERY social "MATCH (n:Person) RETURN n"
//! ```

use dashmap::DashMap;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// A graph database.
#[derive(Debug)]
pub struct Graph {
    /// Graph name
    pub name: String,
    /// Nodes by ID
    nodes: DashMap<u64, Node>,
    /// Edges by ID
    edges: DashMap<u64, Edge>,
    /// Node index by label
    label_index: DashMap<String, HashSet<u64>>,
    /// Node index by property
    property_index: DashMap<(String, String, PropertyValue), HashSet<u64>>,
    /// Next node ID
    next_node_id: AtomicU64,
    /// Next edge ID
    next_edge_id: AtomicU64,
    /// Statistics
    stats: GraphStats,
}

/// A node in the graph.
#[derive(Debug, Clone)]
pub struct Node {
    /// Node ID
    pub id: u64,
    /// Node labels
    pub labels: Vec<String>,
    /// Node properties
    pub properties: HashMap<String, PropertyValue>,
    /// Outgoing edge IDs
    pub outgoing: HashSet<u64>,
    /// Incoming edge IDs
    pub incoming: HashSet<u64>,
}

/// An edge in the graph.
#[derive(Debug, Clone)]
pub struct Edge {
    /// Edge ID
    pub id: u64,
    /// Source node ID
    pub src: u64,
    /// Destination node ID
    pub dst: u64,
    /// Relationship type
    pub rel_type: String,
    /// Edge properties
    pub properties: HashMap<String, PropertyValue>,
}

/// Property value types.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PropertyValue {
    /// Null value
    Null,
    /// Boolean
    Bool(bool),
    /// Integer
    Integer(i64),
    /// Float (stored as bits for hashing)
    Float(u64),
    /// String
    String(String),
    /// List of values
    List(Vec<PropertyValue>),
}

impl PropertyValue {
    /// Create a float property.
    pub fn float(f: f64) -> Self {
        Self::Float(f.to_bits())
    }

    /// Get as float.
    pub fn as_float(&self) -> Option<f64> {
        match self {
            Self::Float(bits) => Some(f64::from_bits(*bits)),
            Self::Integer(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Get as integer.
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Self::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// Get as string.
    pub fn as_string(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s),
            _ => None,
        }
    }

    /// Get as bool.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(*b),
            _ => None,
        }
    }
}

impl std::fmt::Display for PropertyValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "null"),
            Self::Bool(b) => write!(f, "{}", b),
            Self::Integer(i) => write!(f, "{}", i),
            Self::Float(bits) => write!(f, "{}", f64::from_bits(*bits)),
            Self::String(s) => write!(f, "\"{}\"", s),
            Self::List(l) => {
                write!(f, "[")?;
                for (i, v) in l.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", v)?;
                }
                write!(f, "]")
            }
        }
    }
}

/// Graph statistics.
#[derive(Debug, Default)]
pub struct GraphStats {
    /// Number of nodes
    pub node_count: AtomicU64,
    /// Number of edges
    pub edge_count: AtomicU64,
    /// Number of labels
    pub label_count: AtomicU64,
    /// Number of relationship types
    pub rel_type_count: AtomicU64,
    /// Queries executed
    pub queries_executed: AtomicU64,
}

impl Graph {
    /// Create a new graph.
    pub fn new(name: String) -> Self {
        Self {
            name,
            nodes: DashMap::new(),
            edges: DashMap::new(),
            label_index: DashMap::new(),
            property_index: DashMap::new(),
            next_node_id: AtomicU64::new(1),
            next_edge_id: AtomicU64::new(1),
            stats: GraphStats::default(),
        }
    }

    /// Create a node.
    pub fn create_node(
        &self,
        labels: Vec<String>,
        properties: HashMap<String, PropertyValue>,
    ) -> u64 {
        let id = self.next_node_id.fetch_add(1, Ordering::Relaxed);

        // Add to label index
        for label in &labels {
            self.label_index
                .entry(label.clone())
                .or_default()
                .insert(id);
        }

        // Add to property index (for indexed properties)
        for (key, value) in &properties {
            self.property_index
                .entry((String::new(), key.clone(), value.clone()))
                .or_default()
                .insert(id);
        }

        let node = Node {
            id,
            labels,
            properties,
            outgoing: HashSet::new(),
            incoming: HashSet::new(),
        };

        self.nodes.insert(id, node);
        self.stats.node_count.fetch_add(1, Ordering::Relaxed);

        id
    }

    /// Create an edge.
    pub fn create_edge(
        &self,
        src: u64,
        dst: u64,
        rel_type: String,
        properties: HashMap<String, PropertyValue>,
    ) -> Option<u64> {
        // Verify both nodes exist
        if !self.nodes.contains_key(&src) || !self.nodes.contains_key(&dst) {
            return None;
        }

        let id = self.next_edge_id.fetch_add(1, Ordering::Relaxed);

        let edge = Edge {
            id,
            src,
            dst,
            rel_type,
            properties,
        };

        // Update node adjacency lists
        if let Some(mut node) = self.nodes.get_mut(&src) {
            node.outgoing.insert(id);
        }
        if let Some(mut node) = self.nodes.get_mut(&dst) {
            node.incoming.insert(id);
        }

        self.edges.insert(id, edge);
        self.stats.edge_count.fetch_add(1, Ordering::Relaxed);

        Some(id)
    }

    /// Delete a node and all its edges.
    pub fn delete_node(&self, id: u64) -> bool {
        if let Some((_, node)) = self.nodes.remove(&id) {
            // Delete all outgoing edges
            for edge_id in &node.outgoing {
                self.delete_edge_internal(*edge_id);
            }

            // Delete all incoming edges
            for edge_id in &node.incoming {
                self.delete_edge_internal(*edge_id);
            }

            // Remove from label index
            for label in &node.labels {
                if let Some(mut ids) = self.label_index.get_mut(label) {
                    ids.remove(&id);
                }
            }

            self.stats.node_count.fetch_sub(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Delete an edge.
    pub fn delete_edge(&self, id: u64) -> bool {
        self.delete_edge_internal(id)
    }

    fn delete_edge_internal(&self, id: u64) -> bool {
        if let Some((_, edge)) = self.edges.remove(&id) {
            // Update source node
            if let Some(mut node) = self.nodes.get_mut(&edge.src) {
                node.outgoing.remove(&id);
            }

            // Update destination node
            if let Some(mut node) = self.nodes.get_mut(&edge.dst) {
                node.incoming.remove(&id);
            }

            self.stats.edge_count.fetch_sub(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Get a node by ID.
    pub fn get_node(&self, id: u64) -> Option<Node> {
        self.nodes.get(&id).map(|n| n.clone())
    }

    /// Get an edge by ID.
    pub fn get_edge(&self, id: u64) -> Option<Edge> {
        self.edges.get(&id).map(|e| e.clone())
    }

    /// Get nodes by label.
    pub fn get_nodes_by_label(&self, label: &str) -> Vec<Node> {
        if let Some(ids) = self.label_index.get(label) {
            ids.iter()
                .filter_map(|id| self.nodes.get(id).map(|n| n.clone()))
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Get outgoing edges from a node.
    pub fn get_outgoing_edges(&self, node_id: u64) -> Vec<Edge> {
        if let Some(node) = self.nodes.get(&node_id) {
            node.outgoing
                .iter()
                .filter_map(|id| self.edges.get(id).map(|e| e.clone()))
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Get incoming edges to a node.
    pub fn get_incoming_edges(&self, node_id: u64) -> Vec<Edge> {
        if let Some(node) = self.nodes.get(&node_id) {
            node.incoming
                .iter()
                .filter_map(|id| self.edges.get(id).map(|e| e.clone()))
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Get neighbors of a node.
    pub fn get_neighbors(&self, node_id: u64, direction: Direction) -> Vec<u64> {
        let mut neighbors = Vec::new();

        if let Some(node) = self.nodes.get(&node_id) {
            match direction {
                Direction::Outgoing => {
                    for edge_id in &node.outgoing {
                        if let Some(edge) = self.edges.get(edge_id) {
                            neighbors.push(edge.dst);
                        }
                    }
                }
                Direction::Incoming => {
                    for edge_id in &node.incoming {
                        if let Some(edge) = self.edges.get(edge_id) {
                            neighbors.push(edge.src);
                        }
                    }
                }
                Direction::Both => {
                    for edge_id in &node.outgoing {
                        if let Some(edge) = self.edges.get(edge_id) {
                            neighbors.push(edge.dst);
                        }
                    }
                    for edge_id in &node.incoming {
                        if let Some(edge) = self.edges.get(edge_id) {
                            neighbors.push(edge.src);
                        }
                    }
                }
            }
        }

        neighbors
    }

    /// Find shortest path between two nodes (BFS).
    pub fn shortest_path(&self, src: u64, dst: u64) -> Option<Vec<u64>> {
        if src == dst {
            return Some(vec![src]);
        }

        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut parent: HashMap<u64, u64> = HashMap::new();

        visited.insert(src);
        queue.push_back(src);

        while let Some(current) = queue.pop_front() {
            for neighbor in self.get_neighbors(current, Direction::Outgoing) {
                if !visited.contains(&neighbor) {
                    visited.insert(neighbor);
                    parent.insert(neighbor, current);

                    if neighbor == dst {
                        // Reconstruct path
                        let mut path = vec![dst];
                        let mut node = dst;
                        while let Some(&p) = parent.get(&node) {
                            path.push(p);
                            node = p;
                        }
                        path.reverse();
                        return Some(path);
                    }

                    queue.push_back(neighbor);
                }
            }
        }

        None
    }

    /// Get all paths between two nodes (up to max depth).
    pub fn all_paths(&self, src: u64, dst: u64, max_depth: usize) -> Vec<Vec<u64>> {
        let mut paths = Vec::new();
        let mut current_path = vec![src];
        let mut visited = HashSet::new();
        visited.insert(src);

        self.dfs_paths(
            src,
            dst,
            max_depth,
            &mut current_path,
            &mut visited,
            &mut paths,
        );

        paths
    }

    fn dfs_paths(
        &self,
        current: u64,
        dst: u64,
        max_depth: usize,
        path: &mut Vec<u64>,
        visited: &mut HashSet<u64>,
        paths: &mut Vec<Vec<u64>>,
    ) {
        if current == dst {
            paths.push(path.clone());
            return;
        }

        if path.len() >= max_depth {
            return;
        }

        for neighbor in self.get_neighbors(current, Direction::Outgoing) {
            if !visited.contains(&neighbor) {
                visited.insert(neighbor);
                path.push(neighbor);

                self.dfs_paths(neighbor, dst, max_depth, path, visited, paths);

                path.pop();
                visited.remove(&neighbor);
            }
        }
    }

    /// Match nodes by pattern.
    pub fn match_pattern(&self, pattern: &MatchPattern) -> Vec<MatchResult> {
        let mut results = Vec::new();

        // Start with nodes matching the first node pattern
        let start_nodes: Vec<Node> = if let Some(label) = &pattern.node_pattern.label {
            self.get_nodes_by_label(label)
        } else {
            self.nodes.iter().map(|e| e.clone()).collect()
        };

        // Filter by properties
        for node in start_nodes {
            let mut matches = true;
            for (key, value) in &pattern.node_pattern.properties {
                if node.properties.get(key) != Some(value) {
                    matches = false;
                    break;
                }
            }

            if matches {
                results.push(MatchResult {
                    bindings: vec![(pattern.node_pattern.alias.clone(), MatchValue::Node(node))],
                });
            }
        }

        self.stats.queries_executed.fetch_add(1, Ordering::Relaxed);
        results
    }

    /// Get graph statistics.
    #[must_use]
    pub fn stats(&self) -> &GraphStats {
        &self.stats
    }

    /// Get node count.
    #[must_use]
    pub fn node_count(&self) -> u64 {
        self.stats.node_count.load(Ordering::Relaxed)
    }

    /// Get edge count.
    #[must_use]
    pub fn edge_count(&self) -> u64 {
        self.stats.edge_count.load(Ordering::Relaxed)
    }

    /// Get all labels.
    #[must_use]
    pub fn labels(&self) -> Vec<String> {
        self.label_index.iter().map(|e| e.key().clone()).collect()
    }

    /// Get all relationship types.
    #[must_use]
    pub fn relationship_types(&self) -> Vec<String> {
        let mut types = HashSet::new();
        for edge in self.edges.iter() {
            types.insert(edge.rel_type.clone());
        }
        types.into_iter().collect()
    }
}

impl Default for Graph {
    fn default() -> Self {
        Self::new(String::new())
    }
}

/// Edge direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Outgoing edges
    Outgoing,
    /// Incoming edges
    Incoming,
    /// Both directions
    Both,
}

/// Node pattern for matching.
#[derive(Debug, Clone)]
pub struct NodePattern {
    /// Variable alias
    pub alias: String,
    /// Label filter
    pub label: Option<String>,
    /// Property filters
    pub properties: HashMap<String, PropertyValue>,
}

/// Match pattern.
#[derive(Debug, Clone)]
pub struct MatchPattern {
    /// Node pattern
    pub node_pattern: NodePattern,
    // TODO: Add relationship patterns for more complex queries
}

/// Match result value.
#[derive(Debug, Clone)]
pub enum MatchValue {
    /// A matched node
    Node(Node),
    /// A matched edge
    Edge(Edge),
    /// A scalar value
    Scalar(PropertyValue),
}

/// Match result.
#[derive(Debug, Clone)]
pub struct MatchResult {
    /// Variable bindings
    pub bindings: Vec<(String, MatchValue)>,
}

/// Global graph manager.
#[derive(Debug, Default)]
pub struct GraphManager {
    /// All graphs
    graphs: DashMap<String, Arc<Graph>>,
}

impl GraphManager {
    /// Create a new graph manager.
    #[must_use]
    pub fn new() -> Self {
        Self {
            graphs: DashMap::new(),
        }
    }

    /// Create a new graph.
    pub fn create_graph(&self, name: &str) -> Arc<Graph> {
        let graph = Arc::new(Graph::new(name.to_string()));
        self.graphs.insert(name.to_string(), graph.clone());
        graph
    }

    /// Get a graph.
    pub fn get_graph(&self, name: &str) -> Option<Arc<Graph>> {
        self.graphs.get(name).map(|g| g.clone())
    }

    /// Get or create a graph.
    pub fn get_or_create(&self, name: &str) -> Arc<Graph> {
        self.graphs
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Graph::new(name.to_string())))
            .clone()
    }

    /// Delete a graph.
    pub fn delete_graph(&self, name: &str) -> bool {
        self.graphs.remove(name).is_some()
    }

    /// List all graphs.
    pub fn list_graphs(&self) -> Vec<String> {
        self.graphs.iter().map(|e| e.key().clone()).collect()
    }
}

/// Graph errors.
#[derive(Debug, Clone, thiserror::Error)]
pub enum GraphError {
    /// Graph not found
    #[error("graph not found: {0}")]
    GraphNotFound(String),
    /// Node not found
    #[error("node not found: {0}")]
    NodeNotFound(u64),
    /// Edge not found
    #[error("edge not found: {0}")]
    EdgeNotFound(u64),
    /// Query error
    #[error("query error: {0}")]
    QueryError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_node() {
        let graph = Graph::new("test".to_string());

        let mut props = HashMap::new();
        props.insert(
            "name".to_string(),
            PropertyValue::String("Alice".to_string()),
        );

        let id = graph.create_node(vec!["Person".to_string()], props);

        let node = graph.get_node(id).unwrap();
        assert_eq!(node.labels, vec!["Person"]);
        assert_eq!(
            node.properties.get("name"),
            Some(&PropertyValue::String("Alice".to_string()))
        );
    }

    #[test]
    fn test_create_edge() {
        let graph = Graph::new("test".to_string());

        let alice = graph.create_node(vec!["Person".to_string()], HashMap::new());
        let bob = graph.create_node(vec!["Person".to_string()], HashMap::new());

        let edge_id = graph
            .create_edge(alice, bob, "KNOWS".to_string(), HashMap::new())
            .unwrap();

        let edge = graph.get_edge(edge_id).unwrap();
        assert_eq!(edge.src, alice);
        assert_eq!(edge.dst, bob);
        assert_eq!(edge.rel_type, "KNOWS");
    }

    #[test]
    fn test_shortest_path() {
        let graph = Graph::new("test".to_string());

        // Create a chain: A -> B -> C -> D
        let a = graph.create_node(vec![], HashMap::new());
        let b = graph.create_node(vec![], HashMap::new());
        let c = graph.create_node(vec![], HashMap::new());
        let d = graph.create_node(vec![], HashMap::new());

        graph.create_edge(a, b, "NEXT".to_string(), HashMap::new());
        graph.create_edge(b, c, "NEXT".to_string(), HashMap::new());
        graph.create_edge(c, d, "NEXT".to_string(), HashMap::new());

        let path = graph.shortest_path(a, d).unwrap();
        assert_eq!(path, vec![a, b, c, d]);
    }

    #[test]
    fn test_get_nodes_by_label() {
        let graph = Graph::new("test".to_string());

        graph.create_node(vec!["Person".to_string()], HashMap::new());
        graph.create_node(vec!["Person".to_string()], HashMap::new());
        graph.create_node(vec!["Company".to_string()], HashMap::new());

        let persons = graph.get_nodes_by_label("Person");
        assert_eq!(persons.len(), 2);

        let companies = graph.get_nodes_by_label("Company");
        assert_eq!(companies.len(), 1);
    }

    #[test]
    fn test_delete_node() {
        let graph = Graph::new("test".to_string());

        let a = graph.create_node(vec!["Person".to_string()], HashMap::new());
        let b = graph.create_node(vec!["Person".to_string()], HashMap::new());
        graph.create_edge(a, b, "KNOWS".to_string(), HashMap::new());

        assert_eq!(graph.node_count(), 2);
        assert_eq!(graph.edge_count(), 1);

        graph.delete_node(a);

        assert_eq!(graph.node_count(), 1);
        assert_eq!(graph.edge_count(), 0);
    }
}
