//! Vector Set data structure for similarity search (Redis 8.0+).
//!
//! Vector Sets provide native vector similarity search capabilities,
//! supporting operations like VADD, VSIM, VREM, etc.

use bytes::Bytes;
use std::collections::HashMap;

/// Distance metric for vector similarity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DistanceMetric {
    /// Cosine similarity (default)
    #[default]
    Cosine,
    /// Euclidean distance (L2)
    Euclidean,
    /// Inner product
    InnerProduct,
}

impl DistanceMetric {
    /// Parse from string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "COSINE" | "COS" => Some(Self::Cosine),
            "EUCLIDEAN" | "L2" => Some(Self::Euclidean),
            "IP" | "INNERPRODUCT" | "DOT" => Some(Self::InnerProduct),
            _ => None,
        }
    }

    /// Get string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Cosine => "cosine",
            Self::Euclidean => "euclidean",
            Self::InnerProduct => "ip",
        }
    }
}

/// A vector element in the vector set.
#[derive(Debug, Clone)]
pub struct VectorElement {
    /// The element name/key
    pub name: Bytes,
    /// The vector embedding
    pub vector: Vec<f32>,
    /// Optional attributes (key-value pairs)
    pub attributes: HashMap<Bytes, Bytes>,
}

impl VectorElement {
    /// Create a new vector element.
    pub fn new(name: Bytes, vector: Vec<f32>) -> Self {
        Self {
            name,
            vector,
            attributes: HashMap::new(),
        }
    }

    /// Get the dimensionality of this element's vector.
    pub fn dim(&self) -> usize {
        self.vector.len()
    }

    /// Calculate the L2 norm of the vector.
    pub fn norm(&self) -> f32 {
        self.vector.iter().map(|x| x * x).sum::<f32>().sqrt()
    }

    /// Normalize the vector in place.
    pub fn normalize(&mut self) {
        let norm = self.norm();
        if norm > 0.0 {
            for x in &mut self.vector {
                *x /= norm;
            }
        }
    }
}

/// Vector Set data structure.
#[derive(Debug, Clone, Default)]
pub struct VectorSet {
    /// Elements stored by name
    elements: HashMap<Bytes, VectorElement>,
    /// Vector dimensionality (fixed after first add)
    dim: Option<usize>,
    /// Distance metric
    metric: DistanceMetric,
    /// Quantization type (for info)
    quantization: String,
}

impl VectorSet {
    /// Create a new empty vector set.
    pub fn new() -> Self {
        Self {
            elements: HashMap::new(),
            dim: None,
            metric: DistanceMetric::Cosine,
            quantization: "NOQUANT".to_string(),
        }
    }

    /// Create a new vector set with specific parameters.
    pub fn with_params(dim: usize, metric: DistanceMetric) -> Self {
        Self {
            elements: HashMap::new(),
            dim: Some(dim),
            metric,
            quantization: "NOQUANT".to_string(),
        }
    }

    /// Set the distance metric.
    pub fn set_metric(&mut self, metric: DistanceMetric) {
        self.metric = metric;
    }

    /// Get the distance metric.
    pub fn metric(&self) -> DistanceMetric {
        self.metric
    }

    /// Add an element to the vector set.
    /// Returns true if the element was added (new), false if updated.
    pub fn add(&mut self, name: Bytes, vector: Vec<f32>) -> Result<bool, &'static str> {
        // Check dimensionality
        if let Some(dim) = self.dim {
            if vector.len() != dim {
                return Err("vector dimension mismatch");
            }
        } else {
            self.dim = Some(vector.len());
        }

        let is_new = !self.elements.contains_key(&name);
        let mut element = VectorElement::new(name.clone(), vector);

        // Normalize for cosine similarity
        if self.metric == DistanceMetric::Cosine {
            element.normalize();
        }

        self.elements.insert(name, element);
        Ok(is_new)
    }

    /// Add an element with attributes.
    pub fn add_with_attrs(
        &mut self,
        name: Bytes,
        vector: Vec<f32>,
        attrs: HashMap<Bytes, Bytes>,
    ) -> Result<bool, &'static str> {
        let result = self.add(name.clone(), vector)?;
        if let Some(elem) = self.elements.get_mut(&name) {
            elem.attributes = attrs;
        }
        Ok(result)
    }

    /// Remove an element from the vector set.
    pub fn remove(&mut self, name: &Bytes) -> bool {
        self.elements.remove(name).is_some()
    }

    /// Get the cardinality (number of elements).
    pub fn card(&self) -> usize {
        self.elements.len()
    }

    /// Get the dimensionality.
    pub fn dim(&self) -> Option<usize> {
        self.dim
    }

    /// Get an element's vector.
    pub fn get_embedding(&self, name: &Bytes) -> Option<&[f32]> {
        self.elements.get(name).map(|e| e.vector.as_slice())
    }

    /// Get an element.
    pub fn get(&self, name: &Bytes) -> Option<&VectorElement> {
        self.elements.get(name)
    }

    /// Get a mutable reference to an element.
    pub fn get_mut(&mut self, name: &Bytes) -> Option<&mut VectorElement> {
        self.elements.get_mut(name)
    }

    /// Check if an element exists.
    pub fn contains(&self, name: &Bytes) -> bool {
        self.elements.contains_key(name)
    }

    /// Get an attribute from an element.
    pub fn get_attr(&self, name: &Bytes, attr: &Bytes) -> Option<&Bytes> {
        self.elements.get(name)?.attributes.get(attr)
    }

    /// Set an attribute on an element.
    pub fn set_attr(&mut self, name: &Bytes, attr: Bytes, value: Bytes) -> bool {
        if let Some(elem) = self.elements.get_mut(name) {
            elem.attributes.insert(attr, value);
            true
        } else {
            false
        }
    }

    /// Get all attributes for an element as a HashMap.
    /// Returns an empty HashMap if the element doesn't exist.
    pub fn get_all_attrs(&self, name: &Bytes) -> HashMap<String, String> {
        self.elements
            .get(name)
            .map(|elem| {
                elem.attributes
                    .iter()
                    .filter_map(|(k, v)| {
                        let key = std::str::from_utf8(k).ok()?.to_string();
                        let val = std::str::from_utf8(v).ok()?.to_string();
                        Some((key, val))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Set all attributes for an element from a HashMap.
    /// Returns true if the element exists, false otherwise.
    pub fn set_all_attrs(&mut self, name: &Bytes, attrs: HashMap<Bytes, Bytes>) -> bool {
        if let Some(elem) = self.elements.get_mut(name) {
            for (k, v) in attrs {
                elem.attributes.insert(k, v);
            }
            true
        } else {
            false
        }
    }

    /// Calculate distance between two vectors.
    pub fn distance(&self, v1: &[f32], v2: &[f32]) -> f32 {
        match self.metric {
            DistanceMetric::Cosine => {
                // For normalized vectors, cosine distance = 1 - dot product
                let dot: f32 = v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum();
                1.0 - dot
            }
            DistanceMetric::Euclidean => {
                v1.iter()
                    .zip(v2.iter())
                    .map(|(a, b)| (a - b).powi(2))
                    .sum::<f32>()
                    .sqrt()
            }
            DistanceMetric::InnerProduct => {
                // For inner product, higher is better, so negate for distance
                -v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum::<f32>()
            }
        }
    }

    /// Calculate similarity (1 - distance for cosine/euclidean, raw for IP).
    pub fn similarity(&self, v1: &[f32], v2: &[f32]) -> f32 {
        match self.metric {
            DistanceMetric::Cosine => {
                v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum()
            }
            DistanceMetric::Euclidean => {
                let dist: f32 = v1
                    .iter()
                    .zip(v2.iter())
                    .map(|(a, b)| (a - b).powi(2))
                    .sum::<f32>()
                    .sqrt();
                1.0 / (1.0 + dist)
            }
            DistanceMetric::InnerProduct => {
                v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum()
            }
        }
    }

    /// Find similar elements to a query vector.
    pub fn search(&self, query: &[f32], count: usize) -> Vec<(Bytes, f32)> {
        if self.dim.map_or(false, |d| d != query.len()) {
            return vec![];
        }

        // Normalize query for cosine similarity
        let query_normalized: Vec<f32> = if self.metric == DistanceMetric::Cosine {
            let norm: f32 = query.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                query.iter().map(|x| x / norm).collect()
            } else {
                query.to_vec()
            }
        } else {
            query.to_vec()
        };

        let mut results: Vec<_> = self
            .elements
            .iter()
            .map(|(name, elem)| {
                let sim = self.similarity(&query_normalized, &elem.vector);
                (name.clone(), sim)
            })
            .collect();

        // Sort by similarity (descending)
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(count);
        results
    }

    /// Find similar elements to another element by name.
    pub fn search_by_element(&self, element_name: &Bytes, count: usize) -> Vec<(Bytes, f32)> {
        let query = match self.get_embedding(element_name) {
            Some(v) => v.to_vec(),
            None => return vec![],
        };

        let mut results: Vec<_> = self
            .elements
            .iter()
            .filter(|(name, _)| *name != element_name)
            .map(|(name, elem)| {
                let sim = self.similarity(&query, &elem.vector);
                (name.clone(), sim)
            })
            .collect();

        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(count);
        results
    }

    /// Get random members from the vector set.
    pub fn random_members(&self, count: usize, allow_duplicates: bool) -> Vec<Bytes> {
        use rand::seq::SliceRandom;

        let names: Vec<_> = self.elements.keys().cloned().collect();
        if names.is_empty() {
            return vec![];
        }

        let mut rng = rand::thread_rng();
        if allow_duplicates {
            (0..count)
                .map(|_| names.choose(&mut rng).unwrap().clone())
                .collect()
        } else {
            let mut shuffled = names;
            shuffled.shuffle(&mut rng);
            shuffled.into_iter().take(count).collect()
        }
    }

    /// Get all element names.
    pub fn members(&self) -> Vec<&Bytes> {
        self.elements.keys().collect()
    }

    /// Get info about the vector set.
    pub fn info(&self) -> VectorSetInfo {
        VectorSetInfo {
            num_elements: self.elements.len(),
            dim: self.dim.unwrap_or(0),
            metric: self.metric,
            quantization: self.quantization.clone(),
        }
    }

    /// Estimate memory usage.
    pub fn memory_usage(&self) -> usize {
        let base = size_of::<Self>();
        let elements: usize = self
            .elements
            .iter()
            .map(|(k, v)| {
                k.len()
                    + v.name.len()
                    + v.vector.len() * size_of::<f32>()
                    + v.attributes
                        .iter()
                        .map(|(ak, av)| ak.len() + av.len())
                        .sum::<usize>()
            })
            .sum();
        base + elements
    }

    /// Check if the vector set is empty.
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Get the number of elements.
    pub fn len(&self) -> usize {
        self.elements.len()
    }
}

/// Info about a vector set.
#[derive(Debug, Clone)]
pub struct VectorSetInfo {
    pub num_elements: usize,
    pub dim: usize,
    pub metric: DistanceMetric,
    pub quantization: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_set_basic() {
        let mut vs = VectorSet::new();

        // Add vectors
        assert!(vs.add(Bytes::from("a"), vec![1.0, 0.0, 0.0]).unwrap());
        assert!(vs.add(Bytes::from("b"), vec![0.0, 1.0, 0.0]).unwrap());
        assert!(vs.add(Bytes::from("c"), vec![1.0, 1.0, 0.0]).unwrap());

        assert_eq!(vs.card(), 3);
        assert_eq!(vs.dim(), Some(3));
    }

    #[test]
    fn test_similarity_search() {
        let mut vs = VectorSet::new();

        vs.add(Bytes::from("a"), vec![1.0, 0.0]).unwrap();
        vs.add(Bytes::from("b"), vec![0.0, 1.0]).unwrap();
        vs.add(Bytes::from("c"), vec![0.707, 0.707]).unwrap();

        let results = vs.search(&[1.0, 0.0], 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, Bytes::from("a"));
    }

    #[test]
    fn test_dimension_mismatch() {
        let mut vs = VectorSet::new();

        vs.add(Bytes::from("a"), vec![1.0, 0.0, 0.0]).unwrap();
        assert!(vs.add(Bytes::from("b"), vec![1.0, 0.0]).is_err());
    }

    #[test]
    fn test_attributes() {
        let mut vs = VectorSet::new();

        vs.add(Bytes::from("a"), vec![1.0, 0.0]).unwrap();
        vs.set_attr(&Bytes::from("a"), Bytes::from("color"), Bytes::from("red"));

        assert_eq!(
            vs.get_attr(&Bytes::from("a"), &Bytes::from("color")),
            Some(&Bytes::from("red"))
        );
    }
}
