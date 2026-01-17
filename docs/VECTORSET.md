# Viator VectorSet

> **Note:** Viator is Redis® 8.4.0 protocol-compatible. Redis is a registered trademark of Redis Ltd.

This document details Viator's VectorSet data type for similarity search.

## Overview

VectorSet provides high-dimensional vector similarity search using the HNSW (Hierarchical Navigable Small World) algorithm.

### Use Cases

- **Semantic Search**: Find similar documents, products, or content
- **Recommendation Systems**: "Users who liked X also liked Y"
- **Image Search**: Find visually similar images
- **Anomaly Detection**: Find outliers in high-dimensional data
- **RAG Applications**: Retrieve relevant context for LLMs

---

## Commands

### VADD - Add Vectors

```bash
VADD key [FP32|FP64] VALUES dim v1 v2 ... vN element [SETATTR attr1 val1 ...]
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `key` | VectorSet key name |
| `FP32/FP64` | Vector precision (default: FP32) |
| `VALUES` | Keyword for inline vector values |
| `dim` | Vector dimensionality |
| `v1..vN` | Vector components |
| `element` | Element identifier (string) |
| `SETATTR` | Optional attributes for filtering |

**Examples:**

```bash
# Add a 3-dimensional vector
VADD embeddings FP32 VALUES 3 0.1 0.2 0.3 "doc1"

# Add with attributes
VADD products FP32 VALUES 128 0.1 0.2 ... 0.9 "product123" \
  SETATTR category "electronics" price "299.99"

# Add 768-dim embedding (typical for sentence transformers)
VADD sentences FP32 VALUES 768 0.01 0.02 ... 0.99 "sentence_abc"
```

### VSIM - Similarity Search

```bash
VSIM key [FP32|FP64] VALUES dim v1 v2 ... vN [TOPK k] [EF ef] [FILTER expr]
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `TOPK` | Number of results (default: 10) |
| `EF` | Search quality parameter (higher = better, slower) |
| `FILTER` | Attribute filter expression |

**Examples:**

```bash
# Find 5 most similar vectors
VSIM embeddings FP32 VALUES 3 0.1 0.2 0.3 TOPK 5

# With filter
VSIM products FP32 VALUES 128 0.1 0.2 ... 0.9 TOPK 10 \
  FILTER "category == 'electronics' AND price < 500"
```

**Response:**

```
1) 1) "doc1"
   2) "0.95"      # similarity score
2) 1) "doc2"
   2) "0.87"
3) 1) "doc3"
   2) "0.82"
```

### VGET - Get Vector

```bash
VGET key element
```

Returns the vector values for an element.

### VDEL - Delete Element

```bash
VDEL key element
```

Removes an element from the VectorSet.

### VCARD - Count Elements

```bash
VCARD key
```

Returns the number of elements in the VectorSet.

### VINFO - Index Information

```bash
VINFO key
```

Returns index metadata:

```
1) "dimension"
2) (integer) 128
3) "metric"
4) "cosine"
5) "count"
6) (integer) 10000
7) "memory_bytes"
8) (integer) 5242880
```

---

## Distance Metrics

| Metric | Description | Use Case |
|--------|-------------|----------|
| `COSINE` | Cosine similarity (default) | Text embeddings, normalized vectors |
| `L2` | Euclidean distance | Spatial data, image features |
| `IP` | Inner product | Maximum inner product search |

### Metric Selection

```bash
# Create with specific metric (at first VADD)
VADD myindex METRIC COSINE FP32 VALUES 3 0.1 0.2 0.3 "elem1"
```

---

## HNSW Algorithm

VectorSet uses Hierarchical Navigable Small World graphs for approximate nearest neighbor search.

### How HNSW Works

```
Layer 3:  [A] ─────────────────── [B]
           │                       │
Layer 2:  [A] ───── [C] ───── [D] ─ [B]
           │         │         │    │
Layer 1:  [A] ─ [E] ─ [C] ─ [F] ─ [D] ─ [B] ─ [G]
           │    │     │     │     │     │     │
Layer 0:  [A]─[E]─[H]─[C]─[I]─[F]─[J]─[D]─[K]─[B]─[L]─[G]
```

1. **Hierarchical Layers**: Sparse top layers for fast navigation
2. **Greedy Search**: Navigate to approximate region quickly
3. **Local Refinement**: Exhaustive search in bottom layer

### Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `M` | Max connections per node | 16 |
| `ef_construction` | Build-time search quality | 100 |
| `ef_search` | Query-time search quality | 50 |

**Trade-offs:**

| Setting | Build Time | Query Time | Recall | Memory |
|---------|------------|------------|--------|--------|
| Low M/ef | Fast | Fast | Lower | Less |
| High M/ef | Slow | Slower | Higher | More |

---

## Persistence

VectorSets are fully persisted in both VDB and AOF.

### VDB Format

```
+--------+--------+------+---------+----------+------------+
| METRIC | DIM    | COUNT| ELEMENT | VECTOR   | ATTRIBUTES |
| (1 byte)| (u32) | (u32)| NAME    | (f32*dim)| (key-value)|
+--------+--------+------+---------+----------+------------+
```

**VDB Type:** 20 (VDB_TYPE_VECTORSET)

### AOF Format

VectorSets are reconstructed using VADD commands:

```
*8
$4
VADD
$10
myindex
$4
FP32
$6
VALUES
$1
3
$3
0.1
$3
0.2
$3
0.3
$5
elem1
```

With attributes:

```
*12
$4
VADD
$10
myindex
$4
FP32
$6
VALUES
$1
3
...
$5
elem1
$7
SETATTR
$8
category
$11
electronics
```

---

## Memory Estimation

Approximate memory usage per element:

```
Base: 8 bytes (pointer)
Vector: dim × 4 bytes (FP32) or dim × 8 bytes (FP64)
HNSW: M × 8 bytes × layers (average ~1.5 layers)
Element name: length + 24 bytes overhead
Attributes: key + value lengths + overhead
```

**Example (128-dim, M=16):**

```
Vector:     128 × 4 = 512 bytes
HNSW:       16 × 8 × 1.5 = 192 bytes
Overhead:   ~100 bytes
Total:      ~800 bytes per element

1M elements ≈ 800 MB
```

---

## Performance

### Complexity

| Operation | Time Complexity |
|-----------|-----------------|
| VADD | O(log N × M × ef_construction) |
| VSIM | O(log N × M × ef_search) |
| VGET | O(1) |
| VDEL | O(M × log N) |
| VCARD | O(1) |

### Tuning

```bash
# Higher recall (slower)
VSIM key ... EF 200 TOPK 10

# Faster queries (lower recall)
VSIM key ... EF 20 TOPK 10
```

---

## Best Practices

### Normalize Vectors

For cosine similarity, pre-normalize vectors:

```python
import numpy as np

def normalize(v):
    norm = np.linalg.norm(v)
    return v / norm if norm > 0 else v
```

### Batch Inserts

Use pipelining for bulk inserts:

```python
pipe = redis.pipeline()
for item in items:
    pipe.execute_command('VADD', 'index', 'FP32', 'VALUES',
                         dim, *item.vector, item.id)
pipe.execute()
```

### Memory Management

- Use FP32 unless FP64 precision required
- Monitor memory with `VINFO` and `INFO memory`
- Consider sharding across multiple keys for very large datasets

### Attribute Filtering

For filtered search, add relevant attributes:

```bash
VADD products FP32 VALUES 128 ... "prod1" \
  SETATTR category "shoes" brand "nike" price "120"

# Then filter:
VSIM products FP32 VALUES 128 ... TOPK 10 \
  FILTER "category == 'shoes' AND price < 150"
```

---

## Integration Example

### Python with sentence-transformers

```python
from sentence_transformers import SentenceTransformer
import redis

# Load model
model = SentenceTransformer('all-MiniLM-L6-v2')

# Connect to Viator
r = redis.Redis(host='localhost', port=6379)

# Index documents
documents = [
    "The quick brown fox jumps over the lazy dog",
    "A fast auburn fox leaps above a sleepy canine",
    "Python is a programming language",
]

for i, doc in enumerate(documents):
    embedding = model.encode(doc)
    r.execute_command(
        'VADD', 'docs', 'FP32', 'VALUES', len(embedding),
        *embedding.tolist(), f'doc{i}'
    )

# Search
query = "quick fox"
query_vec = model.encode(query)
results = r.execute_command(
    'VSIM', 'docs', 'FP32', 'VALUES', len(query_vec),
    *query_vec.tolist(), 'TOPK', 5
)

for element, score in zip(results[::2], results[1::2]):
    print(f"{element}: {score}")
```

### Output

```
doc0: 0.92
doc1: 0.85
doc2: 0.12
```
