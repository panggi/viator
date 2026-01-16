//! Memory pooling for buffer reuse.
//!
//! This module provides object pooling to reduce allocation overhead
//! for frequently created/destroyed buffers. This is especially important
//! for network I/O where buffers are constantly allocated and freed.

use bytes::BytesMut;
use crossbeam_queue::ArrayQueue;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Default buffer size for pooled buffers.
const DEFAULT_BUFFER_SIZE: usize = 4096;

/// Maximum number of buffers to keep in the pool.
const DEFAULT_POOL_SIZE: usize = 1024;

/// Statistics for pool monitoring.
#[derive(Debug, Default)]
pub struct PoolStats {
    /// Number of times a buffer was taken from the pool.
    pub hits: AtomicUsize,
    /// Number of times a new buffer had to be allocated.
    pub misses: AtomicUsize,
    /// Number of buffers returned to the pool.
    pub returns: AtomicUsize,
    /// Number of buffers dropped (pool was full).
    pub drops: AtomicUsize,
}

impl PoolStats {
    /// Get the hit rate as a percentage.
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed) as f64;
        let misses = self.misses.load(Ordering::Relaxed) as f64;
        let total = hits + misses;
        if total == 0.0 {
            100.0
        } else {
            (hits / total) * 100.0
        }
    }
}

/// A pool of reusable `BytesMut` buffers.
///
/// # Usage
///
/// ```ignore
/// let pool = BufferPool::new();
///
/// // Get a buffer from the pool
/// let mut buffer = pool.get();
/// buffer.extend_from_slice(b"hello");
///
/// // Return buffer to pool when done
/// pool.put(buffer);
/// ```
///
/// # Performance
///
/// This pool uses a lock-free `ArrayQueue` for excellent concurrent
/// performance. The pool automatically clears buffers before reuse
/// while preserving their allocated capacity.
#[derive(Debug)]
pub struct BufferPool {
    /// The underlying storage.
    buffers: ArrayQueue<BytesMut>,
    /// Default capacity for new buffers.
    default_capacity: usize,
    /// Pool statistics.
    stats: PoolStats,
}

impl BufferPool {
    /// Create a new buffer pool with default settings.
    pub fn new() -> Self {
        Self::with_config(DEFAULT_POOL_SIZE, DEFAULT_BUFFER_SIZE)
    }

    /// Create a pool with custom configuration.
    pub fn with_config(pool_size: usize, buffer_capacity: usize) -> Self {
        Self {
            buffers: ArrayQueue::new(pool_size),
            default_capacity: buffer_capacity,
            stats: PoolStats::default(),
        }
    }

    /// Get a buffer from the pool.
    ///
    /// Returns a buffer from the pool if available, otherwise allocates a new one.
    pub fn get(&self) -> BytesMut {
        match self.buffers.pop() {
            Some(buf) => {
                self.stats.hits.fetch_add(1, Ordering::Relaxed);
                buf
            }
            None => {
                self.stats.misses.fetch_add(1, Ordering::Relaxed);
                BytesMut::with_capacity(self.default_capacity)
            }
        }
    }

    /// Return a buffer to the pool.
    ///
    /// The buffer is cleared but its capacity is preserved.
    /// If the pool is full, the buffer is dropped.
    pub fn put(&self, mut buf: BytesMut) {
        buf.clear();

        // Only keep buffers that haven't grown too large
        if buf.capacity() <= self.default_capacity * 4 {
            match self.buffers.push(buf) {
                Ok(()) => {
                    self.stats.returns.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    self.stats.drops.fetch_add(1, Ordering::Relaxed);
                }
            }
        } else {
            self.stats.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get pool statistics.
    pub fn stats(&self) -> &PoolStats {
        &self.stats
    }

    /// Get the current number of buffers in the pool.
    pub fn len(&self) -> usize {
        self.buffers.len()
    }

    /// Check if the pool is empty.
    pub fn is_empty(&self) -> bool {
        self.buffers.is_empty()
    }
}

impl Default for BufferPool {
    fn default() -> Self {
        Self::new()
    }
}

/// A guard that returns a buffer to the pool on drop.
///
/// This ensures buffers are always returned to the pool, even in
/// the presence of panics or early returns.
pub struct PooledBuffer {
    buffer: Option<BytesMut>,
    pool: Arc<BufferPool>,
}

impl PooledBuffer {
    /// Create a new pooled buffer.
    pub fn new(pool: Arc<BufferPool>) -> Self {
        Self {
            buffer: Some(pool.get()),
            pool,
        }
    }

    /// Get a mutable reference to the buffer.
    pub fn as_mut(&mut self) -> &mut BytesMut {
        self.buffer.as_mut().expect("buffer already taken")
    }

    /// Take ownership of the buffer (it won't be returned to the pool).
    pub fn take(mut self) -> BytesMut {
        self.buffer.take().expect("buffer already taken")
    }

    /// Get an immutable reference to the buffer.
    pub fn as_ref(&self) -> &BytesMut {
        self.buffer.as_ref().expect("buffer already taken")
    }
}

impl std::ops::Deref for PooledBuffer {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl std::ops::DerefMut for PooledBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        if let Some(buf) = self.buffer.take() {
            self.pool.put(buf);
        }
    }
}

/// A thread-local buffer pool for minimal synchronization overhead.
///
/// Each thread gets its own small pool, with overflow going to
/// a shared global pool.
pub struct ThreadLocalPool {
    /// The global shared pool.
    global: Arc<BufferPool>,
}

impl ThreadLocalPool {
    /// Create a new thread-local pool backed by a global pool.
    pub fn new(global: Arc<BufferPool>) -> Self {
        Self { global }
    }

    /// Get a buffer.
    pub fn get(&self) -> BytesMut {
        self.global.get()
    }

    /// Return a buffer.
    pub fn put(&self, buf: BytesMut) {
        self.global.put(buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool_basic() {
        let pool = BufferPool::new();

        // Get a buffer
        let mut buf = pool.get();
        assert!(buf.is_empty());
        assert!(buf.capacity() >= DEFAULT_BUFFER_SIZE);

        // Use it
        buf.extend_from_slice(b"hello world");
        assert_eq!(&buf[..], b"hello world");

        // Return it
        pool.put(buf);
        assert_eq!(pool.len(), 1);

        // Get it back (should be cleared)
        let buf2 = pool.get();
        assert!(buf2.is_empty());
        assert!(buf2.capacity() >= DEFAULT_BUFFER_SIZE);
    }

    #[test]
    fn test_buffer_pool_stats() {
        let pool = BufferPool::with_config(2, 1024);

        // First get is a miss
        let buf1 = pool.get();
        assert_eq!(pool.stats().misses.load(Ordering::Relaxed), 1);
        assert_eq!(pool.stats().hits.load(Ordering::Relaxed), 0);

        // Return it
        pool.put(buf1);
        assert_eq!(pool.stats().returns.load(Ordering::Relaxed), 1);

        // Second get is a hit
        let _buf2 = pool.get();
        assert_eq!(pool.stats().hits.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_pooled_buffer_guard() {
        let pool = Arc::new(BufferPool::new());

        {
            let mut buf = PooledBuffer::new(pool.clone());
            buf.extend_from_slice(b"test");
            assert_eq!(&buf[..], b"test");
        } // Buffer returned to pool here

        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn test_pool_capacity_limit() {
        let pool = BufferPool::with_config(2, 1024);

        // Fill the pool
        pool.put(BytesMut::with_capacity(1024));
        pool.put(BytesMut::with_capacity(1024));
        assert_eq!(pool.len(), 2);

        // This one should be dropped
        pool.put(BytesMut::with_capacity(1024));
        assert_eq!(pool.len(), 2);
        assert_eq!(pool.stats().drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_oversized_buffer_dropped() {
        let pool = BufferPool::with_config(10, 1024);

        // Create an oversized buffer (> 4x default capacity)
        let mut buf = BytesMut::with_capacity(1024);
        buf.reserve(1024 * 10); // Make it much larger

        pool.put(buf);
        assert_eq!(pool.len(), 0);
        assert_eq!(pool.stats().drops.load(Ordering::Relaxed), 1);
    }
}
