//! Rate limiting for connection and command throttling.
//!
//! Provides protection against DoS attacks by limiting:
//! - Connection rate per IP
//! - Command rate per connection
//! - Memory usage per connection

use dashmap::DashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Configuration for rate limiting.
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Maximum new connections per second per IP.
    pub max_connections_per_ip_per_sec: u32,
    /// Maximum commands per second per connection.
    pub max_commands_per_connection_per_sec: u32,
    /// Burst allowance (tokens above the rate limit).
    pub burst_allowance: u32,
    /// Time window for rate calculation.
    pub window: Duration,
    /// Whether rate limiting is enabled.
    pub enabled: bool,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_connections_per_ip_per_sec: 100,
            max_commands_per_connection_per_sec: 10_000,
            burst_allowance: 50,
            window: Duration::from_secs(1),
            enabled: true,
        }
    }
}

/// Token bucket rate limiter.
///
/// Uses the token bucket algorithm which allows for bursting while
/// maintaining a long-term average rate limit.
#[derive(Debug)]
pub struct TokenBucket {
    /// Current number of tokens.
    tokens: AtomicU64,
    /// Maximum tokens (bucket capacity).
    max_tokens: u64,
    /// Tokens added per second.
    refill_rate: u64,
    /// Last refill time (as nanos since some epoch).
    last_refill: AtomicU64,
}

impl TokenBucket {
    /// Create a new token bucket.
    pub fn new(max_tokens: u64, refill_rate: u64) -> Self {
        Self {
            tokens: AtomicU64::new(max_tokens),
            max_tokens,
            refill_rate,
            last_refill: AtomicU64::new(Self::now_nanos()),
        }
    }

    /// Try to consume one token.
    ///
    /// Returns `true` if a token was consumed, `false` if rate limited.
    pub fn try_consume(&self) -> bool {
        self.try_consume_n(1)
    }

    /// Try to consume n tokens.
    pub fn try_consume_n(&self, n: u64) -> bool {
        self.refill();

        loop {
            let current = self.tokens.load(Ordering::Acquire);
            if current < n {
                return false;
            }

            if self
                .tokens
                .compare_exchange_weak(current, current - n, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Refill tokens based on elapsed time.
    fn refill(&self) {
        let now = Self::now_nanos();
        let last = self.last_refill.load(Ordering::Acquire);
        let elapsed_nanos = now.saturating_sub(last);

        // Calculate tokens to add (tokens_per_sec * elapsed_secs)
        let tokens_to_add = (self.refill_rate * elapsed_nanos) / 1_000_000_000;

        if tokens_to_add > 0 {
            // Try to update last_refill
            if self
                .last_refill
                .compare_exchange_weak(last, now, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                // Add tokens, capped at max
                loop {
                    let current = self.tokens.load(Ordering::Acquire);
                    let new_tokens = (current + tokens_to_add).min(self.max_tokens);

                    if self
                        .tokens
                        .compare_exchange_weak(
                            current,
                            new_tokens,
                            Ordering::Release,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        break;
                    }
                }
            }
        }
    }

    /// Get current token count.
    pub fn tokens(&self) -> u64 {
        self.tokens.load(Ordering::Relaxed)
    }

    fn now_nanos() -> u64 {
        // Use a monotonic clock
        static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
        let start = START.get_or_init(Instant::now);
        start.elapsed().as_nanos() as u64
    }
}

/// Rate limiter for IP-based connection limiting.
#[derive(Debug)]
pub struct ConnectionRateLimiter {
    /// Per-IP rate limiters.
    limiters: DashMap<IpAddr, TokenBucket>,
    /// Configuration.
    config: RateLimitConfig,
}

impl ConnectionRateLimiter {
    /// Create a new connection rate limiter.
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            limiters: DashMap::new(),
            config,
        }
    }

    /// Check if a new connection from this IP is allowed.
    pub fn allow_connection(&self, ip: IpAddr) -> bool {
        if !self.config.enabled {
            return true;
        }

        let max_tokens =
            u64::from(self.config.max_connections_per_ip_per_sec + self.config.burst_allowance);
        let refill_rate = u64::from(self.config.max_connections_per_ip_per_sec);

        let bucket = self
            .limiters
            .entry(ip)
            .or_insert_with(|| TokenBucket::new(max_tokens, refill_rate));

        bucket.try_consume()
    }

    /// Remove stale entries (IPs that haven't connected recently).
    pub fn cleanup(&self) {
        // Remove entries that are at full capacity (inactive)
        self.limiters.retain(|_, bucket| {
            bucket.tokens()
                < u64::from(
                    self.config.max_connections_per_ip_per_sec + self.config.burst_allowance,
                )
        });
    }

    /// Get the number of tracked IPs.
    pub fn tracked_ips(&self) -> usize {
        self.limiters.len()
    }
}

impl Default for ConnectionRateLimiter {
    fn default() -> Self {
        Self::new(RateLimitConfig::default())
    }
}

/// Per-connection command rate limiter.
#[derive(Debug)]
pub struct CommandRateLimiter {
    /// The token bucket for this connection.
    bucket: TokenBucket,
}

impl CommandRateLimiter {
    /// Create a new command rate limiter with the given config.
    pub fn new(config: &RateLimitConfig) -> Self {
        let max_tokens =
            u64::from(config.max_commands_per_connection_per_sec + config.burst_allowance);
        let refill_rate = u64::from(config.max_commands_per_connection_per_sec);

        Self {
            bucket: TokenBucket::new(max_tokens, refill_rate),
        }
    }

    /// Check if a command is allowed.
    pub fn allow_command(&self) -> bool {
        self.bucket.try_consume()
    }

    /// Get remaining command allowance.
    pub fn remaining(&self) -> u64 {
        self.bucket.tokens()
    }
}

/// Statistics for rate limiting.
#[derive(Debug, Default)]
pub struct RateLimitStats {
    /// Total connections allowed.
    pub connections_allowed: AtomicU64,
    /// Total connections rejected.
    pub connections_rejected: AtomicU64,
    /// Total commands allowed.
    pub commands_allowed: AtomicU64,
    /// Total commands rejected.
    pub commands_rejected: AtomicU64,
}

impl RateLimitStats {
    /// Record an allowed connection.
    pub fn record_connection_allowed(&self) {
        self.connections_allowed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a rejected connection.
    pub fn record_connection_rejected(&self) {
        self.connections_rejected.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an allowed command.
    pub fn record_command_allowed(&self) {
        self.commands_allowed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a rejected command.
    pub fn record_command_rejected(&self) {
        self.commands_rejected.fetch_add(1, Ordering::Relaxed);
    }

    /// Get connection rejection rate.
    pub fn connection_rejection_rate(&self) -> f64 {
        let allowed = self.connections_allowed.load(Ordering::Relaxed) as f64;
        let rejected = self.connections_rejected.load(Ordering::Relaxed) as f64;
        let total = allowed + rejected;
        if total == 0.0 {
            0.0
        } else {
            (rejected / total) * 100.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn test_token_bucket_basic() {
        let bucket = TokenBucket::new(10, 10);

        // Should be able to consume up to max
        for _ in 0..10 {
            assert!(bucket.try_consume());
        }

        // Should be rate limited now
        assert!(!bucket.try_consume());
    }

    #[test]
    fn test_token_bucket_burst() {
        let bucket = TokenBucket::new(100, 10);

        // Consume 50 (within burst)
        for _ in 0..50 {
            assert!(bucket.try_consume());
        }

        assert_eq!(bucket.tokens(), 50);
    }

    #[test]
    fn test_connection_rate_limiter() {
        let config = RateLimitConfig {
            max_connections_per_ip_per_sec: 5,
            burst_allowance: 2,
            ..Default::default()
        };

        let limiter = ConnectionRateLimiter::new(config);
        let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

        // Should allow 7 connections (5 + 2 burst)
        for _ in 0..7 {
            assert!(limiter.allow_connection(ip));
        }

        // 8th should be rejected
        assert!(!limiter.allow_connection(ip));
    }

    #[test]
    fn test_different_ips_independent() {
        let config = RateLimitConfig {
            max_connections_per_ip_per_sec: 2,
            burst_allowance: 0,
            ..Default::default()
        };

        let limiter = ConnectionRateLimiter::new(config);
        let ip1 = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        let ip2 = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));

        // Each IP gets its own limit
        assert!(limiter.allow_connection(ip1));
        assert!(limiter.allow_connection(ip1));
        assert!(!limiter.allow_connection(ip1));

        assert!(limiter.allow_connection(ip2));
        assert!(limiter.allow_connection(ip2));
        assert!(!limiter.allow_connection(ip2));
    }

    #[test]
    fn test_command_rate_limiter() {
        let config = RateLimitConfig {
            max_commands_per_connection_per_sec: 100,
            burst_allowance: 10,
            ..Default::default()
        };

        let limiter = CommandRateLimiter::new(&config);

        // Should allow 110 commands (100 + 10 burst)
        for _ in 0..110 {
            assert!(limiter.allow_command());
        }

        assert!(!limiter.allow_command());
    }

    #[test]
    fn test_rate_limit_disabled() {
        let config = RateLimitConfig {
            enabled: false,
            max_connections_per_ip_per_sec: 1,
            burst_allowance: 0,
            ..Default::default()
        };

        let limiter = ConnectionRateLimiter::new(config);
        let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

        // Should allow unlimited when disabled
        for _ in 0..1000 {
            assert!(limiter.allow_connection(ip));
        }
    }
}
