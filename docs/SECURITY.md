# Viator Security

> **Note:** Viator is Redis® 8.4.0 protocol-compatible. Redis is a registered trademark of Redis Ltd.

This document details Viator's security features and best practices.

## Overview

Viator implements defense-in-depth with multiple security layers:

```
┌─────────────────────────────────────────────────────────────┐
│                    Network Security                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │   Binding   │  │    TLS      │  │   Rate Limiting     │  │
│  │  127.0.0.1  │  │   1.3       │  │   Per-IP/Conn       │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                   Authentication                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │    AUTH     │  │  Argon2id   │  │  Brute-Force        │  │
│  │  Command    │  │   Hashing   │  │  Protection         │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    Authorization                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │    ACL      │  │   Command   │  │     Key Pattern     │  │
│  │   Users     │  │  Categories │  │     Matching        │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                   Execution Safety                           │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │    Lua      │  │   Memory    │  │   Command           │  │
│  │  Sandbox    │  │   Limits    │  │   Protection        │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## Memory Safety

Viator is written in safe Rust, eliminating entire classes of vulnerabilities:

| Vulnerability | Status | How |
|--------------|--------|-----|
| Buffer overflow | Eliminated | Bounds checking |
| Use-after-free | Eliminated | Ownership system |
| Double-free | Eliminated | Ownership system |
| Null pointer dereference | Eliminated | Option types |
| Data races | Eliminated | Borrow checker |
| Format string attacks | Eliminated | Type-safe formatting |

```rust
// Library root enforces safety
#![forbid(unsafe_code)]
```

---

## Authentication

### AUTH Command

```bash
# Simple password (legacy)
AUTH mypassword

# Username + password (Redis 6+)
AUTH username mypassword
```

### Password Hashing

Passwords are hashed using Argon2id (winner of Password Hashing Competition):

```rust
use argon2::{Argon2, PasswordHasher, PasswordVerifier};

pub fn hash_password(password: &str) -> Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    let hash = argon2.hash_password(password.as_bytes(), &salt)?;
    Ok(hash.to_string())
}

pub fn verify_password(password: &str, hash: &str) -> Result<bool> {
    let parsed = PasswordHash::new(hash)?;
    Ok(Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok())
}
```

**Why Argon2id:**
- Memory-hard (resistant to GPU/ASIC attacks)
- Data-dependent memory access (side-channel resistant)
- Configurable time/memory trade-offs

### Constant-Time Password Comparison

For simple `requirepass` authentication, Viator uses constant-time comparison to prevent timing attacks:

```rust
use subtle::ConstantTimeEq;

fn validate_password(provided: &str, expected: &str) -> bool {
    // Constant-time comparison prevents timing attacks
    // Attackers cannot infer password length/content from response times
    provided.as_bytes().ct_eq(expected.as_bytes()).into()
}
```

**Why constant-time:**
- Variable-time comparison (like `==`) returns faster on mismatch
- Attackers can measure response times to infer correct characters
- Constant-time comparison takes the same time regardless of match position

### Brute-Force Protection

Per-connection rate limiting prevents password guessing:

```rust
const MAX_AUTH_FAILURES: u32 = 10;
const AUTH_LOCKOUT_SECONDS: u64 = 60;

impl ClientState {
    /// Check if AUTH is currently blocked
    pub fn auth_is_blocked(&self) -> Option<u64> {
        let lockout_until = self.auth_lockout_until.load(Ordering::Relaxed);
        let now = current_timestamp_secs();
        if now < lockout_until {
            Some(lockout_until - now)  // Seconds remaining
        } else {
            None
        }
    }

    /// Record a failed AUTH attempt
    pub fn auth_record_failure(&self) -> bool {
        let failures = self.auth_failures.fetch_add(1, Ordering::Relaxed) + 1;
        if failures >= MAX_AUTH_FAILURES {
            let lockout_until = current_timestamp_secs() + AUTH_LOCKOUT_SECONDS;
            self.auth_lockout_until.store(lockout_until, Ordering::Relaxed);
            true  // Now locked out
        } else {
            false
        }
    }

    /// Reset on successful auth
    pub fn auth_reset_failures(&self) {
        self.auth_failures.store(0, Ordering::Relaxed);
        self.auth_lockout_until.store(0, Ordering::Relaxed);
    }
}
```

**Behavior:**
- After 10 failed attempts: 60-second lockout
- Error message includes remaining lockout time
- Lockout resets on successful authentication

---

## Access Control Lists (ACL)

### User Management

```bash
# Create user with permissions
ACL SETUSER alice on >secretpass ~cached:* +get +set +del

# List users
ACL LIST

# Get user details
ACL GETUSER alice

# Delete user
ACL DELUSER alice
```

### Permission Model

```rust
pub struct AclUser {
    /// Username
    name: String,

    /// Argon2id password hash
    password_hash: Option<String>,

    /// Account enabled
    enabled: bool,

    /// Command permissions
    commands: CommandPermissions,

    /// Key access patterns
    keys: KeyPatterns,

    /// Pub/Sub channel patterns
    channels: ChannelPatterns,
}

pub struct CommandPermissions {
    /// Explicitly allowed commands
    allowed: HashSet<String>,

    /// Explicitly denied commands
    denied: HashSet<String>,

    /// Allowed categories (@read, @write, @admin, etc.)
    categories: HashSet<String>,
}
```

### Command Categories

| Category | Description |
|----------|-------------|
| `@read` | Read-only commands |
| `@write` | Data modification |
| `@admin` | Administrative commands |
| `@dangerous` | Potentially harmful (KEYS, FLUSHALL) |
| `@slow` | O(N) or higher complexity |
| `@fast` | O(1) complexity |
| `@pubsub` | Pub/Sub commands |
| `@scripting` | Lua scripting |
| `@keyspace` | Key manipulation |

### Key Patterns

```bash
# Allow access to specific key patterns
ACL SETUSER alice ~user:* ~cache:*

# Allow read access to all, write to specific
ACL SETUSER alice %R~* %W~user:*

# Deny specific patterns
ACL SETUSER alice ~* resetkeys ~!admin:*
```

### Examples

```bash
# Read-only user
ACL SETUSER readonly on >pass ~* +@read -@dangerous

# Cache-only user
ACL SETUSER cache on >pass ~cache:* +get +set +del +expire

# Admin user
ACL SETUSER admin on >adminpass ~* +@all

# Pub/Sub only
ACL SETUSER pubsub on >pass &* +@pubsub
```

---

## TLS Encryption

### Configuration

```bash
viator \
  --tls-port 6380 \
  --tls-cert-file /path/to/cert.pem \
  --tls-key-file /path/to/key.pem \
  --tls-ca-cert-file /path/to/ca.pem
```

### Features

- TLS 1.3 via rustls (memory-safe TLS)
- Client certificate authentication (mTLS)
- Modern cipher suites only

```rust
pub struct TlsConfig {
    /// Server certificate chain
    cert_chain: Vec<Certificate>,

    /// Private key
    private_key: PrivateKey,

    /// CA for client verification (optional)
    client_ca: Option<RootCertStore>,

    /// Require client certificates
    require_client_auth: bool,
}
```

### Client Connection

```bash
# With TLS
viator-cli --tls -p 6380 \
  --cert /path/to/client.crt \
  --key /path/to/client.key \
  --cacert /path/to/ca.crt
```

---

## Lua Sandboxing

### Restricted Environment

Lua scripts run in a restricted sandbox:

```rust
const ALLOWED_GLOBALS: &[&str] = &[
    // Redis API
    "redis", "KEYS", "ARGV",

    // Safe standard library
    "string", "table", "math",
    "tonumber", "tostring", "type",
    "pairs", "ipairs", "next",
    "unpack", "select", "error",
    "pcall", "assert",

    // Constants
    "cjson", "cmsgpack",
];

// REMOVED (dangerous)
// os, io, debug, loadfile, dofile, require, package
```

### Resource Limits

```rust
pub struct LuaSandbox {
    /// Maximum memory usage (bytes)
    memory_limit: usize,

    /// Maximum instructions before timeout
    instruction_limit: u64,

    /// Execution timeout
    timeout: Duration,
}
```

### SHA1 Script Identification

Scripts are identified by real SHA1 hash (Redis-compatible):

```rust
fn sha1_hash(script: &str) -> String {
    use sha1::{Sha1, Digest};
    let mut hasher = Sha1::new();
    hasher.update(script.as_bytes());
    let result = hasher.finalize();
    result.iter().map(|b| format!("{:02x}", b)).collect()
}
```

---

## Command Protection

### Dangerous Commands

Some commands can impact availability:

| Command | Risk | Mitigation |
|---------|------|------------|
| `KEYS *` | Blocks on large DBs | Warning at 10K keys, limit at 100K |
| `FLUSHALL` | Data loss | ACL restriction |
| `DEBUG` | Server crash | ACL restriction |
| `CONFIG` | Security bypass | ACL restriction |
| `SHUTDOWN` | Availability | ACL restriction |

### KEYS Protection

```rust
const KEYS_WARN_THRESHOLD: usize = 10_000;
const KEYS_MAX_RETURN: usize = 100_000;

pub fn cmd_keys(...) -> Result<Frame> {
    let db_size = db.len();

    // Warn on large databases
    if db_size > KEYS_WARN_THRESHOLD {
        tracing::warn!(
            "KEYS command on large database ({} keys). Consider SCAN instead.",
            db_size
        );
    }

    // Collect with limit
    let keys: Vec<_> = db.keys()
        .filter(|k| pattern.matches(k))
        .take(KEYS_MAX_RETURN)
        .collect();

    Ok(Frame::Array(keys))
}
```

---

## Connection Limits

### Configuration

```rust
pub struct ConnectionLimits {
    /// Maximum concurrent connections
    max_clients: usize,

    /// Maximum input buffer size per client
    max_input_buffer: usize,

    /// Maximum output buffer size per client
    max_output_buffer: usize,

    /// Connection timeout (idle)
    timeout: Duration,
}
```

### Rate Limiting

Per-IP rate limiting prevents DoS:

```rust
pub struct RateLimiter {
    /// Requests per second per IP
    requests_per_second: u32,

    /// Burst allowance
    burst_size: u32,

    /// Current state per IP
    state: DashMap<IpAddr, TokenBucket>,
}
```

---

## Audit Logging

### Configuration

```bash
viator --audit-log /var/log/viator/audit.log
```

### Events Logged

| Event | Data |
|-------|------|
| AUTH success/failure | Username, IP, timestamp |
| ACL changes | User, old/new permissions |
| CONFIG changes | Setting, old/new value |
| Admin commands | Command, user, timestamp |
| Connection events | IP, connect/disconnect |

### Log Format

```json
{
  "timestamp": "2026-01-15T10:30:00Z",
  "event": "auth_failure",
  "client_ip": "192.168.1.100",
  "username": "unknown",
  "attempt": 3
}
```

---

## Best Practices

### Production Checklist

- [ ] Bind to specific interfaces (not 0.0.0.0)
- [ ] Enable TLS for all connections
- [ ] Use strong passwords (16+ characters)
- [ ] Create least-privilege ACL users
- [ ] Disable dangerous commands for non-admin users
- [ ] Enable audit logging
- [ ] Set connection limits
- [ ] Use firewall rules
- [ ] Regular security updates

### Secure Configuration

```bash
viator \
  --bind 10.0.0.1 \
  --port 6379 \
  --tls-port 6380 \
  --tls-cert-file /etc/viator/tls/server.crt \
  --tls-key-file /etc/viator/tls/server.key \
  --requirepass "$(cat /etc/viator/secrets/password)" \
  --maxclients 1000 \
  --timeout 300 \
  --audit-log /var/log/viator/audit.log
```

### ACL Best Practices

```bash
# Disable default user
ACL SETUSER default off

# Create admin with strong password
ACL SETUSER admin on >"$(openssl rand -base64 32)" ~* +@all

# Create application users with minimal permissions
ACL SETUSER app on >apppass ~app:* +@read +@write -@dangerous

# Create monitoring user
ACL SETUSER monitor on >monpass ~* +info +ping +client +slowlog
```
