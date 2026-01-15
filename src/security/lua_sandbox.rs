//! Lua sandbox for secure script execution.
//!
//! Provides:
//! - Removal of dangerous globals (os, io, loadfile, etc.)
//! - Memory limits per script
//! - Execution time limits (instruction count)
//! - Restricted standard library
//!
//! # Redis Compatibility
//!
//! Compatible with Redis Lua scripting security:
//! - Only whitelisted globals available
//! - redis.call/redis.pcall for server interaction
//! - Same execution model as Redis EVAL/EVALSHA
//!
//! # Security Model
//!
//! The sandbox follows the principle of least privilege:
//! 1. Start with empty environment
//! 2. Add only safe, necessary functions
//! 3. Wrap dangerous operations with checks
//! 4. Enforce resource limits at runtime

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Globals that are explicitly forbidden in the sandbox.
pub const FORBIDDEN_GLOBALS: &[&str] = &[
    // OS access
    "os",
    "io",
    // Code loading from files
    "loadfile",
    "dofile",
    // Raw bytecode (can bypass sandbox)
    "loadstring",
    "load",
    // Package system (can load C modules)
    "package",
    "require",
    // Debug library (can access internals)
    "debug",
    // Coroutine (can be used for resource abuse)
    "coroutine",
    // FFI (if using LuaJIT)
    "ffi",
    "jit",
    // Collectgarbage can cause DoS
    "collectgarbage",
    // newproxy can leak memory
    "newproxy",
    // These can be used to escape sandbox
    "rawget",
    "rawset",
    "rawequal",
    "rawlen",
    // Metatable manipulation
    "setmetatable",
    "getmetatable",
    // Global environment manipulation
    "setfenv",
    "getfenv",
    "_G",
];

/// Safe globals that are allowed in the sandbox.
pub const SAFE_GLOBALS: &[&str] = &[
    // Basic functions
    "assert",
    "error",
    "ipairs",
    "pairs",
    "next",
    "pcall",
    "xpcall",
    "select",
    "tonumber",
    "tostring",
    "type",
    "unpack",
    // Safe modules
    "string",
    "table",
    "math",
    // Constants
    "_VERSION",
];

/// Safe string library functions.
pub const SAFE_STRING_FUNCS: &[&str] = &[
    "byte", "char", "find", "format", "gmatch", "gsub", "len", "lower", "match", "rep", "reverse",
    "sub", "upper",
];

/// Safe table library functions.
pub const SAFE_TABLE_FUNCS: &[&str] =
    &["concat", "insert", "remove", "sort", "maxn", "pack", "unpack"];

/// Safe math library functions.
pub const SAFE_MATH_FUNCS: &[&str] = &[
    "abs", "acos", "asin", "atan", "atan2", "ceil", "cos", "cosh", "deg", "exp", "floor", "fmod",
    "frexp", "huge", "ldexp", "log", "log10", "max", "min", "modf", "pi", "pow", "rad", "random",
    "randomseed", "sin", "sinh", "sqrt", "tan", "tanh",
];

/// Configuration for the Lua sandbox.
#[derive(Debug, Clone)]
pub struct LuaSandboxConfig {
    /// Maximum memory per script (bytes)
    pub max_memory: usize,
    /// Maximum execution time
    pub max_execution_time: Duration,
    /// Maximum instruction count (0 = unlimited)
    pub max_instructions: u64,
    /// Allow random number generation
    pub allow_random: bool,
    /// Custom allowed globals (beyond SAFE_GLOBALS)
    pub custom_globals: HashSet<String>,
    /// Custom forbidden globals (beyond FORBIDDEN_GLOBALS)
    pub custom_forbidden: HashSet<String>,
    /// Enable debug mode (more permissive, for development only)
    pub debug_mode: bool,
}

impl Default for LuaSandboxConfig {
    fn default() -> Self {
        Self {
            max_memory: 256 * 1024 * 1024, // 256 MB
            max_execution_time: Duration::from_secs(5),
            max_instructions: 10_000_000, // 10M instructions
            allow_random: true,
            custom_globals: HashSet::new(),
            custom_forbidden: HashSet::new(),
            debug_mode: false,
        }
    }
}

/// Statistics for sandbox execution.
#[derive(Debug, Default)]
pub struct LuaSandboxStats {
    /// Total scripts executed
    pub scripts_executed: AtomicU64,
    /// Scripts terminated for exceeding time limit
    pub time_limit_exceeded: AtomicU64,
    /// Scripts terminated for exceeding memory limit
    pub memory_limit_exceeded: AtomicU64,
    /// Scripts terminated for exceeding instruction limit
    pub instruction_limit_exceeded: AtomicU64,
    /// Scripts that threw errors
    pub script_errors: AtomicU64,
}

/// Error from sandbox execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SandboxError {
    /// Script exceeded memory limit
    MemoryLimitExceeded {
        /// Bytes used
        used: usize,
        /// Limit
        limit: usize,
    },
    /// Script exceeded time limit
    TimeLimitExceeded {
        /// Time elapsed
        elapsed: Duration,
        /// Limit
        limit: Duration,
    },
    /// Script exceeded instruction limit
    InstructionLimitExceeded {
        /// Instructions executed
        executed: u64,
        /// Limit
        limit: u64,
    },
    /// Attempted to use forbidden function
    ForbiddenFunction {
        /// Name of the forbidden function
        name: String,
    },
    /// Script execution error
    ExecutionError {
        /// Error message
        message: String,
    },
    /// Invalid script
    InvalidScript {
        /// Error message
        message: String,
    },
}

impl std::fmt::Display for SandboxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MemoryLimitExceeded { used, limit } => {
                write!(f, "script memory limit exceeded: {used} > {limit} bytes")
            }
            Self::TimeLimitExceeded { elapsed, limit } => {
                write!(f, "script time limit exceeded: {elapsed:?} > {limit:?}")
            }
            Self::InstructionLimitExceeded { executed, limit } => {
                write!(
                    f,
                    "script instruction limit exceeded: {executed} > {limit}"
                )
            }
            Self::ForbiddenFunction { name } => {
                write!(f, "forbidden function: {name}")
            }
            Self::ExecutionError { message } => {
                write!(f, "script error: {message}")
            }
            Self::InvalidScript { message } => {
                write!(f, "invalid script: {message}")
            }
        }
    }
}

impl std::error::Error for SandboxError {}

/// Validates that a Lua script doesn't contain obvious sandbox escapes.
///
/// This is a static analysis pass that catches common bypass attempts.
/// It's not a replacement for runtime sandboxing but provides defense in depth.
#[derive(Debug)]
pub struct ScriptValidator {
    /// Patterns that indicate sandbox escape attempts
    forbidden_patterns: Vec<&'static str>,
}

impl Default for ScriptValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl ScriptValidator {
    /// Create a new script validator.
    #[must_use]
    pub fn new() -> Self {
        Self {
            forbidden_patterns: vec![
                // Direct global table access
                "_G[",
                "_G.",
                // Metatable manipulation
                "getmetatable",
                "setmetatable",
                // Raw access
                "rawget",
                "rawset",
                "rawequal",
                // Debug library
                "debug.",
                "debug[",
                // OS/IO access
                "os.",
                "os[",
                "io.",
                "io[",
                // Loading external code
                "loadfile",
                "dofile",
                "loadstring",
                // Package system
                "require",
                "package.",
                "package[",
                // FFI (LuaJIT)
                "ffi.",
                "ffi[",
                // JIT manipulation
                "jit.",
                "jit[",
                // Environment manipulation
                "setfenv",
                "getfenv",
            ],
        }
    }

    /// Validate a script for obvious sandbox escape attempts.
    ///
    /// Returns `Ok(())` if the script passes validation, or an error
    /// describing the violation.
    pub fn validate(&self, script: &str) -> Result<(), SandboxError> {
        let script_lower = script.to_lowercase();

        for pattern in &self.forbidden_patterns {
            if script_lower.contains(&pattern.to_lowercase()) {
                return Err(SandboxError::ForbiddenFunction {
                    name: (*pattern).to_string(),
                });
            }
        }

        // Check for string.dump (can create bytecode)
        if script_lower.contains("string.dump") {
            return Err(SandboxError::ForbiddenFunction {
                name: "string.dump".to_string(),
            });
        }

        // Check for load with mode that allows bytecode
        // This is a heuristic - not perfect but catches common cases
        if script_lower.contains("load(") || script_lower.contains("load (") {
            return Err(SandboxError::ForbiddenFunction {
                name: "load".to_string(),
            });
        }

        Ok(())
    }

    /// Add a custom forbidden pattern.
    pub fn add_forbidden_pattern(&mut self, pattern: &'static str) {
        self.forbidden_patterns.push(pattern);
    }
}

/// Builder for safe Lua environment setup.
///
/// This struct provides methods to generate Lua code that sets up
/// a safe execution environment.
#[derive(Debug, Default)]
pub struct SafeEnvironmentBuilder {
    /// Additional safe globals to include
    extra_safe_globals: HashSet<String>,
    /// Whether to include Redis-specific functions
    include_redis_funcs: bool,
}

impl SafeEnvironmentBuilder {
    /// Create a new environment builder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            extra_safe_globals: HashSet::new(),
            include_redis_funcs: true,
        }
    }

    /// Add an extra safe global.
    pub fn add_safe_global(&mut self, name: &str) -> &mut Self {
        self.extra_safe_globals.insert(name.to_string());
        self
    }

    /// Set whether to include Redis functions.
    pub fn include_redis_funcs(&mut self, include: bool) -> &mut Self {
        self.include_redis_funcs = include;
        self
    }

    /// Generate Lua code that creates a safe environment table.
    ///
    /// The generated code creates a table with only whitelisted functions.
    #[must_use]
    pub fn generate_environment_setup(&self) -> String {
        let mut code = String::from(
            r#"
-- Create safe environment
local safe_env = {}

-- Basic safe functions
safe_env.assert = assert
safe_env.error = error
safe_env.ipairs = ipairs
safe_env.pairs = pairs
safe_env.next = next
safe_env.pcall = pcall
safe_env.xpcall = xpcall
safe_env.select = select
safe_env.tonumber = tonumber
safe_env.tostring = tostring
safe_env.type = type
safe_env.unpack = unpack or table.unpack
safe_env._VERSION = _VERSION

-- Safe string library
safe_env.string = {
    byte = string.byte,
    char = string.char,
    find = string.find,
    format = string.format,
    gmatch = string.gmatch,
    gsub = string.gsub,
    len = string.len,
    lower = string.lower,
    match = string.match,
    rep = string.rep,
    reverse = string.reverse,
    sub = string.sub,
    upper = string.upper,
}

-- Safe table library
safe_env.table = {
    concat = table.concat,
    insert = table.insert,
    remove = table.remove,
    sort = table.sort,
    pack = table.pack,
    unpack = table.unpack,
}
if table.maxn then safe_env.table.maxn = table.maxn end

-- Safe math library
safe_env.math = {
    abs = math.abs,
    acos = math.acos,
    asin = math.asin,
    atan = math.atan,
    atan2 = math.atan2,
    ceil = math.ceil,
    cos = math.cos,
    deg = math.deg,
    exp = math.exp,
    floor = math.floor,
    fmod = math.fmod,
    huge = math.huge,
    log = math.log,
    max = math.max,
    min = math.min,
    modf = math.modf,
    pi = math.pi,
    pow = math.pow,
    rad = math.rad,
    random = math.random,
    randomseed = math.randomseed,
    sin = math.sin,
    sqrt = math.sqrt,
    tan = math.tan,
}
if math.cosh then safe_env.math.cosh = math.cosh end
if math.sinh then safe_env.math.sinh = math.sinh end
if math.tanh then safe_env.math.tanh = math.tanh end
if math.frexp then safe_env.math.frexp = math.frexp end
if math.ldexp then safe_env.math.ldexp = math.ldexp end
if math.log10 then safe_env.math.log10 = math.log10 end

-- Bit operations (if available)
if bit then
    safe_env.bit = {
        tobit = bit.tobit,
        tohex = bit.tohex,
        bnot = bit.bnot,
        band = bit.band,
        bor = bit.bor,
        bxor = bit.bxor,
        lshift = bit.lshift,
        rshift = bit.rshift,
        arshift = bit.arshift,
        rol = bit.rol,
        ror = bit.ror,
        bswap = bit.bswap,
    }
elseif bit32 then
    safe_env.bit32 = {
        arshift = bit32.arshift,
        band = bit32.band,
        bnot = bit32.bnot,
        bor = bit32.bor,
        btest = bit32.btest,
        bxor = bit32.bxor,
        extract = bit32.extract,
        lrotate = bit32.lrotate,
        lshift = bit32.lshift,
        replace = bit32.replace,
        rrotate = bit32.rrotate,
        rshift = bit32.rshift,
    }
end
"#,
        );

        if self.include_redis_funcs {
            code.push_str(
                r#"
-- Redis functions (provided by the runtime)
-- safe_env.redis = redis -- Will be injected by runtime
"#,
            );
        }

        code.push_str(
            r#"
-- Return the safe environment
return safe_env
"#,
        );

        code
    }
}

/// Represents limits that should be enforced during script execution.
#[derive(Debug, Clone)]
pub struct ExecutionLimits {
    /// Maximum memory in bytes
    pub max_memory: usize,
    /// Maximum instructions
    pub max_instructions: u64,
    /// Maximum execution time
    pub max_time: Duration,
}

impl Default for ExecutionLimits {
    fn default() -> Self {
        Self {
            max_memory: 256 * 1024 * 1024,
            max_instructions: 10_000_000,
            max_time: Duration::from_secs(5),
        }
    }
}

impl From<&LuaSandboxConfig> for ExecutionLimits {
    fn from(config: &LuaSandboxConfig) -> Self {
        Self {
            max_memory: config.max_memory,
            max_instructions: config.max_instructions,
            max_time: config.max_execution_time,
        }
    }
}

/// Track resource usage during script execution.
#[derive(Debug, Default)]
pub struct ResourceTracker {
    /// Memory allocated
    memory_used: AtomicU64,
    /// Instructions executed
    instructions: AtomicU64,
}

impl ResourceTracker {
    /// Create a new resource tracker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add memory allocation.
    pub fn add_memory(&self, bytes: usize) {
        self.memory_used
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// Remove memory allocation.
    pub fn free_memory(&self, bytes: usize) {
        let current = self.memory_used.load(Ordering::Relaxed);
        let new_val = current.saturating_sub(bytes as u64);
        self.memory_used.store(new_val, Ordering::Relaxed);
    }

    /// Get current memory usage.
    #[must_use]
    pub fn memory_used(&self) -> u64 {
        self.memory_used.load(Ordering::Relaxed)
    }

    /// Increment instruction count.
    pub fn increment_instructions(&self) -> u64 {
        self.instructions.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Get instruction count.
    #[must_use]
    pub fn instructions(&self) -> u64 {
        self.instructions.load(Ordering::Relaxed)
    }

    /// Check if memory limit is exceeded.
    #[must_use]
    pub fn check_memory(&self, limit: usize) -> Result<(), SandboxError> {
        let used = self.memory_used.load(Ordering::Relaxed) as usize;
        if used > limit {
            Err(SandboxError::MemoryLimitExceeded { used, limit })
        } else {
            Ok(())
        }
    }

    /// Check if instruction limit is exceeded.
    #[must_use]
    pub fn check_instructions(&self, limit: u64) -> Result<(), SandboxError> {
        if limit == 0 {
            return Ok(()); // 0 means unlimited
        }
        let executed = self.instructions.load(Ordering::Relaxed);
        if executed > limit {
            Err(SandboxError::InstructionLimitExceeded { executed, limit })
        } else {
            Ok(())
        }
    }

    /// Reset the tracker.
    pub fn reset(&self) {
        self.memory_used.store(0, Ordering::Relaxed);
        self.instructions.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_script_validator_detects_os_access() {
        let validator = ScriptValidator::new();

        let malicious = "os.execute('rm -rf /')";
        assert!(validator.validate(malicious).is_err());

        let safe = "return 1 + 1";
        assert!(validator.validate(safe).is_ok());
    }

    #[test]
    fn test_script_validator_detects_io_access() {
        let validator = ScriptValidator::new();

        let malicious = "io.open('/etc/passwd')";
        assert!(validator.validate(malicious).is_err());
    }

    #[test]
    fn test_script_validator_detects_debug() {
        let validator = ScriptValidator::new();

        let malicious = "debug.getinfo(1)";
        assert!(validator.validate(malicious).is_err());
    }

    #[test]
    fn test_script_validator_detects_metatable() {
        let validator = ScriptValidator::new();

        let malicious = "setmetatable({}, {__index = _G})";
        assert!(validator.validate(malicious).is_err());
    }

    #[test]
    fn test_script_validator_detects_load() {
        let validator = ScriptValidator::new();

        let malicious = "load('return os')()";
        assert!(validator.validate(malicious).is_err());
    }

    #[test]
    fn test_script_validator_detects_require() {
        let validator = ScriptValidator::new();

        let malicious = "require('socket')";
        assert!(validator.validate(malicious).is_err());
    }

    #[test]
    fn test_script_validator_allows_safe_code() {
        let validator = ScriptValidator::new();

        let safe_scripts = vec![
            "return 1 + 2",
            "local x = 'hello'; return x",
            "for i = 1, 10 do print(i) end",
            "local t = {1, 2, 3}; return #t",
            "return string.upper('hello')",
            "return math.sqrt(16)",
        ];

        for script in safe_scripts {
            assert!(
                validator.validate(script).is_ok(),
                "Script should be safe: {script}"
            );
        }
    }

    #[test]
    fn test_resource_tracker() {
        let tracker = ResourceTracker::new();

        tracker.add_memory(1000);
        assert_eq!(tracker.memory_used(), 1000);

        tracker.free_memory(500);
        assert_eq!(tracker.memory_used(), 500);

        // Test instruction counting
        assert_eq!(tracker.increment_instructions(), 1);
        assert_eq!(tracker.increment_instructions(), 2);
        assert_eq!(tracker.instructions(), 2);
    }

    #[test]
    fn test_resource_limits() {
        let tracker = ResourceTracker::new();

        tracker.add_memory(100);
        assert!(tracker.check_memory(200).is_ok());
        assert!(tracker.check_memory(50).is_err());

        for _ in 0..100 {
            tracker.increment_instructions();
        }
        assert!(tracker.check_instructions(200).is_ok());
        assert!(tracker.check_instructions(50).is_err());
        assert!(tracker.check_instructions(0).is_ok()); // 0 = unlimited
    }

    #[test]
    fn test_environment_builder() {
        let builder = SafeEnvironmentBuilder::new();
        let code = builder.generate_environment_setup();

        // Should include safe functions
        assert!(code.contains("safe_env.assert = assert"));
        assert!(code.contains("safe_env.string"));
        assert!(code.contains("safe_env.math"));
        assert!(code.contains("safe_env.table"));

        // Should not include dangerous functions
        assert!(!code.contains("safe_env.os"));
        assert!(!code.contains("safe_env.io"));
        assert!(!code.contains("safe_env.debug"));
    }

    #[test]
    fn test_sandbox_config_default() {
        let config = LuaSandboxConfig::default();

        assert_eq!(config.max_memory, 256 * 1024 * 1024);
        assert_eq!(config.max_execution_time, Duration::from_secs(5));
        assert_eq!(config.max_instructions, 10_000_000);
        assert!(config.allow_random);
        assert!(!config.debug_mode);
    }

    #[test]
    fn test_execution_limits_from_config() {
        let config = LuaSandboxConfig {
            max_memory: 1024,
            max_execution_time: Duration::from_secs(10),
            max_instructions: 1000,
            ..Default::default()
        };

        let limits = ExecutionLimits::from(&config);
        assert_eq!(limits.max_memory, 1024);
        assert_eq!(limits.max_time, Duration::from_secs(10));
        assert_eq!(limits.max_instructions, 1000);
    }
}
