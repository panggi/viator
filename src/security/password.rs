//! Secure password hashing using Argon2id.
//!
//! This module provides:
//! - Argon2id password hashing (winner of Password Hashing Competition)
//! - Constant-time comparison to prevent timing attacks
//! - Secure memory wiping via zeroize
//!
//! # Redis Compatibility
//!
//! Redis uses SHA256 for password storage in ACL. We use Argon2id internally
//! for stronger security but support SHA256 hashes for compatibility.

use argon2::{
    Argon2, PasswordHash as Argon2PasswordHash, PasswordVerifier as Argon2Verifier,
    password_hash::{PasswordHasher, SaltString, rand_core::OsRng},
};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use zeroize::Zeroize;

/// Password hash representation.
#[derive(Debug, Clone)]
pub enum PasswordHash {
    /// Argon2id hash (recommended)
    Argon2id(String),
    /// SHA256 hash (Redis compatibility)
    Sha256([u8; 32]),
    /// Plaintext (for nopass users, internal only)
    NoPass,
}

impl PasswordHash {
    /// Create a new Argon2id hash from a password.
    ///
    /// # Errors
    ///
    /// Returns an error if hashing fails.
    pub fn new_argon2id(password: &str) -> Result<Self, PasswordError> {
        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();

        let hash = argon2
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| PasswordError::HashingFailed(e.to_string()))?;

        Ok(Self::Argon2id(hash.to_string()))
    }

    /// Create a SHA256 hash (Redis compatibility mode).
    #[must_use]
    pub fn new_sha256(password: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(password.as_bytes());
        let result = hasher.finalize();
        Self::Sha256(result.into())
    }

    /// Create from a Redis-style hash string.
    ///
    /// Supports:
    /// - `#<hex>` - SHA256 hash
    /// - `>password` - Plaintext password (hashed internally)
    /// - `nopass` - No password required
    #[must_use]
    pub fn from_redis_format(s: &str) -> Option<Self> {
        if s == "nopass" {
            return Some(Self::NoPass);
        }

        if let Some(hex) = s.strip_prefix('#') {
            // SHA256 hash in hex
            if hex.len() == 64 {
                let mut bytes = [0u8; 32];
                if hex::decode_to_slice(hex, &mut bytes).is_ok() {
                    return Some(Self::Sha256(bytes));
                }
            }
            return None;
        }

        if let Some(password) = s.strip_prefix('>') {
            // Plaintext password - hash it
            return Some(Self::new_sha256(password));
        }

        None
    }

    /// Convert to Redis-style format for ACL GETUSER.
    #[must_use]
    pub fn to_redis_format(&self) -> String {
        match self {
            Self::Argon2id(_) => {
                // Argon2id hashes are not exposed in Redis format
                // Return a placeholder
                "#<argon2id>".to_string()
            }
            Self::Sha256(hash) => {
                format!("#{}", hex::encode(hash))
            }
            Self::NoPass => "nopass".to_string(),
        }
    }
}

/// Password verifier with timing-attack protection.
pub struct PasswordVerifier;

impl PasswordVerifier {
    /// Verify a password against a hash using constant-time comparison.
    ///
    /// This function is designed to take the same amount of time regardless
    /// of whether the password is correct, preventing timing attacks.
    pub fn verify(password: &str, hash: &PasswordHash) -> bool {
        match hash {
            PasswordHash::Argon2id(hash_str) => {
                let parsed = match Argon2PasswordHash::new(hash_str) {
                    Ok(h) => h,
                    Err(_) => return false,
                };
                Argon2::default()
                    .verify_password(password.as_bytes(), &parsed)
                    .is_ok()
            }
            PasswordHash::Sha256(expected) => {
                let mut hasher = Sha256::new();
                hasher.update(password.as_bytes());
                let computed: [u8; 32] = hasher.finalize().into();

                // Constant-time comparison
                computed.ct_eq(expected).into()
            }
            PasswordHash::NoPass => true,
        }
    }

    /// Verify with automatic scrubbing of the password from memory.
    pub fn verify_and_zeroize(mut password: String, hash: &PasswordHash) -> bool {
        let result = Self::verify(&password, hash);
        password.zeroize();
        result
    }
}

/// Password-related errors.
#[derive(Debug, Clone, thiserror::Error)]
pub enum PasswordError {
    /// Hashing operation failed.
    #[error("password hashing failed: {0}")]
    HashingFailed(String),
}

/// Simple hex encoding/decoding (to avoid external dependency).
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    pub fn decode_to_slice(hex: &str, out: &mut [u8]) -> Result<(), ()> {
        if hex.len() != out.len() * 2 {
            return Err(());
        }

        for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
            let high = hex_char_to_nibble(chunk[0])?;
            let low = hex_char_to_nibble(chunk[1])?;
            out[i] = (high << 4) | low;
        }

        Ok(())
    }

    fn hex_char_to_nibble(c: u8) -> Result<u8, ()> {
        match c {
            b'0'..=b'9' => Ok(c - b'0'),
            b'a'..=b'f' => Ok(c - b'a' + 10),
            b'A'..=b'F' => Ok(c - b'A' + 10),
            _ => Err(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha256_hash() {
        let hash = PasswordHash::new_sha256("mypassword");
        assert!(PasswordVerifier::verify("mypassword", &hash));
        assert!(!PasswordVerifier::verify("wrongpassword", &hash));
    }

    #[test]
    fn test_argon2id_hash() {
        let hash = PasswordHash::new_argon2id("securepassword").unwrap();
        assert!(PasswordVerifier::verify("securepassword", &hash));
        assert!(!PasswordVerifier::verify("wrongpassword", &hash));
    }

    #[test]
    fn test_nopass() {
        let hash = PasswordHash::NoPass;
        assert!(PasswordVerifier::verify("anything", &hash));
        assert!(PasswordVerifier::verify("", &hash));
    }

    #[test]
    fn test_redis_format_sha256() {
        let hash = PasswordHash::new_sha256("test");
        let redis_format = hash.to_redis_format();
        assert!(redis_format.starts_with('#'));

        let parsed = PasswordHash::from_redis_format(&redis_format).unwrap();
        assert!(PasswordVerifier::verify("test", &parsed));
    }

    #[test]
    fn test_redis_format_plaintext() {
        let hash = PasswordHash::from_redis_format(">mypassword").unwrap();
        assert!(PasswordVerifier::verify("mypassword", &hash));
    }

    #[test]
    fn test_constant_time_comparison() {
        // This test verifies that wrong passwords don't short-circuit
        // (though timing is hard to test directly)
        let hash = PasswordHash::new_sha256("correct");

        // Both should take similar time
        assert!(!PasswordVerifier::verify("wrong1", &hash));
        assert!(!PasswordVerifier::verify(
            "completely_different_password",
            &hash
        ));
        assert!(PasswordVerifier::verify("correct", &hash));
    }
}
