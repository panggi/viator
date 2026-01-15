//! TLS configuration for secure connections.
//!
//! Provides:
//! - TLS 1.2/1.3 support via rustls
//! - Certificate and key loading
//! - mTLS (mutual TLS) for client authentication
//! - Cipher suite configuration
//!
//! # Redis Compatibility
//!
//! Compatible with Redis TLS configuration options:
//! - tls-port
//! - tls-cert-file
//! - tls-key-file
//! - tls-ca-cert-file
//! - tls-auth-clients

use std::fs::File;
use std::path::Path;

#[cfg(feature = "tls")]
use std::io::BufReader;
#[cfg(feature = "tls")]
use std::sync::Arc;

#[cfg(feature = "tls")]
use tokio_rustls::rustls::{
    pki_types::{CertificateDer, PrivateKeyDer},
    server::WebPkiClientVerifier,
    RootCertStore, ServerConfig,
};

/// TLS configuration options.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Enable TLS
    pub enabled: bool,
    /// TLS port (default: 6380)
    pub port: u16,
    /// Path to server certificate file (PEM format)
    pub cert_file: Option<String>,
    /// Path to server private key file (PEM format)
    pub key_file: Option<String>,
    /// Path to CA certificate file for client verification (mTLS)
    pub ca_cert_file: Option<String>,
    /// Require client certificates (mTLS)
    pub auth_clients: TlsAuthClients,
    /// Minimum TLS version (1.2 or 1.3)
    pub min_version: TlsVersion,
    /// Preferred cipher suites (empty = use defaults)
    pub ciphersuites: Vec<String>,
    /// Session caching
    pub session_caching: bool,
    /// Session timeout in seconds
    pub session_timeout: u64,
}

/// Client authentication mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TlsAuthClients {
    /// No client certificate required
    #[default]
    No,
    /// Client certificate optional
    Optional,
    /// Client certificate required (mTLS)
    Required,
}

/// TLS version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TlsVersion {
    /// TLS 1.2
    Tls12,
    /// TLS 1.3 (recommended)
    #[default]
    Tls13,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            port: 6380,
            cert_file: None,
            key_file: None,
            ca_cert_file: None,
            auth_clients: TlsAuthClients::No,
            min_version: TlsVersion::Tls13,
            ciphersuites: Vec::new(),
            session_caching: true,
            session_timeout: 300,
        }
    }
}

/// TLS-related errors.
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    /// Failed to load certificate
    #[error("failed to load certificate: {0}")]
    CertificateLoad(String),
    /// Failed to load private key
    #[error("failed to load private key: {0}")]
    PrivateKeyLoad(String),
    /// Failed to load CA certificate
    #[error("failed to load CA certificate: {0}")]
    CaCertificateLoad(String),
    /// Configuration error
    #[error("TLS configuration error: {0}")]
    Configuration(String),
    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

#[cfg(feature = "tls")]
impl TlsConfig {
    /// Build a rustls ServerConfig from this configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if certificates or keys cannot be loaded.
    pub fn build_server_config(&self) -> Result<ServerConfig, TlsError> {
        let cert_file = self
            .cert_file
            .as_ref()
            .ok_or_else(|| TlsError::Configuration("cert_file is required".into()))?;

        let key_file = self
            .key_file
            .as_ref()
            .ok_or_else(|| TlsError::Configuration("key_file is required".into()))?;

        // Load certificates
        let certs = load_certs(cert_file)?;
        let key = load_private_key(key_file)?;

        // Build config based on client auth mode
        let config = match self.auth_clients {
            TlsAuthClients::No => ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .map_err(|e| TlsError::Configuration(e.to_string()))?,

            TlsAuthClients::Optional | TlsAuthClients::Required => {
                let ca_cert_file = self
                    .ca_cert_file
                    .as_ref()
                    .ok_or_else(|| TlsError::Configuration(
                        "ca_cert_file is required for client authentication".into(),
                    ))?;

                let mut root_store = RootCertStore::empty();
                let ca_certs = load_certs(ca_cert_file)?;
                for cert in ca_certs {
                    root_store
                        .add(cert)
                        .map_err(|e| TlsError::CaCertificateLoad(e.to_string()))?;
                }

                let client_verifier = if self.auth_clients == TlsAuthClients::Required {
                    WebPkiClientVerifier::builder(Arc::new(root_store))
                        .build()
                        .map_err(|e| TlsError::Configuration(e.to_string()))?
                } else {
                    WebPkiClientVerifier::builder(Arc::new(root_store))
                        .allow_unauthenticated()
                        .build()
                        .map_err(|e| TlsError::Configuration(e.to_string()))?
                };

                ServerConfig::builder()
                    .with_client_cert_verifier(client_verifier)
                    .with_single_cert(certs, key)
                    .map_err(|e| TlsError::Configuration(e.to_string()))?
            }
        };

        Ok(config)
    }
}

#[cfg(feature = "tls")]
fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let file = File::open(path).map_err(|e| {
        TlsError::CertificateLoad(format!("cannot open {path}: {e}"))
    })?;
    let mut reader = BufReader::new(file);

    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TlsError::CertificateLoad(format!("failed to parse {path}: {e}")))
}

#[cfg(feature = "tls")]
fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>, TlsError> {
    let file = File::open(path).map_err(|e| {
        TlsError::PrivateKeyLoad(format!("cannot open {path}: {e}"))
    })?;
    let mut reader = BufReader::new(file);

    // Try to read as PKCS#8 first, then RSA, then EC
    let keys: Vec<_> = rustls_pemfile::read_all(&mut reader)
        .filter_map(|item| item.ok())
        .collect();

    for item in keys {
        match item {
            rustls_pemfile::Item::Pkcs8Key(key) => {
                return Ok(PrivateKeyDer::Pkcs8(key));
            }
            rustls_pemfile::Item::Pkcs1Key(key) => {
                return Ok(PrivateKeyDer::Pkcs1(key));
            }
            rustls_pemfile::Item::Sec1Key(key) => {
                return Ok(PrivateKeyDer::Sec1(key));
            }
            _ => continue,
        }
    }

    Err(TlsError::PrivateKeyLoad(format!(
        "no private key found in {path}"
    )))
}

/// Helper to check if a path exists and is readable.
pub fn validate_path(path: &str) -> Result<(), TlsError> {
    if !Path::new(path).exists() {
        return Err(TlsError::Configuration(format!("file not found: {path}")));
    }
    File::open(path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = TlsConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.port, 6380);
        assert_eq!(config.auth_clients, TlsAuthClients::No);
    }

    #[test]
    fn test_tls_auth_clients() {
        assert_eq!(TlsAuthClients::default(), TlsAuthClients::No);
    }
}
