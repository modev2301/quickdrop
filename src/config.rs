use std::path::PathBuf;
use std::fs;
use serde::{Deserialize, Serialize};
use crate::error::TransferError;

// Performance tuning constants
pub const BASE_CHUNK_SIZE: usize = 64 * 1024; // 64KB
pub const DEFAULT_PARALLEL_STREAMS: usize = 16;
pub const MEMORY_POOL_SIZE: usize = 2000;
pub const SIMD_CHUNK_SIZE: usize = 1024 * 1024; // 1MB for SIMD operations
pub const PREDICTIVE_CHUNKING_ENABLED: bool = true;
pub const ZERO_COPY_ENABLED: bool = true;
pub const MPTCP_ENABLED: bool = false; // Placeholder for Multi-Path TCP
pub const HARDWARE_ACCELERATION_ENABLED: bool = false; // Placeholder for GPU/CPU acceleration
pub const AI_OPTIMIZATION_ENABLED: bool = false; // Placeholder for ML-based optimization

// Network constants
pub const DEFAULT_TIMEOUT_SECONDS: u64 = 30;
pub const MAX_RETRY_ATTEMPTS: u32 = 3;
pub const RETRY_DELAY_MS: u64 = 1000;
pub const CONNECTION_POOL_SIZE: usize = 100;
pub const ADAPTIVE_CHUNKING_ENABLED: bool = true;

// Security constants
pub const DEFAULT_AUTH_TOKEN: &str = "quickdrop_default_token";
pub const ENCRYPTION_ENABLED: bool = false; // Placeholder for encryption
pub const TLS_ENABLED: bool = false; // Placeholder for TLS

// Compression constants
pub const COMPRESSION_THRESHOLD: usize = 1024; // Only compress chunks larger than 1KB
pub const LZ4_COMPRESSION_LEVEL: u32 = 1; // Fast compression
pub const ZSTD_COMPRESSION_LEVEL: i32 = 3; // Balanced compression

// Monitoring constants
pub const METRICS_ENABLED: bool = true;
pub const PROGRESS_BAR_ENABLED: bool = true;
pub const DETAILED_LOGGING: bool = true;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub client: ClientConfig,
    pub security: SecurityConfig,
    pub performance: PerformanceConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub address: String,
    pub port: u16,
    pub output_directory: String,
    pub max_clients: usize,
    pub parallel_streams: Option<usize>,
    pub buffer_size: Option<usize>,
    pub enable_progress_bar: bool,
    pub enable_compression: bool,
    pub enable_resume: bool,
    pub timeout_seconds: u64,
    pub max_file_size: Option<u64>,
    pub allowed_extensions: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientConfig {
    pub server_address: String,
    pub server_port: u16,
    pub parallel_streams: Option<usize>,
    pub chunk_size: Option<usize>,
    pub buffer_size: Option<usize>,
    pub enable_compression: bool,
    pub enable_resume: bool,
    pub timeout_seconds: u64,
    pub retry_attempts: u32,
    pub retry_delay_ms: u64,
    pub progress_bar_enabled: bool,
    pub detailed_logging: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    pub auth_token: String,
    pub encryption_enabled: bool,
    pub tls_enabled: bool,
    pub certificate_path: Option<String>,
    pub private_key_path: Option<String>,
    pub allowed_ips: Option<Vec<String>>,
    pub max_connections_per_ip: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    pub simd_enabled: bool,
    pub zero_copy_enabled: bool,
    pub mptcp_enabled: bool,
    pub hardware_acceleration_enabled: bool,
    pub ai_optimization_enabled: bool,
    pub predictive_chunking_enabled: bool,
    pub adaptive_chunking_enabled: bool,
    pub memory_pool_size: usize,
    pub connection_pool_size: usize,
    pub compression_level: u32,
    pub metrics_enabled: bool,
}

impl Config {
    pub fn load_or_create(path: &PathBuf) -> Result<Self, TransferError> {
        if path.exists() {
            let content = fs::read_to_string(path)
                .map_err(|e| TransferError::Io(e))?;
            
            toml::from_str(&content)
                .map_err(|e| TransferError::TomlDeserialization(e))
        } else {
            let config = Self::default();
            config.save(path)?;
            Ok(config)
        }
    }

    pub fn save(&self, path: &PathBuf) -> Result<(), TransferError> {
        let content = toml::to_string_pretty(self)
            .map_err(|e| TransferError::TomlSerialization(e))?;
        
        fs::write(path, content)
            .map_err(|e| TransferError::Io(e))?;
        
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            client: ClientConfig::default(),
            security: SecurityConfig::default(),
            performance: PerformanceConfig::default(),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            address: "127.0.0.1".to_string(),
            port: 8080,
            output_directory: "./downloads".to_string(),
            max_clients: 100,
            parallel_streams: Some(DEFAULT_PARALLEL_STREAMS),
            buffer_size: Some(BASE_CHUNK_SIZE),
            enable_progress_bar: PROGRESS_BAR_ENABLED,
            enable_compression: true,
            enable_resume: true,
            timeout_seconds: DEFAULT_TIMEOUT_SECONDS,
            max_file_size: Some(1024 * 1024 * 1024), // 1GB
            allowed_extensions: Some(vec!["*".to_string()]),
        }
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            server_address: "127.0.0.1".to_string(),
            server_port: 8080,
            parallel_streams: Some(DEFAULT_PARALLEL_STREAMS),
            chunk_size: Some(BASE_CHUNK_SIZE),
            buffer_size: Some(BASE_CHUNK_SIZE),
            enable_compression: true,
            enable_resume: true,
            timeout_seconds: DEFAULT_TIMEOUT_SECONDS,
            retry_attempts: MAX_RETRY_ATTEMPTS,
            retry_delay_ms: RETRY_DELAY_MS,
            progress_bar_enabled: PROGRESS_BAR_ENABLED,
            detailed_logging: DETAILED_LOGGING,
        }
    }
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            auth_token: DEFAULT_AUTH_TOKEN.to_string(),
            encryption_enabled: ENCRYPTION_ENABLED,
            tls_enabled: TLS_ENABLED,
            certificate_path: None,
            private_key_path: None,
            allowed_ips: None,
            max_connections_per_ip: Some(10),
        }
    }
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            simd_enabled: true,
            zero_copy_enabled: ZERO_COPY_ENABLED,
            mptcp_enabled: MPTCP_ENABLED,
            hardware_acceleration_enabled: HARDWARE_ACCELERATION_ENABLED,
            ai_optimization_enabled: AI_OPTIMIZATION_ENABLED,
            predictive_chunking_enabled: PREDICTIVE_CHUNKING_ENABLED,
            adaptive_chunking_enabled: ADAPTIVE_CHUNKING_ENABLED,
            memory_pool_size: MEMORY_POOL_SIZE,
            connection_pool_size: CONNECTION_POOL_SIZE,
            compression_level: LZ4_COMPRESSION_LEVEL,
            metrics_enabled: METRICS_ENABLED,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_config_default() {
        let config = Config::default();
        
        assert_eq!(config.server.address, "127.0.0.1");
        assert_eq!(config.server.port, 8080);
        assert_eq!(config.client.server_address, "127.0.0.1");
        assert_eq!(config.client.server_port, 8080);
        assert_eq!(config.security.auth_token, DEFAULT_AUTH_TOKEN);
        assert!(config.performance.simd_enabled);
    }

    #[test]
    fn test_server_config_default() {
        let config = ServerConfig::default();
        
        assert_eq!(config.address, "127.0.0.1");
        assert_eq!(config.port, 8080);
        assert_eq!(config.output_directory, "./downloads");
        assert_eq!(config.max_clients, 100);
        assert_eq!(config.parallel_streams, Some(DEFAULT_PARALLEL_STREAMS));
        assert_eq!(config.buffer_size, Some(BASE_CHUNK_SIZE));
        assert!(config.enable_progress_bar);
        assert!(config.enable_compression);
        assert!(config.enable_resume);
        assert_eq!(config.timeout_seconds, DEFAULT_TIMEOUT_SECONDS);
    }

    #[test]
    fn test_client_config_default() {
        let config = ClientConfig::default();
        
        assert_eq!(config.server_address, "127.0.0.1");
        assert_eq!(config.server_port, 8080);
        assert_eq!(config.parallel_streams, Some(DEFAULT_PARALLEL_STREAMS));
        assert_eq!(config.chunk_size, Some(BASE_CHUNK_SIZE));
        assert_eq!(config.buffer_size, Some(BASE_CHUNK_SIZE));
        assert!(config.enable_compression);
        assert!(config.enable_resume);
        assert_eq!(config.timeout_seconds, DEFAULT_TIMEOUT_SECONDS);
        assert_eq!(config.retry_attempts, MAX_RETRY_ATTEMPTS);
        assert_eq!(config.retry_delay_ms, RETRY_DELAY_MS);
    }

    #[test]
    fn test_security_config_default() {
        let config = SecurityConfig::default();
        
        assert_eq!(config.auth_token, DEFAULT_AUTH_TOKEN);
        assert!(!config.encryption_enabled);
        assert!(!config.tls_enabled);
        assert!(config.certificate_path.is_none());
        assert!(config.private_key_path.is_none());
        assert!(config.allowed_ips.is_none());
        assert_eq!(config.max_connections_per_ip, Some(10));
    }

    #[test]
    fn test_performance_config_default() {
        let config = PerformanceConfig::default();
        
        assert!(config.simd_enabled);
        assert!(config.zero_copy_enabled);
        assert!(!config.mptcp_enabled);
        assert!(!config.hardware_acceleration_enabled);
        assert!(!config.ai_optimization_enabled);
        assert!(config.predictive_chunking_enabled);
        assert!(config.adaptive_chunking_enabled);
        assert_eq!(config.memory_pool_size, MEMORY_POOL_SIZE);
        assert_eq!(config.connection_pool_size, CONNECTION_POOL_SIZE);
        assert_eq!(config.compression_level, LZ4_COMPRESSION_LEVEL);
        assert!(config.metrics_enabled);
    }

    #[test]
    fn test_config_serialization() {
        let config = Config::default();
        let serialized = toml::to_string(&config).unwrap();
        let deserialized: Config = toml::from_str(&serialized).unwrap();
        
        assert_eq!(config.server.address, deserialized.server.address);
        assert_eq!(config.server.port, deserialized.server.port);
        assert_eq!(config.client.server_address, deserialized.client.server_address);
        assert_eq!(config.client.server_port, deserialized.client.server_port);
    }

    #[test]
    fn test_config_save_and_load() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("test_config.toml");
        
        let original_config = Config::default();
        original_config.save(&config_path).unwrap();
        
        let loaded_config = Config::load_or_create(&config_path).unwrap();
        
        assert_eq!(original_config.server.address, loaded_config.server.address);
        assert_eq!(original_config.server.port, loaded_config.server.port);
        assert_eq!(original_config.client.server_address, loaded_config.client.server_address);
        assert_eq!(original_config.client.server_port, loaded_config.client.server_port);
    }

    #[test]
    fn test_config_create_new() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("new_config.toml");
        
        // Should create new config file
        let config = Config::load_or_create(&config_path).unwrap();
        
        assert!(config_path.exists());
        assert_eq!(config.server.address, "127.0.0.1");
        assert_eq!(config.server.port, 8080);
    }

    #[test]
    fn test_constants() {
        assert_eq!(BASE_CHUNK_SIZE, 64 * 1024);
        assert_eq!(DEFAULT_PARALLEL_STREAMS, 16);
        assert_eq!(MEMORY_POOL_SIZE, 2000);
        assert_eq!(SIMD_CHUNK_SIZE, 1024 * 1024);
        assert!(PREDICTIVE_CHUNKING_ENABLED);
        assert!(ZERO_COPY_ENABLED);
        assert!(!MPTCP_ENABLED);
        assert!(!HARDWARE_ACCELERATION_ENABLED);
        assert!(!AI_OPTIMIZATION_ENABLED);
        assert_eq!(DEFAULT_TIMEOUT_SECONDS, 30);
        assert_eq!(MAX_RETRY_ATTEMPTS, 3);
        assert_eq!(RETRY_DELAY_MS, 1000);
        assert_eq!(CONNECTION_POOL_SIZE, 100);
        assert!(ADAPTIVE_CHUNKING_ENABLED);
        assert_eq!(DEFAULT_AUTH_TOKEN, "quickdrop_default_token");
        assert!(!ENCRYPTION_ENABLED);
        assert!(!TLS_ENABLED);
        assert_eq!(COMPRESSION_THRESHOLD, 1024);
        assert_eq!(LZ4_COMPRESSION_LEVEL, 1);
        assert_eq!(ZSTD_COMPRESSION_LEVEL, 3);
        assert!(METRICS_ENABLED);
        assert!(PROGRESS_BAR_ENABLED);
        assert!(DETAILED_LOGGING);
    }

    #[test]
    fn test_custom_config() {
        let mut config = Config::default();
        config.server.address = "0.0.0.0".to_string();
        config.server.port = 9000;
        config.client.server_address = "192.168.1.100".to_string();
        config.client.server_port = 9000;
        config.security.auth_token = "custom_token".to_string();
        config.performance.simd_enabled = false;
        
        assert_eq!(config.server.address, "0.0.0.0");
        assert_eq!(config.server.port, 9000);
        assert_eq!(config.client.server_address, "192.168.1.100");
        assert_eq!(config.client.server_port, 9000);
        assert_eq!(config.security.auth_token, "custom_token");
        assert!(!config.performance.simd_enabled);
    }
} 