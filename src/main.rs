use clap::{Parser, Subcommand};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::fs::{File, create_dir_all, metadata};
use std::env;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, mpsc, oneshot, Semaphore};
use serde::{Deserialize, Serialize};
use toml;
use indicatif::{ProgressBar, ProgressStyle, MultiProgress};
use bytes::{Bytes, BytesMut};
use std::collections::{HashMap, HashSet, VecDeque};
use uuid::Uuid;
use std::time::{Duration, Instant, SystemTime};
use tokio::time::{timeout, sleep};
use std::sync::atomic::{AtomicBool, AtomicUsize, AtomicU64, Ordering};
use sha2::{Sha256, Digest};
use tracing::{info, warn, error, debug, instrument};
use tracing_subscriber::{EnvFilter, fmt};
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use std::io;
use async_trait::async_trait;

// --- Constants ---
const BASE_CHUNK_SIZE: usize = 512 * 1024; // 512KB
const MIN_CHUNK_SIZE: usize = 64 * 1024;   // 64KB
const MAX_CHUNK_SIZE: usize = 32 * 1024 * 1024; // 32MB
const DEFAULT_PARALLEL_STREAMS: usize = 8; // Increased for HPC
const DEFAULT_BUFFER_SIZE: usize = 128 * 1024; // 128KB buffer, larger for better throughput
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(60); // Increased
const CHUNK_TIMEOUT: Duration = Duration::from_secs(30); // Increased
const MAX_RETRIES: u32 = 10; // Increased for resilience
const MAX_CONCURRENT_CHUNKS: usize = 500; // Increased for higher concurrency
const MEMORY_POOL_SIZE: usize = 1000; // Larger pool
const ACK_BATCH_SIZE: usize = 100; // Batch more ACKs
const COMPRESSION_THRESHOLD: usize = 2048; // 2KB, adjust based on typical data compressibility
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10); // Heartbeats for liveness check
const RESUME_INFO_SAVE_INTERVAL: Duration = Duration::from_secs(5); // How often to save resume info

// --- Custom Error Type ---
#[derive(Debug, thiserror::Error)]
enum TransferError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("TOML serialization error: {0}")]
    TomlSerialization(#[from] toml::ser::Error),
    #[error("TOML deserialization error: {0}")]
    TomlDeserialization(#[from] toml::de::Error),
    #[error("Timeout error: {0}")]
    Timeout(#[from] tokio::time::error::Elapsed),
    #[error("Chunk validation failed for chunk ID {chunk_id}")]
    ChunkValidationFailed { chunk_id: u64 },
    #[error("Transfer protocol error: {0}")]
    ProtocolError(String),
    #[error("Configuration error: {0}")]
    ConfigError(String),
    #[error("File not found: {0}")]
    FileNotFound(PathBuf),
    #[error("Transfer cancelled")]
    Cancelled,
    #[error("Received error from remote: {0}")]
    RemoteError(String),
    #[error("No response from remote")]
    NoResponse,
}

// --- Configuration ---
#[derive(Deserialize, Serialize, Clone, Debug)]
struct Config {
    server: ServerConfig,
    client: ClientConfig,
    security: Option<SecurityConfig>,
    performance: Option<PerformanceConfig>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
struct ServerConfig {
    address: String,
    port: u16,
    max_clients: usize,
    enable_progress_bar: bool,
    parallel_streams: Option<usize>,
    buffer_size: Option<usize>,
    resume_directory: Option<String>,
    max_file_size: Option<u64>,
    allowed_extensions: Option<Vec<String>>,
    output_directory: String, // Where to save received files
}

#[derive(Deserialize, Serialize, Clone, Debug)]
struct ClientConfig {
    server_address: String,
    enable_progress_bar: bool,
    parallel_streams: Option<usize>,
    buffer_size: Option<usize>,
    max_speed_mbps: Option<f64>,
    resume_directory: Option<String>,
    compression_enabled: Option<bool>,
    auto_resume: Option<bool>,
    tcp_nodelay: Option<bool>,
    tcp_send_buffer_size: Option<usize>,
    tcp_recv_buffer_size: Option<usize>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
struct SecurityConfig {
    encryption_enabled: bool, // Placeholder, requires external crypto library
    auth_token: Option<String>,
    max_session_duration: Option<u64>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
struct PerformanceConfig {
    adaptive_chunking: bool,
    zero_copy_enabled: bool, // Placeholder, requires OS-specific implementation
    connection_pooling: bool,
    memory_limit_mb: Option<usize>,
    tcp_nodelay: Option<bool>,
    tcp_send_buffer_size: Option<usize>,
    tcp_recv_buffer_size: Option<usize>,
}

impl Config {
    fn load_or_create(path: &Path) -> Result<Self, TransferError> {
        if path.exists() {
            let content = std::fs::read_to_string(path)?;
            Ok(toml::from_str(&content)?)
        } else {
            let config = Self::default();
            let content = toml::to_string_pretty(&config)?;
            std::fs::write(path, content)?;
            info!("Created default config file at {:?}", path);
            Ok(config)
        }
    }

    fn default() -> Self {
        Config {
            server: ServerConfig {
                address: "0.0.0.0".to_string(),
                port: 8080,
                max_clients: 100,
                enable_progress_bar: true,
                parallel_streams: Some(DEFAULT_PARALLEL_STREAMS),
                buffer_size: Some(DEFAULT_BUFFER_SIZE),
                resume_directory: Some("./resume_server".to_string()),
                max_file_size: None,
                allowed_extensions: None,
                output_directory: "./received_files".to_string(),
            },
            client: ClientConfig {
                server_address: "127.0.0.1:8080".to_string(),
                enable_progress_bar: true,
                parallel_streams: Some(DEFAULT_PARALLEL_STREAMS),
                buffer_size: Some(DEFAULT_BUFFER_SIZE),
                max_speed_mbps: None,
                resume_directory: Some("./resume_client".to_string()),
                compression_enabled: Some(true),
                auto_resume: Some(true),
                tcp_nodelay: Some(true),
                tcp_send_buffer_size: None,
                tcp_recv_buffer_size: None,
            },
            security: Some(SecurityConfig {
                encryption_enabled: false,
                auth_token: None,
                max_session_duration: None,
            }),
            performance: Some(PerformanceConfig {
                adaptive_chunking: true,
                zero_copy_enabled: false, // Set to true if OS-specific implementation is done
                connection_pooling: true,
                memory_limit_mb: None,
                tcp_nodelay: Some(true),
                tcp_send_buffer_size: None,
                tcp_recv_buffer_size: None,
            }),
        }
    }
}

// --- Wire Protocol Messages ---
#[derive(Debug, Serialize, Deserialize)]
enum Message {
    Handshake {
        session_id: String,
        filename: String,
        file_size: u64,
        chunk_size: usize,
        parallel_streams: usize,
        compression_enabled: bool,
        auth_token: Option<String>,
        resume_info: Option<SerializableResumeInfo>,
    },
    FileChunk {
        chunk_id: u64,
        session_id: String,
        data: Bytes,
        is_last: bool,
        is_compressed: bool,
        checksum: [u8; 32],
        original_size: usize,
        sequence_number: u64,
        retry_count: u32,
    },
    ChunkAck {
        session_id: String,
        received_chunks: Vec<u64>,
        missing_chunks: Vec<u64>,
        timestamp: u64,
    },
    EndOfStream {
        session_id: String,
        stream_id: usize, // To indicate which parallel stream has finished
    },
    Error {
        session_id: String,
        message: String,
    },
    Heartbeat {
        session_id: String,
        timestamp: u64,
    },
}

impl Message {
    async fn write_to_stream(
        &self,
        writer: &mut BufWriter<TcpStream>,
    ) -> Result<(), TransferError> {
        let serialized = serde_json::to_vec(self)?;
        let len = serialized.len() as u32;
        writer.write_all(&len.to_be_bytes()).await?;
        writer.write_all(&serialized).await?;
        writer.flush().await?;
        Ok(())
    }

    async fn read_from_stream(
        reader: &mut BufReader<TcpStream>,
    ) -> Result<Option<Self>, TransferError> {
        let mut len_buf = [0u8; 4];
        if reader.read_exact(&mut len_buf).await.is_err() {
            // Stream closed or error
            return Ok(None);
        }
        let len = u32::from_be_bytes(len_buf) as usize;

        if len == 0 || len > (100 * 1024 * 1024) { // Max 100MB message size
            warn!("Received invalid message length: {}", len);
            return Err(TransferError::ProtocolError(format!("Invalid message length: {}", len)));
        }

        let mut buf = BytesMut::with_capacity(len);
        buf.resize(len, 0);
        reader.read_exact(&mut buf).await?;
        let msg: Message = serde_json::from_slice(&buf)?;
        Ok(Some(msg))
    }
}

// --- Enhanced FileChunk ---
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileChunk {
    chunk_id: u64,
    session_id: String,
    data: Bytes,
    is_last: bool,
    is_compressed: bool,
    checksum: [u8; 32],
    original_size: usize, // Size before compression
    sequence_number: u64,
    retry_count: u32,
}

impl FileChunk {
    fn new(
        chunk_id: u64,
        session_id: String,
        data: Bytes,
        is_last: bool,
        sequence_number: u64,
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(&data);
        let checksum: [u8; 32] = hasher.finalize().into();
        let original_size = data.len();

        Self {
            chunk_id,
            session_id,
            data,
            is_last,
            is_compressed: false,
            checksum,
            original_size,
            sequence_number,
            retry_count: 0,
        }
    }

    fn compress(&mut self) -> Result<(), TransferError> {
        if self.data.len() > COMPRESSION_THRESHOLD && !self.is_compressed {
            let compressed = compress_prepend_size(&self.data)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("LZ4 compression failed: {}", e)))?;
            if compressed.len() < self.data.len() {
                self.data = Bytes::from(compressed);
                self.is_compressed = true;
                debug!(
                    "Compressed chunk {} from {} to {} bytes",
                    self.chunk_id, self.original_size, self.data.len()
                );
            }
        }
        Ok(())
    }

    fn decompress(&mut self) -> Result<(), TransferError> {
        if self.is_compressed {
            let decompressed = decompress_size_prepended(&self.data)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("LZ4 decompression failed: {}", e)))?;
            self.data = Bytes::from(decompressed);
            self.is_compressed = false;
        }
        Ok(())
    }

    fn validate(&self) -> bool {
        let mut temp_chunk = self.clone();
        if temp_chunk.is_compressed {
            if temp_chunk.decompress().is_err() {
                error!("Failed to decompress chunk {} for validation", self.chunk_id);
                return false;
            }
        }

        let mut hasher = Sha256::new();
        hasher.update(&temp_chunk.data);
        let computed: [u8; 32] = hasher.finalize().into();
        computed == self.checksum
    }
}

// --- Resume Information with custom serialization for Vec<bool> ---
// Vec<bool> is used for simplicity with serde, BitVec could be more space-efficient
// but requires custom serde implementations.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerializableResumeInfo {
    session_id: String,
    filename: String,
    file_size: u64,
    chunk_bitmap: Vec<bool>,
    last_chunk_id: u64,
    bytes_transferred: u64,
    timestamp: u64,
    file_hash: Option<[u8; 32]>,
    chunk_size: usize,
    compression_enabled: bool,
}

#[derive(Debug, Clone)]
struct ResumeInfo {
    session_id: String,
    filename: String,
    file_size: u64,
    chunk_bitmap: Vec<bool>,
    last_chunk_id: u64,
    bytes_transferred: u64,
    timestamp: u64,
    file_hash: Option<[u8; 32]>,
    chunk_size: usize,
    compression_enabled: bool,
}

impl ResumeInfo {
    fn new(session_id: String, filename: String, file_size: u64, chunk_size: usize) -> Self {
        let total_chunks = (file_size as f64 / chunk_size as f64).ceil() as usize;
        Self {
            session_id,
            filename,
            file_size,
            chunk_bitmap: vec![false; total_chunks],
            last_chunk_id: 0,
            bytes_transferred: 0,
            timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            file_hash: None,
            chunk_size,
            compression_enabled: false,
        }
    }

    fn mark_chunk_received(&mut self, chunk_id: u64, chunk_len: usize) {
        if (chunk_id as usize) < self.chunk_bitmap.len() {
            if !self.chunk_bitmap[chunk_id as usize] {
                self.chunk_bitmap[chunk_id as usize] = true;
                self.bytes_transferred += chunk_len as u64;
                self.last_chunk_id = chunk_id;
            }
        }
        self.timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    fn get_missing_chunks(&self) -> Vec<u64> {
        self.chunk_bitmap
            .iter()
            .enumerate()
            .filter(|(_, &received)| !received)
            .map(|(idx, _)| idx as u64)
            .collect()
    }

    fn completion_percentage(&self) -> f64 {
        let received_chunks = self.chunk_bitmap.iter().filter(|&&b| b).count();
        if self.chunk_bitmap.is_empty() {
            return 0.0;
        }
        received_chunks as f64 / self.chunk_bitmap.len() as f64 * 100.0
    }

    async fn save(&self, resume_dir: &Path) -> Result<(), TransferError> {
        create_dir_all(resume_dir).await?;
        let resume_file = resume_dir.join(format!("{}.resume", self.session_id));

        let serializable = SerializableResumeInfo {
            session_id: self.session_id.clone(),
            filename: self.filename.clone(),
            file_size: self.file_size,
            chunk_bitmap: self.chunk_bitmap.clone(),
            last_chunk_id: self.last_chunk_id,
            bytes_transferred: self.bytes_transferred,
            timestamp: self.timestamp,
            file_hash: self.file_hash,
            chunk_size: self.chunk_size,
            compression_enabled: self.compression_enabled,
        };

        let json = serde_json::to_string_pretty(&serializable)?;
        tokio::fs::write(resume_file, json).await?;
        Ok(())
    }

    async fn load(session_id: &str, resume_dir: &Path) -> Option<Self> {
        let resume_file = resume_dir.join(format!("{}.resume", session_id));
        if let Ok(json) = tokio::fs::read_to_string(resume_file).await {
            if let Ok(serializable) = serde_json::from_str::<SerializableResumeInfo>(&json) {
                Some(Self {
                    session_id: serializable.session_id,
                    filename: serializable.filename,
                    file_size: serializable.file_size,
                    chunk_bitmap: serializable.chunk_bitmap,
                    last_chunk_id: serializable.last_chunk_id,
                    bytes_transferred: serializable.bytes_transferred,
                    timestamp: serializable.timestamp,
                    file_hash: serializable.file_hash,
                    chunk_size: serializable.chunk_size,
                    compression_enabled: serializable.compression_enabled,
                })
            } else {
                None
            }
        } else {
            None
        }
    }

    async fn cleanup(&self, resume_dir: &Path) {
        let resume_file = resume_dir.join(format!("{}.resume", self.session_id));
        let _ = tokio::fs::remove_file(resume_file).await;
    }
}

// --- Memory Pool for chunk reuse ---
#[derive(Debug)]
struct ChunkPool {
    pool: Arc<Mutex<VecDeque<BytesMut>>>,
    chunk_size: usize,
    max_pool_size: usize,
}

impl ChunkPool {
    fn new(chunk_size: usize, max_pool_size: usize) -> Self {
        Self {
            pool: Arc::new(Mutex::new(VecDeque::with_capacity(max_pool_size))),
            chunk_size,
            max_pool_size,
        }
    }

    async fn get_buffer(&self) -> BytesMut {
        let mut pool = self.pool.lock().await;
        pool.pop_front()
            .unwrap_or_else(|| BytesMut::with_capacity(self.chunk_size))
    }

    async fn return_buffer(&self, mut buffer: BytesMut) {
        buffer.clear();
        let mut pool = self.pool.lock().await;
        if pool.len() < self.max_pool_size {
            pool.push_back(buffer);
        }
    }
}

// --- Network Statistics ---
#[derive(Debug, Clone)]
struct NetworkStats {
    rtt: Duration,
    bandwidth_bps: f64,
    packet_loss_rate: f64,
    jitter: Duration,
    last_updated: Instant,
    bytes_sent_interval: u64,
    last_sent_time: Instant,
}

impl NetworkStats {
    fn new() -> Self {
        Self {
            rtt: Duration::from_millis(50),
            bandwidth_bps: 1_000_000_000.0, // 1 Gbps default for HPC
            packet_loss_rate: 0.0,
            jitter: Duration::from_millis(5),
            last_updated: Instant::now(),
            bytes_sent_interval: 0,
            last_sent_time: Instant::now(),
        }
    }

    fn update_rtt(&mut self, new_rtt: Duration) {
        let alpha = 0.125; // Smoothing factor
        let old_rtt_ms = self.rtt.as_millis() as f64;
        let new_rtt_ms = new_rtt.as_millis() as f64;
        let smoothed_rtt_ms = alpha * new_rtt_ms + (1.0 - alpha) * old_rtt_ms;
        self.rtt = Duration::from_millis(smoothed_rtt_ms as u64);
        self.last_updated = Instant::now();
    }

    fn record_sent_bytes(&mut self, bytes: u66) {
        self.bytes_sent_interval += bytes;
        if self.last_sent_time.elapsed() >= Duration::from_secs(1) {
            self.update_bandwidth(self.bytes_sent_interval, self.last_sent_time.elapsed());
            self.bytes_sent_interval = 0;
            self.last_sent_time = Instant::now();
        }
    }

    fn update_bandwidth(&mut self, bytes_transferred: u64, duration: Duration) {
        if !duration.is_zero() {
            let new_bps = bytes_transferred as f64 / duration.as_secs_f64();
            let alpha = 0.2; // Smoothing factor
            self.bandwidth_bps = alpha * new_bps + (1.0 - alpha) * self.bandwidth_bps;
        }
    }

    fn update_packet_loss(&mut self, success_count: u32, total_attempts: u32) {
        if total_attempts > 0 {
            let new_loss = 1.0 - (success_count as f64 / total_attempts as f64);
            let alpha = 0.1; // Smoothing factor
            self.packet_loss_rate = alpha * new_loss + (1.0 - alpha) * self.packet_loss_rate;
        }
    }
}

// --- Adaptive Chunking System ---
#[derive(Debug, Clone)]
struct AdaptiveChunking {
    current_size: usize,
    min_size: usize,
    max_size: usize,
    congestion_window: f64,
    slow_start_threshold: usize,
    in_slow_start: bool,
    consecutive_successes: u32,
    consecutive_failures: u32,
    last_adjustment: Instant,
    network_stats: NetworkStats,
    // Add parameters for more aggressive scaling in HPC
    rtt_factor: f64, // How much RTT influences chunk size
    loss_factor: f64, // How much packet loss influences chunk size
}

impl AdaptiveChunking {
    fn new() -> Self {
        Self {
            current_size: BASE_CHUNK_SIZE,
            min_size: MIN_CHUNK_SIZE,
            max_size: MAX_CHUNK_SIZE,
            congestion_window: 1.0,
            slow_start_threshold: BASE_CHUNK_SIZE * 16, // More aggressive slow start
            in_slow_start: true,
            consecutive_successes: 0,
            consecutive_failures: 0,
            last_adjustment: Instant::now(),
            network_stats: NetworkStats::new(),
            rtt_factor: 1.0,
            loss_factor: 2.0,
        }
    }

    fn on_chunk_success(&mut self, transfer_time: Duration, chunk_size: usize) {
        self.consecutive_successes += 1;
        self.consecutive_failures = 0;

        self.network_stats.update_bandwidth(chunk_size as u64, transfer_time);
        self.network_stats.update_rtt(transfer_time); // Simplistic RTT update based on chunk time

        if self.should_adjust() {
            if self.in_slow_start {
                self.congestion_window += 1.0; // Exponential increase
                if self.congestion_window * self.current_size as f64 > self.slow_start_threshold as f64 {
                    self.in_slow_start = false;
                    info!("Exited slow start, cwnd: {}", self.congestion_window);
                }
            } else {
                self.congestion_window += 1.0 / self.congestion_window; // Additive increase
            }
            self.apply_adjustment();
        }
    }

    fn on_chunk_failure(&mut self) {
        self.consecutive_failures += 1;
        self.consecutive_successes = 0;

        self.network_stats.update_packet_loss(0, 1); // Indicate a loss event

        // Multiplicative decrease
        self.slow_start_threshold = (self.congestion_window * self.current_size as f64 / 2.0) as usize;
        self.congestion_window = (self.congestion_window * 0.75).max(1.0); // More aggressive decrease
        self.in_slow_start = false;
        
        warn!("Chunk failure detected, reducing congestion window to {:.2}", self.congestion_window);
        self.apply_adjustment();
    }

    fn apply_adjustment(&mut self) {
        let mut new_size = (self.congestion_window * BASE_CHUNK_SIZE as f64) as usize;

        // Further adjust based on network stats for HPC
        // If RTT is high, smaller chunks might reduce retransmission cost.
        // If bandwidth is low, larger chunks might reduce overhead.
        let estimated_optimal_bytes_per_rtt = self.network_stats.bandwidth_bps * self.network_stats.rtt.as_secs_f64() / 8.0; // Bytes per RTT
        let rtt_adjusted_size = estimated_optimal_bytes_per_rtt.max(self.min_size as f64) as usize;
        new_size = new_size.min(rtt_adjusted_size); // Cap based on RTT estimation

        // Adjust for packet loss: high loss -> smaller chunks
        if self.network_stats.packet_loss_rate > 0.01 { // If loss is significant
            new_size = (new_size as f64 * (1.0 - self.network_stats.packet_loss_rate * self.loss_factor)) as usize;
        }

        self.current_size = new_size.clamp(self.min_size, self.max_size);
        self.last_adjustment = Instant::now();
        debug!(
            "Adaptive chunk size adjusted to {} bytes (CWND: {:.2}, RTT: {:?}, BW: {:.2} Mbps, Loss: {:.2}%)",
            self.current_size,
            self.congestion_window,
            self.network_stats.rtt,
            self.network_stats.bandwidth_bps / 1_000_000.0,
            self.network_stats.packet_loss_rate * 100.0
        );
    }

    fn should_adjust(&self) -> bool {
        self.last_adjustment.elapsed() > Duration::from_millis(500) // Adjust more frequently
            && self.consecutive_successes > 5 // Only adjust after a few successful transfers
    }

    fn get_optimal_chunk_size(&self) -> usize {
        self.current_size
    }
}

// --- Connection Pool ---
struct ConnectionPool {
    pools: Arc<Mutex<HashMap<String, VecDeque<TcpStream>>>>,
    max_connections_per_host: usize,
    connection_timeout: Duration,
    tcp_nodelay: bool,
    tcp_send_buffer_size: Option<usize>,
    tcp_recv_buffer_size: Option<usize>,
}

impl ConnectionPool {
    fn new(
        max_connections_per_host: usize,
        connection_timeout: Duration,
        tcp_nodelay: bool,
        tcp_send_buffer_size: Option<usize>,
        tcp_recv_buffer_size: Option<usize>,
    ) -> Self {
        Self {
            pools: Arc::new(Mutex::new(HashMap::new())),
            max_connections_per_host,
            connection_timeout,
            tcp_nodelay,
            tcp_send_buffer_size,
            tcp_recv_buffer_size,
        }
    }

    async fn get_connection(&self, address: &str) -> Result<TcpStream, TransferError> {
        let mut pools = self.pools.lock().await;

        if let Some(pool) = pools.get_mut(address) {
            if let Some(stream) = pool.pop_front() {
                debug!("Reusing connection for {}", address);
                return Ok(stream);
            }
        }

        debug!("Creating new connection to {}", address);
        let stream = timeout(self.connection_timeout, TcpStream::connect(address)).await??;

        stream.set_nodelay(self.tcp_nodelay)?;
        if let Some(size) = self.tcp_send_buffer_size {
            stream.set_send_buffer_size(size)?;
        }
        if let Some(size) = self.tcp_recv_buffer_size {
            stream.set_recv_buffer_size(size)?;
        }

        Ok(stream)
    }

    async fn return_connection(&self, address: String, stream: TcpStream) {
        let mut pools = self.pools.lock().await;
        let pool = pools.entry(address).or_insert_with(VecDeque::new);

        if pool.len() < self.max_connections_per_host {
            pool.push_back(stream);
        } else {
            // If pool is full, connection is dropped
            debug!("Connection pool full for {}, dropping connection.", address);
        }
    }
}

// --- Transfer Session (Server Side) ---
struct TransferSession {
    session_id: String,
    filename: String,
    file_size: u64,
    chunks_received: HashMap<u64, FileChunk>,
    total_chunks: u64,
    progress_bar: Option<ProgressBar>,
    start_time: Instant,
    chunk_size: usize,
    completion_sender: Option<oneshot::Sender<Result<PathBuf, TransferError>>>,
    finalized: AtomicBool,
    end_markers_received: AtomicUsize,
    expected_end_markers: usize,
    resume_info: Arc<Mutex<ResumeInfo>>,
    ack_sender: mpsc::Sender<Message>, // Server sends ACKs back to client
    last_activity: AtomicU64,
    chunk_pool: Arc<ChunkPool>,
    bytes_received: AtomicU64,
    missing_chunks_set: Arc<Mutex<HashSet<u64>>>, // A set for quick lookups of missing chunks
    file_writer_semaphore: Arc<Semaphore>, // Limit concurrent writes to file
    output_dir: PathBuf,
}

impl TransferSession {
    fn new(
        session_id: String,
        filename: String,
        file_size: u64,
        enable_progress: bool,
        chunk_size: usize,
        completion_sender: oneshot::Sender<Result<PathBuf, TransferError>>,
        expected_streams: usize,
        initial_resume_info: Option<ResumeInfo>,
        ack_sender: mpsc::Sender<Message>,
        chunk_pool: Arc<ChunkPool>,
        output_dir: PathBuf,
    ) -> Self {
        let total_chunks = (file_size as f64 / chunk_size as f64).ceil() as u64;

        let progress_bar = if enable_progress {
            Some(ProgressBar::new(file_size))
        } else {
            None
        };

        if let Some(ref pb) = progress_bar {
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}) ETA: {eta} - {msg}")
                    .unwrap()
                    .progress_chars("#>-")
            );
            pb.set_message(format!("Receiving: {}", filename));
        }

        let resume_info = Arc::new(Mutex::new(
            initial_resume_info.unwrap_or_else(|| {
                ResumeInfo::new(session_id.clone(), filename.clone(), file_size, chunk_size)
            }),
        ));

        let missing_chunks_set = Arc::new(Mutex::new(
            (0..total_chunks).filter(|&i| {
                let resume_locked = resume_info.blocking_lock();
                i as usize >= resume_locked.chunk_bitmap.len() || !resume_locked.chunk_bitmap[i as usize]
            }).collect::<HashSet<_>>()
        ));

        let session = Self {
            session_id,
            filename,
            file_size,
            chunks_received: HashMap::new(),
            total_chunks,
            progress_bar,
            start_time: Instant::now(),
            chunk_size,
            completion_sender: Some(completion_sender),
            finalized: AtomicBool::new(false),
            end_markers_received: AtomicUsize::new(0),
            expected_end_markers: expected_streams,
            resume_info,
            ack_sender,
            last_activity: AtomicU64::new(
                SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs(),
            ),
            chunk_pool,
            bytes_received: AtomicU64::new(0),
            missing_chunks_set,
            file_writer_semaphore: Arc::new(Semaphore::new(DEFAULT_PARALLEL_STREAMS * 2)), // Allow more concurrent writes than streams
            output_dir,
        };

        // Initialize progress bar from resume info
        let bytes_transferred_at_start = session.resume_info.blocking_lock().bytes_transferred;
        session.bytes_received.store(bytes_transferred_at_start, Ordering::SeqCst);
        if let Some(ref pb) = session.progress_bar {
            pb.set_position(bytes_transferred_at_start);
        }

        session
    }

    #[instrument(skip(self, chunk), fields(chunk_id = %chunk.chunk_id, size = chunk.data.len()))]
    async fn add_chunk(&mut self, mut chunk: FileChunk) -> Result<bool, TransferError> {
        self.last_activity.store(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            Ordering::SeqCst,
        );

        // Check for duplicate
        if self.chunks_received.contains_key(&chunk.chunk_id) {
            debug!("Duplicate chunk {} received", chunk.chunk_id);
            return Ok(self.is_transfer_complete());
        }

        // Validate before decompressing/processing
        if !chunk.validate() {
            error!("Chunk {} validation failed", chunk.chunk_id);
            return Err(TransferError::ChunkValidationFailed { chunk_id: chunk.chunk_id });
        }

        // Decompress if needed
        chunk.decompress()?;

        let chunk_data_size = chunk.data.len();
        let chunk_id = chunk.chunk_id;
        self.chunks_received.insert(chunk_id, chunk);

        // Update progress and resume info
        self.bytes_received.fetch_add(chunk_data_size as u64, Ordering::SeqCst);
        self.resume_info.lock().await.mark_chunk_received(chunk_id, chunk_data_size);
        self.missing_chunks_set.lock().await.remove(&chunk_id);

        if let Some(ref pb) = self.progress_bar {
            pb.set_position(self.bytes_received.load(Ordering::SeqCst));
        }

        debug!(
            "Chunk {} added successfully, total received chunks: {}",
            chunk_id,
            self.chunks_received.len()
        );

        Ok(self.is_transfer_complete())
    }

    fn handle_end_marker(&self) -> bool {
        let markers_received = self.end_markers_received.fetch_add(1, Ordering::SeqCst) + 1;
        let bytes_received = self.bytes_received.load(Ordering::SeqCst);

        info!(
            "End marker received ({}/{}), bytes: {}/{}",
            markers_received, self.expected_end_markers, bytes_received, self.file_size
        );

        let has_all_data = bytes_received >= self.file_size;
        let has_all_markers = markers_received >= self.expected_end_markers;

        has_all_data && has_all_markers && self.missing_chunks_set.lock().await.is_empty()
    }

    fn is_transfer_complete(&self) -> bool {
        let bytes_received = self.bytes_received.load(Ordering::SeqCst);
        let all_chunks_accounted_for = self.missing_chunks_set.blocking_lock().is_empty();
        bytes_received >= self.file_size && all_chunks_accounted_for
    }

    async fn get_missing_chunks(&self) -> Vec<u64> {
        self.missing_chunks_set.lock().await.iter().cloned().collect()
    }

    #[instrument(skip(self), fields(session_id = %self.session_id, filename = %self.filename))]
    async fn write_to_file(&self) -> Result<PathBuf, TransferError> {
        info!("Writing file: {}", self.filename);

        let filepath = self.output_dir.join(&self.filename);
        let mut file = File::create(&filepath).await?;

        let mut chunk_ids: Vec<u64> = self.chunks_received.keys().cloned().collect();
        chunk_ids.sort_unstable(); // Use unstable sort for potential speedup if elements are often in order

        let mut total_written = 0usize;
        let mut file_hasher = Sha256::new();

        // Write chunks in order
        for chunk_id in chunk_ids {
            // Re-acquire lock for each chunk, or make `chunks_received` an Arc<Mutex>
            // For now, assume this is called after all chunks are received and no more modifications happen
            if let Some(chunk) = self.chunks_received.get(&chunk_id) {
                file.write_all(&chunk.data).await?;
                file_hasher.update(&chunk.data);
                total_written += chunk.data.len();
            } else {
                error!("Missing chunk {} during file write!", chunk_id);
                return Err(TransferError::Io(io::Error::new(io::ErrorKind::NotFound, format!("Chunk {} not found for writing", chunk_id))));
            }
        }

        file.flush().await?;

        let file_hash = file_hasher.finalize();
        let duration = self.start_time.elapsed();
        let throughput = (total_written as f64) / (1024.0 * 1024.0) / duration.as_secs_f64();

        info!(
            "Transfer completed: {} ({:.2} MB/s, SHA256: {:x})",
            self.filename, throughput, file_hash
        );

        if let Some(ref pb) = self.progress_bar {
            pb.finish_with_message(format!("✅ {} ({:.2} MB/s)", self.filename, throughput));
        }

        self.resume_info.lock().await.file_hash = Some(file_hash.into());
        self.resume_info.lock().await.cleanup(&self.output_dir.parent().unwrap().join("resume_server")).await;

        Ok(filepath)
    }

    async fn send_acknowledgment(&self) {
        let resume_info = self.resume_info.lock().await;
        let received_chunks: Vec<u64> = resume_info.chunk_bitmap.iter()
            .enumerate()
            .filter(|(_, &received)| received)
            .map(|(idx, _)| idx as u64)
            .collect();
        let missing_chunks = resume_info.get_missing_chunks();

        let ack = Message::ChunkAck {
            session_id: self.session_id.clone(),
            received_chunks,
            missing_chunks,
            timestamp: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs(),
        };

        if let Err(e) = self.ack_sender.send(ack).await {
            warn!("Failed to send acknowledgment for session {}: {}", self.session_id, e);
        }
    }
}

// --- Retry System ---
#[derive(Clone)]
struct RetryManager {
    max_retries: u32,
    base_delay: Duration,
    max_delay: Duration,
}

impl RetryManager {
    fn new(max_retries: u32) -> Self {
        Self {
            max_retries,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(60), // Longer max delay for HPC
        }
    }

    async fn execute_with_retry<F, T, E>(&self, mut operation: F) -> Result<T, E>
    where
        F: FnMut() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, E>> + Send>>,
        E: std::fmt::Debug + Send + 'static,
    {
        for attempt in 0..=self.max_retries {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(e) if attempt < self.max_retries => {
                    let delay = std::cmp::min(
                        self.base_delay * 2_u32.pow(attempt),
                        self.max_delay,
                    );
                    debug!(
                        "Retry attempt {} failed: {:?}, waiting {:?}",
                        attempt + 1,
                        e,
                        delay
                    );
                    tokio::time::sleep(delay).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!()
    }
}

// --- Server Implementation ---
#[instrument(skip(stream, sessions, config, chunk_pool, ack_tx))]
async fn handle_server_stream(
    stream: TcpStream,
    sessions: Arc<RwLock<HashMap<String, Arc<Mutex<TransferSession>>>>>,
    config: Arc<ServerConfig>,
    chunk_pool: Arc<ChunkPool>,
    ack_tx: mpsc::Sender<Message>, // To send ACKs back to the client
) -> Result<(), TransferError> {
    let peer_addr = stream.peer_addr()?;
    info!("Handling new stream from: {}", peer_addr);

    let buffer_size = config.buffer_size.unwrap_or(DEFAULT_BUFFER_SIZE);
    let mut reader = BufReader::with_capacity(buffer_size, stream.try_clone()?);
    let mut writer = BufWriter::with_capacity(buffer_size, stream);

    let mut current_session_id: Option<String> = None;

    // Handle incoming messages
    loop {
        let msg = timeout(CONNECTION_TIMEOUT, Message::read_from_stream(&mut reader)).await;

        let msg = match msg {
            Ok(Ok(Some(msg))) => msg,
            Ok(Ok(None)) => {
                debug!("Stream closed gracefully by client {}", peer_addr);
                break; // Stream closed
            }
            Ok(Err(e)) => {
                warn!("Error reading message from {}: {}", peer_addr, e);
                // Send an error message back if possible before breaking
                if let Some(session_id) = current_session_id.clone() {
                    let _ = Message::Error {
                        session_id,
                        message: format!("Protocol error: {}", e),
                    }.write_to_stream(&mut writer).await;
                }
                break;
            }
            Err(e) => {
                warn!("Timeout reading message from {}: {}", peer_addr, e);
                break;
            }
        };

        match msg {
            Message::Handshake {
                session_id,
                filename,
                file_size,
                chunk_size,
                parallel_streams,
                compression_enabled,
                auth_token,
                resume_info: client_resume_info_serialized,
            } => {
                info!(
                    "Received handshake for session {} (File: {}, Size: {}, Streams: {})",
                    session_id, filename, file_size, parallel_streams
                );

                if let Some(expected_token) = &config.security.as_ref().and_then(|s| s.auth_token.clone()) {
                    if auth_token.as_ref() != Some(expected_token) {
                        error!("Authentication failed for session {}", session_id);
                        Message::Error {
                            session_id,
                            message: "Authentication failed".to_string(),
                        }.write_to_stream(&mut writer).await?;
                        break;
                    }
                }

                current_session_id = Some(session_id.clone());

                let (completion_tx, completion_rx) = oneshot::channel();
                let output_dir = PathBuf::from(&config.output_directory);
                create_dir_all(&output_dir).await?;

                let resume_dir = PathBuf::from(config.resume_directory.as_ref().unwrap_or(&"./resume_server".to_string()));
                let server_resume_info = ResumeInfo::load(&session_id, &resume_dir).await;

                // Merge client and server resume info if both exist and are compatible
                let final_resume_info = match (server_resume_info, client_resume_info_serialized) {
                    (Some(mut server_ri), Some(client_ri_serial)) => {
                        let client_ri: ResumeInfo = client_ri_serial.into(); // Convert from Serializable
                        if server_ri.session_id == client_ri.session_id &&
                           server_ri.filename == client_ri.filename &&
                           server_ri.file_size == client_ri.file_size &&
                           server_ri.chunk_size == client_ri.chunk_size {
                            info!("Resuming session {} with merged resume info.", session_id);
                            // Merge bitmaps: if either has a chunk, it's considered received
                            for i in 0..server_ri.chunk_bitmap.len().min(client_ri.chunk_bitmap.len()) {
                                server_ri.chunk_bitmap[i] = server_ri.chunk_bitmap[i] || client_ri.chunk_bitmap[i];
                            }
                            server_ri.bytes_transferred = server_ri.chunk_bitmap.iter().filter(|&&b|b).count() as u64 * chunk_size as u64; // Recalculate
                            Some(server_ri)
                        } else {
                            warn!("Client provided incompatible resume info for session {}. Starting new transfer.", session_id);
                            None
                        }
                    },
                    (Some(server_ri), None) => {
                        info!("Resuming session {} with server-side resume info.", session_id);
                        Some(server_ri)
                    },
                    (None, Some(_)) => { // Client has resume info, but server doesn't
                        warn!("Client provided resume info for session {}, but server has none. Starting new transfer.", session_id);
                        // For simplicity, we ignore client's resume if server doesn't have it for this example.
                        // In a real system, you might trust the client or request verification.
                        None
                    },
                    (None, None) => None,
                };
                
                let session = Arc::new(Mutex::new(TransferSession::new(
                    session_id.clone(),
                    filename.clone(),
                    file_size,
                    config.enable_progress_bar,
                    chunk_size,
                    completion_tx,
                    parallel_streams,
                    final_resume_info,
                    ack_tx.clone(),
                    chunk_pool.clone(),
                    output_dir.clone(),
                )));

                sessions.write().await.insert(session_id.clone(), session.clone());

                // Spawn a task to manage ACKs for this session
                let ack_session_id = session_id.clone();
                let ack_writer = Arc::new(Mutex::new(writer)); // Wrap writer in Arc<Mutex> for sharing
                let session_arc = session.clone();
                tokio::spawn(async move {
                    let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
                    loop {
                        interval.tick().await;
                        let session_guard = session_arc.lock().await;
                        if session_guard.finalized.load(Ordering::SeqCst) {
                            debug!("ACK sender for session {} shutting down (finalized).", ack_session_id);
                            break;
                        }
                        session_guard.send_acknowledgment().await;
                        // Also send a heartbeat for liveness
                        let _ = Message::Heartbeat {
                            session_id: ack_session_id.clone(),
                            timestamp: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs(),
                        }.write_to_stream(&mut ack_writer.lock().await).await;
                    }
                });

                // The writer is now owned by the ACK sender task, so we break this stream's loop.
                // This stream handler is only for receiving data chunks.
                break;
            }
            Message::FileChunk {
                chunk_id,
                session_id,
                data,
                is_last,
                is_compressed,
                checksum,
                original_size,
                sequence_number,
                retry_count,
            } => {
                let sessions_read_guard = sessions.read().await;
                if let Some(session_arc) = sessions_read_guard.get(&session_id) {
                    let mut session = session_arc.lock().await;
                    let chunk = FileChunk {
                        chunk_id,
                        session_id,
                        data,
                        is_last,
                        is_compressed,
                        checksum,
                        original_size,
                        sequence_number,
                        retry_count,
                    };
                    match session.add_chunk(chunk).await {
                        Ok(is_complete) => {
                            if is_complete {
                                info!("Session {} transfer is complete by chunk data.", session.session_id);
                                if !session.finalized.swap(true, Ordering::SeqCst) {
                                    let result = session.write_to_file().await;
                                    if let Some(completion_sender) = session.completion_sender.take() {
                                        let _ = completion_sender.send(result);
                                    }
                                }
                            }
                        }
                        Err(e) => error!("Failed to add chunk to session {}: {}", session.session_id, e),
                    }
                } else {
                    warn!("Received chunk for unknown session: {}", session_id);
                }
            }
            Message::EndOfStream { session_id, stream_id } => {
                info!("End of stream marker received for session {}, stream {}", session_id, stream_id);
                let sessions_read_guard = sessions.read().await;
                if let Some(session_arc) = sessions_read_guard.get(&session_id) {
                    let mut session = session_arc.lock().await;
                    if session.handle_end_marker() && !session.finalized.swap(true, Ordering::SeqCst) {
                        info!("All end markers received and data complete for session {}. Finalizing...", session_id);
                        let result = session.write_to_file().await;
                        if let Some(completion_sender) = session.completion_sender.take() {
                            let _ = completion_sender.send(result);
                        }
                    }
                }
            }
            Message::ChunkAck { .. } | Message::Error { .. } | Message::Heartbeat { .. } => {
                warn!("Server received unexpected message type: {:?}", msg);
            }
        }
    }
    Ok(())
}


// --- Client Implementation ---

// Enum for client-side task communication
#[derive(Debug)]
enum ClientTaskMessage {
    ChunkToSend(FileChunk),
    ChunkAck(ChunkAck),
    Error(String),
    Heartbeat,
    SessionCompleted,
}

#[instrument(skip(reader, writer, sender, receiver, session_id, adaptive_chunking_mutex))]
async fn client_stream_worker(
    mut reader: BufReader<TcpStream>,
    mut writer: BufWriter<TcpStream>,
    sender: mpsc::Sender<ClientTaskMessage>,
    mut receiver: mpsc::Receiver<FileChunk>,
    session_id: String,
    stream_id: usize,
    adaptive_chunking_mutex: Arc<Mutex<AdaptiveChunking>>,
) -> Result<(), TransferError> {
    let mut sequence_number = 0;
    let mut in_flight_chunks: HashMap<u64, (Instant, FileChunk)> = HashMap::new();
    let retry_manager = RetryManager::new(MAX_RETRIES);

    let max_in_flight_bytes = MAX_CONCURRENT_CHUNKS * BASE_CHUNK_SIZE; // Simple window size in bytes
    let mut current_in_flight_bytes = 0usize;

    loop {
        tokio::select! {
            // Try to send a chunk if available and window allows
            chunk_opt = receiver.recv(), if current_in_flight_bytes < max_in_flight_bytes => {
                if let Some(mut chunk) = chunk_opt {
                    if chunk.retry_count > MAX_RETRIES {
                        error!("Max retries reached for chunk {}", chunk.chunk_id);
                        let _ = sender.send(ClientTaskMessage::Error(format!("Max retries for chunk {}", chunk.chunk_id))).await;
                        break;
                    }

                    chunk.sequence_number = sequence_number;
                    sequence_number += 1;

                    // Compress if enabled
                    if chunk.original_size > 0 && chunk.data.len() > COMPRESSION_THRESHOLD { // Only try to compress if it has data
                         if let Err(e) = chunk.compress() {
                            warn!("Failed to compress chunk {}: {}", chunk.chunk_id, e);
                            // Continue without compression if it fails
                         }
                    }

                    debug!("Stream {}: Sending chunk {} (seq: {}) with size {} (orig: {})",
                           stream_id, chunk.chunk_id, chunk.sequence_number, chunk.data.len(), chunk.original_size);

                    let msg = Message::FileChunk {
                        chunk_id: chunk.chunk_id,
                        session_id: session_id.clone(),
                        data: chunk.data.clone(), // Clone Bytes for send
                        is_last: chunk.is_last,
                        is_compressed: chunk.is_compressed,
                        checksum: chunk.checksum,
                        original_size: chunk.original_size,
                        sequence_number: chunk.sequence_number,
                        retry_count: chunk.retry_count,
                    };

                    let chunk_byte_len = chunk.data.len();
                    let send_result = retry_manager.execute_with_retry(|| {
                        let mut temp_writer = writer.by_ref();
                        let msg_clone = msg.clone(); // Clone for retry
                        Box::pin(async move {
                            timeout(CHUNK_TIMEOUT, msg_clone.write_to_stream(&mut temp_writer)).await?
                                .map_err(|e| TransferError::Io(e.into()))
                        })
                    }).await;

                    match send_result {
                        Ok(_) => {
                            in_flight_chunks.insert(chunk.chunk_id, (Instant::now(), chunk));
                            current_in_flight_bytes += chunk_byte_len;
                            debug!("Stream {}: Chunk {} (seq: {}) sent successfully. In-flight: {} bytes.", stream_id, chunk.chunk_id, sequence_number - 1, current_in_flight_bytes);
                        },
                        Err(e) => {
                            error!("Stream {}: Failed to send chunk {} after retries: {}", stream_id, chunk.chunk_id, e);
                            let _ = sender.send(ClientTaskMessage::Error(format!("Failed to send chunk {}: {}", chunk.chunk_id, e))).await;
                            break;
                        }
                    }
                } else {
                    // Receiver closed, no more chunks from main client logic
                    debug!("Stream {}: Chunk sender closed. Sending EndOfStream.", stream_id);
                    let end_msg = Message::EndOfStream { session_id: session_id.clone(), stream_id };
                    retry_manager.execute_with_retry(|| {
                        let mut temp_writer = writer.by_ref();
                        let end_msg_clone = end_msg.clone();
                        Box::pin(async move {
                            timeout(CHUNK_TIMEOUT, end_msg_clone.write_to_stream(&mut temp_writer)).await?
                                .map_err(|e| TransferError::Io(e.into()))
                        })
                    }).await?;
                    debug!("Stream {}: EndOfStream sent. Shutting down worker.", stream_id);
                    break;
                }
            },
            // Process incoming ACKs/Errors/Heartbeats
            msg_opt = timeout(HEARTBEAT_INTERVAL * 2, Message::read_from_stream(&mut reader)) => {
                match msg_opt {
                    Ok(Ok(Some(Message::ChunkAck { session_id: ack_sid, received_chunks, missing_chunks, timestamp: _ }))) => {
                        if ack_sid == session_id {
                            debug!("Stream {}: Received ACK for session {}. Received: {}, Missing: {}",
                                   stream_id, ack_sid, received_chunks.len(), missing_chunks.len());
                            let _ = sender.send(ClientTaskMessage::ChunkAck(ChunkAck {
                                session_id: ack_sid,
                                received_chunks,
                                missing_chunks,
                                timestamp: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs(),
                            })).await;

                            // Update in-flight counts and adaptive chunking based on successful ACKs
                            let mut adaptive_chunking = adaptive_chunking_mutex.lock().await;
                            for chunk_id in &received_chunks {
                                if let Some((send_time, chunk)) = in_flight_chunks.remove(chunk_id) {
                                    current_in_flight_bytes = current_in_flight_bytes.saturating_sub(chunk.data.len());
                                    adaptive_chunking.on_chunk_success(send_time.elapsed(), chunk.data.len());
                                }
                            }
                        }
                    },
                    Ok(Ok(Some(Message::Error { session_id: err_sid, message }))) => {
                        if err_sid == session_id {
                            error!("Stream {}: Server error for session {}: {}", stream_id, err_sid, message);
                            let _ = sender.send(ClientTaskMessage::Error(message)).await;
                            break;
                        }
                    },
                    Ok(Ok(Some(Message::Heartbeat { session_id: hb_sid, timestamp: _ }))) => {
                        if hb_sid == session_id {
                            debug!("Stream {}: Received heartbeat from server for session {}", stream_id, hb_sid);
                            let _ = sender.send(ClientTaskMessage::Heartbeat).await;
                        }
                    },
                    Ok(Ok(Some(other_msg))) => {
                        warn!("Stream {}: Received unexpected message type from server: {:?}", stream_id, other_msg);
                    },
                    Ok(Ok(None)) => {
                        info!("Stream {}: Server closed connection gracefully.", stream_id);
                        break;
                    },
                    Ok(Err(e)) => {
                        warn!("Stream {}: Error reading from server: {}", stream_id, e);
                        let _ = sender.send(ClientTaskMessage::Error(format!("Connection error: {}", e))).await;
                        break;
                    },
                    Err(_) => { // Timeout on read
                        debug!("Stream {}: Read timeout, sending heartbeat to check liveness or if server has more ACKs.", stream_id);
                        // No explicit message to send here, client continues to try to send chunks
                    }
                }
            }
        }
    }
    Ok(())
}

struct ClientTransferManager {
    config: Arc<ClientConfig>,
    retry_manager: RetryManager,
    chunk_pool: Arc<ChunkPool>,
    multi_progress: MultiProgress,
    // Senders for distributing chunks to parallel streams
    chunk_senders: Vec<mpsc::Sender<FileChunk>>,
    // Receiver for all messages from parallel streams (ACk, Errors, Heartbeats)
    client_task_receiver: mpsc::Receiver<ClientTaskMessage>,
    // Track in-flight chunks for retransmission
    in_flight_chunks: Arc<RwLock<HashMap<u64, FileChunk>>>,
    // Global tracking of missing chunks
    missing_chunks: Arc<Mutex<HashSet<u64>>>,
    // Session state
    session_id: String,
    file_path: PathBuf,
    file_size: u64,
    current_chunk_size: Arc<AtomicUsize>,
    resume_info: Arc<Mutex<ResumeInfo>>,
    adaptive_chunking: Arc<Mutex<AdaptiveChunking>>,
    bytes_transferred: Arc<AtomicU64>,
    progress_bar: Option<ProgressBar>,
    start_time: Instant,
    total_chunks: u64,
}

impl ClientTransferManager {
    async fn new(
        file_path: PathBuf,
        config: Arc<ClientConfig>,
        multi_progress: MultiProgress,
    ) -> Result<Self, TransferError> {
        let file_metadata = metadata(&file_path).await?;
        let file_size = file_metadata.len();
        let session_id = Uuid::new_v4().to_string();
        let actual_parallel_streams = config.parallel_streams.unwrap_or(DEFAULT_PARALLEL_STREAMS);
        let actual_chunk_size = BASE_CHUNK_SIZE; // Initial chunk size, adaptive will change it

        let resume_dir = PathBuf::from(config.resume_directory.as_ref().unwrap_or(&"./resume_client".to_string()));
        create_dir_all(&resume_dir).await?;

        let initial_resume_info = if config.auto_resume.unwrap_or(false) {
            ResumeInfo::load(&session_id, &resume_dir).await
        } else {
            None
        };

        let resume_info = Arc::new(Mutex::new(
            initial_resume_info.unwrap_or_else(|| {
                ResumeInfo::new(session_id.clone(), file_path.file_name().unwrap().to_string_lossy().into_owned(), file_size, actual_chunk_size)
            })
        ));

        let current_bytes_transferred = resume_info.lock().await.bytes_transferred;

        let total_chunks = (file_size as f64 / actual_chunk_size as f64).ceil() as u64;

        let missing_chunks: HashSet<u64> = resume_info.lock().await.get_missing_chunks().into_iter().collect();
        let missing_chunks = Arc::new(Mutex::new(missing_chunks));

        let (client_task_tx, client_task_rx) = mpsc::channel(actual_parallel_streams * 2); // Buffer for ACKs/Errors from workers

        let mut chunk_senders = Vec::with_capacity(actual_parallel_streams);
        for _ in 0..actual_parallel_streams {
            let (tx, _rx) = mpsc::channel(MAX_CONCURRENT_CHUNKS); // Each worker has its own receive channel
            chunk_senders.push(tx);
        }

        let progress_bar = if config.enable_progress_bar {
            let pb = multi_progress.add(ProgressBar::new(file_size));
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}) ETA: {eta} - {msg}")
                    .unwrap()
                    .progress_chars("#>-")
            );
            pb.set_message(format!("Sending: {}", file_path.display()));
            pb.set_position(current_bytes_transferred);
            Some(pb)
        } else {
            None
        };

        Ok(Self {
            config,
            retry_manager: RetryManager::new(MAX_RETRIES),
            chunk_pool: Arc::new(ChunkPool::new(
                MAX_CHUNK_SIZE,
                MEMORY_POOL_SIZE,
            )),
            multi_progress,
            chunk_senders,
            client_task_receiver: client_task_rx,
            in_flight_chunks: Arc::new(RwLock::new(HashMap::new())),
            missing_chunks,
            session_id,
            file_path,
            file_size,
            current_chunk_size: Arc::new(AtomicUsize::new(actual_chunk_size)),
            resume_info,
            adaptive_chunking: Arc::new(Mutex::new(AdaptiveChunking::new())),
            bytes_transferred: Arc::new(AtomicU64::new(current_bytes_transferred)),
            progress_bar,
            start_time: Instant::now(),
            total_chunks,
        })
    }

    async fn run_transfer(&mut self) -> Result<(), TransferError> {
        info!(
            "Starting client transfer for session {}",
            self.session_id
        );

        let actual_parallel_streams = self.config.parallel_streams.unwrap_or(DEFAULT_PARALLEL_STREAMS);
        let current_tcp_nodelay = self.config.tcp_nodelay.or(self.config.performance.as_ref().and_then(|p| p.tcp_nodelay)).unwrap_or(true);
        let current_tcp_send_buffer_size = self.config.tcp_send_buffer_size.or(self.config.performance.as_ref().and_then(|p| p.tcp_send_buffer_size));
        let current_tcp_recv_buffer_size = self.config.tcp_recv_buffer_size.or(self.config.performance.as_ref().and_then(|p| p.tcp_recv_buffer_size));

        let conn_pool = Arc::new(ConnectionPool::new(
            actual_parallel_streams,
            CONNECTION_TIMEOUT,
            current_tcp_nodelay,
            current_tcp_send_buffer_size,
            current_tcp_recv_buffer_size,
        ));

        // Spawn parallel stream workers
        let mut worker_handles = Vec::new();
        for stream_id in 0..actual_parallel_streams {
            let stream = conn_pool.get_connection(&self.config.server_address).await?;
            let (read_half, write_half) = tokio::io::split(stream);
            let reader = BufReader::with_capacity(self.config.buffer_size.unwrap_or(DEFAULT_BUFFER_SIZE), read_half);
            let writer = BufWriter::with_capacity(self.config.buffer_size.unwrap_or(DEFAULT_BUFFER_SIZE), write_half);

            let (chunk_tx, chunk_rx) = mpsc::channel(MAX_CONCURRENT_CHUNKS);
            self.chunk_senders[stream_id] = chunk_tx; // Replace the placeholder sender

            let worker_sender = self.client_task_receiver.sender.clone(); // Clone for worker
            let session_id_clone = self.session_id.clone();
            let adaptive_chunking_clone = self.adaptive_chunking.clone();

            let handle = tokio::spawn(async move {
                client_stream_worker(
                    reader,
                    writer,
                    worker_sender,
                    chunk_rx,
                    session_id_clone,
                    stream_id,
                    adaptive_chunking_clone,
                )
                .await
            });
            worker_handles.push(handle);
        }

        // Handshake with server (send on one stream, or a dedicated control stream)
        // For simplicity, we'll assume one of the existing streams is used for handshake.
        // In a real HPC system, a dedicated control connection might be preferred.
        let handshake_stream = conn_pool.get_connection(&self.config.server_address).await?;
        let (mut hs_reader, mut hs_writer) = tokio::io::split(handshake_stream);
        let mut hs_writer_buf = BufWriter::with_capacity(self.config.buffer_size.unwrap_or(DEFAULT_BUFFER_SIZE), hs_writer);

        let client_resume_info_serialized: Option<SerializableResumeInfo> = {
            let ri_guard = self.resume_info.lock().await;
            if ri_guard.bytes_transferred > 0 {
                Some(SerializableResumeInfo {
                    session_id: ri_guard.session_id.clone(),
                    filename: ri_guard.filename.clone(),
                    file_size: ri_guard.file_size,
                    chunk_bitmap: ri_guard.chunk_bitmap.clone(),
                    last_chunk_id: ri_guard.last_chunk_id,
                    bytes_transferred: ri_guard.bytes_transferred,
                    timestamp: ri_guard.timestamp,
                    file_hash: ri_guard.file_hash,
                    chunk_size: ri_guard.chunk_size,
                    compression_enabled: ri_guard.compression_enabled,
                })
            } else {
                None
            }
        };

        let handshake_msg = Message::Handshake {
            session_id: self.session_id.clone(),
            filename: self.file_path.file_name().unwrap().to_string_lossy().into_owned(),
            file_size: self.file_size,
            chunk_size: self.current_chunk_size.load(Ordering::SeqCst),
            parallel_streams: actual_parallel_streams,
            compression_enabled: self.config.compression_enabled.unwrap_or(true),
            auth_token: self.config.security.as_ref().and_then(|s| s.auth_token.clone()),
            resume_info: client_resume_info_serialized,
        };

        self.retry_manager.execute_with_retry(|| {
            let hs_msg_clone = handshake_msg.clone();
            let mut writer_ref = hs_writer_buf.by_ref();
            Box::pin(async move {
                timeout(CONNECTION_TIMEOUT, hs_msg_clone.write_to_stream(&mut writer_ref)).await?
                    .map_err(|e| TransferError::Io(e.into()))
            })
        }).await?;

        info!("Handshake sent for session {}", self.session_id);
        // Do NOT close hs_writer_buf, it's still connected.
        // It's part of the connection pool's stream, handle it explicitly.

        // Main client loop for sending chunks and processing ACKs
        let mut file = File::open(&self.file_path).await?;
        let mut offset = 0u64;
        let mut next_chunk_id = 0u64;
        let mut active_workers = actual_parallel_streams as usize;
        let mut server_finalized = AtomicBool::new(false);

        // Periodically save resume info
        let resume_info_clone = self.resume_info.clone();
        let session_id_clone = self.session_id.clone();
        let save_resume_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(RESUME_INFO_SAVE_INTERVAL);
            loop {
                interval.tick().await;
                let resume_dir = PathBuf::from(
                    env::current_dir()
                        .unwrap()
                        .join("resume_client")
                );
                let resume_guard = resume_info_clone.lock().await;
                if let Err(e) = resume_guard.save(&resume_dir).await {
                    error!("Failed to save resume info for session {}: {}", session_id_clone, e);
                }
            }
        });


        // Producer loop: read chunks from file and distribute to workers
        let producer_handle = tokio::spawn({
            let file_path_clone = self.file_path.clone();
            let chunk_senders_clone = self.chunk_senders.clone();
            let chunk_pool_clone = self.chunk_pool.clone();
            let missing_chunks_clone = self.missing_chunks.clone();
            let current_chunk_size_clone = self.current_chunk_size.clone();
            let session_id_clone_prod = self.session_id.clone();
            let compression_enabled = self.config.compression_enabled.unwrap_or(true);
            let file_size_clone = self.file_size;

            async move {
                let mut local_file = File::open(&file_path_clone).await?;
                let mut current_offset = 0u64;
                let mut next_chunk_id_producer = 0u64;
                let mut missing_chunks_prod = missing_chunks_clone.lock().await;

                loop {
                    let chunk_size_to_read = current_chunk_size_clone.load(Ordering::SeqCst);
                    let end_of_file = current_offset >= file_size_clone;
                    let is_last_chunk_overall = (current_offset + chunk_size_to_read as u64) >= file_size_clone;

                    // If we have missing chunks, prioritize them
                    let chunk_to_send_id: Option<u64> = if !missing_chunks_prod.is_empty() {
                        // Pick a missing chunk
                        missing_chunks_prod.iter().next().cloned()
                    } else if end_of_file {
                        None
                    } else {
                        // Regular chunk
                        Some(next_chunk_id_producer)
                    };

                    if let Some(id_to_send) = chunk_to_send_id {
                        let chunk_offset = id_to_send * chunk_size_to_read as u64;
                        let chunk_len = (file_size_clone - chunk_offset).min(chunk_size_to_read as u64) as usize;

                        if chunk_len == 0 { // No more data to send for this chunk
                            next_chunk_id_producer += 1;
                            missing_chunks_prod.remove(&id_to_send); // Ensure it's removed if empty
                            continue;
                        }

                        let mut buffer = chunk_pool_clone.get_buffer().await;
                        buffer.resize(chunk_len, 0);

                        local_file.seek(tokio::io::SeekFrom::Start(chunk_offset)).await?;
                        let bytes_read = local_file.read_exact(&mut buffer).await?;
                        if bytes_read == 0 {
                            // Should not happen unless file shrinks or seek is wrong
                            error!("Producer: Read 0 bytes for chunk {} at offset {}", id_to_send, chunk_offset);
                            missing_chunks_prod.remove(&id_to_send);
                            continue;
                        }
                        
                        let mut chunk = FileChunk::new(
                            id_to_send,
                            session_id_clone_prod.clone(),
                            buffer.freeze(), // Freeze to Bytes for immutable sharing
                            is_last_chunk_overall, // This is true if this is the last chunk in the file
                            0, // Sequence number will be set by worker
                        );
                        if compression_enabled {
                            if let Err(e) = chunk.compress() {
                                warn!("Producer: Failed to compress chunk {}: {}", chunk.chunk_id, e);
                            }
                        }

                        let worker_idx = (id_to_send as usize) % chunk_senders_clone.len();
                        if let Err(e) = chunk_senders_clone[worker_idx].send(chunk).await {
                            error!("Producer: Failed to send chunk {} to worker: {}", id_to_send, e);
                            // If worker channel closed, maybe worker crashed, try next time
                            break;
                        }
                        missing_chunks_prod.remove(&id_to_send); // Optimistically remove
                        next_chunk_id_producer += 1; // Only increment for regular chunks, not missing ones

                        current_offset += bytes_read as u64; // Only update offset for sequential reading logic
                    } else {
                        // All chunks have been sent (either initially or as retransmissions)
                        // Wait for ACKs to confirm full completion
                        if missing_chunks_prod.is_empty() && end_of_file {
                            debug!("Producer: All chunks initially sent or requested. Waiting for completion.");
                            break; // All chunks are in flight or received
                        }
                        sleep(Duration::from_millis(50)).await; // Wait a bit if no chunks to send
                    }
                }
                Ok::<(), TransferError>(())
            }
        });


        // Consumer loop: process messages from workers
        let mut total_completion_markers_expected = actual_parallel_streams;
        let mut completed_workers = 0;
        let mut server_errors = false;

        loop {
            tokio::select! {
                Some(msg) = self.client_task_receiver.recv() => {
                    match msg {
                        ClientTaskMessage::ChunkAck(ack) => {
                            let mut missing_chunks_guard = self.missing_chunks.lock().await;
                            for chunk_id in ack.received_chunks {
                                if missing_chunks_guard.remove(&chunk_id) {
                                    let mut resume_info_guard = self.resume_info.lock().await;
                                    let chunk_len = resume_info_guard.chunk_size; // Approximation, actual len is in chunk
                                    resume_info_guard.mark_chunk_received(chunk_id, chunk_len);
                                    self.bytes_transferred.store(resume_info_guard.bytes_transferred, Ordering::SeqCst);
                                    if let Some(ref pb) = self.progress_bar {
                                        pb.set_position(self.bytes_transferred.load(Ordering::SeqCst));
                                    }
                                }
                            }
                            // Re-queue missing chunks for retransmission
                            for missing_id in ack.missing_chunks {
                                warn!("Chunk {} reported missing by server. Re-queuing.", missing_id);
                                self.missing_chunks.lock().await.insert(missing_id);
                                // The producer loop will pick this up on its next iteration.
                                // If specific worker retransmission is needed, this would go through
                                // a channel to a specific worker. For simplicity, producer redistributes.
                            }

                            // Adaptive chunking updates based on ACK
                            // This would ideally be done within the worker, which has RTT for individual chunks.
                            // Here, we have aggregated ACKs, so updates are coarser.
                            self.adaptive_chunking.lock().await.network_stats.update_rtt(self.start_time.elapsed() / self.bytes_transferred.load(Ordering::SeqCst).max(1) as u32); // Very rough RTT estimate
                        },
                        ClientTaskMessage::Error(e) => {
                            error!("Client worker reported error: {}", e);
                            server_errors = true;
                            break;
                        },
                        ClientTaskMessage::Heartbeat => {
                            debug!("Received client heartbeat");
                        },
                        ClientTaskMessage::SessionCompleted => {
                            completed_workers += 1;
                            info!("Worker completed. {}/{} workers done.", completed_workers, actual_parallel_streams);
                            if completed_workers == actual_parallel_streams {
                                // All workers have finished sending their parts, and sent their EndOfStream markers.
                                debug!("All workers sent EndOfStream markers.");
                                break;
                            }
                        },
                        ClientTaskMessage::ChunkToSend(_) => {
                            // This should not happen, only ACKs, errors, heartbeats are expected
                            warn!("Received unexpected ChunkToSend from worker channel.");
                        }
                    }
                },
                _ = sleep(HEARTBEAT_INTERVAL * 2) => {
                    // Check for inactivity
                    let missing_chunks_count = self.missing_chunks.lock().await.len();
                    let bytes_sent_total = self.bytes_transferred.load(Ordering::SeqCst);
                    if self.file_size > 0 && bytes_sent_total >= self.file_size && missing_chunks_count == 0 {
                        info!("Client believes transfer complete. Bytes sent: {}, File Size: {}. All chunks accounted for.", bytes_sent_total, self.file_size);
                        break; // Exit if all bytes transferred and no missing chunks
                    } else if self.start_time.elapsed() > CONNECTION_TIMEOUT * 2 {
                        warn!("Client timed out waiting for all ACKs. Missing chunks: {}", missing_chunks_count);
                        return Err(TransferError::Timeout(tokio::time::error::Elapsed::new(CONNECTION_TIMEOUT)));
                    }
                },
                res = producer_handle => {
                    match res {
                        Ok(Ok(_)) => info!("Producer task finished successfully."),
                        Ok(Err(e)) => {
                            error!("Producer task failed: {}", e);
                            server_errors = true; // Indicate failure
                            break;
                        },
                        Err(e) => { // JoinError from tokio::spawn
                            error!("Producer task panicked: {:?}", e);
                            server_errors = true;
                            break;
                        }
                    }
                }
            }

            if server_errors {
                break;
            }

            // Check if overall transfer is complete
            if self.bytes_transferred.load(Ordering::SeqCst) >= self.file_size && self.missing_chunks.lock().await.is_empty() {
                // If producer is done and all chunks are marked as sent, it should mean complete.
                // We rely on server sending final ACK implicitly by marking all received.
                // A final "TransferComplete" message could be explicit.
                info!("Client detects all data sent and acknowledged.");
                break;
            }
        }

        // Wait for all workers to shut down cleanly or handle their termination
        for handle in worker_handles {
            if let Err(e) = handle.await {
                error!("Worker task failed: {:?}", e);
            }
        }
        save_resume_handle.abort(); // Stop periodic saving

        if let Some(ref pb) = self.progress_bar {
            pb.finish_with_message(format!(
                "✅ {} ({:.2} MB/s)",
                self.file_path.display(),
                self.bytes_transferred.load(Ordering::SeqCst) as f64
                    / (1024.0 * 1024.0)
                    / self.start_time.elapsed().as_secs_f64()
            ));
        }

        self.resume_info.lock().await.cleanup(&PathBuf::from(self.config.resume_directory.as_ref().unwrap_or(&"./resume_client".to_string()))).await;

        if server_errors {
            Err(TransferError::RemoteError("Transfer failed due to server error or worker failure.".to_string()))
        } else if self.bytes_transferred.load(Ordering::SeqCst) < self.file_size || !self.missing_chunks.lock().await.is_empty() {
            Err(TransferError::ProtocolError("Transfer incomplete: some chunks still missing.".to_string()))
        } else {
            Ok(())
        }
    }
}


// --- Main Application Logic ---

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Path to the configuration file (TOML format)
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run as a file transfer server
    Server,
    /// Run as a file transfer client
    Client {
        /// Path to the file to transfer
        #[arg(short, long)]
        file: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    fmt::subscriber().with_env_filter(EnvFilter::from_default_env()).init();

    let cli = Cli::parse();
    let config = Arc::new(Config::load_or_create(&cli.config)?);

    match cli.command {
        Commands::Server => run_server(config.server.clone()).await?,
        Commands::Client { file } => run_client(config.client.clone(), file).await?,
    }

    Ok(())
}

async fn run_server(config: ServerConfig) -> Result<(), TransferError> {
    info!("Starting server on {}:{}", config.address, config.port);

    let listener = TcpListener::bind(format!("{}:{}", config.address, config.port)).await?;
    let active_clients = Arc::new(AtomicUsize::new(0));
    let sessions: Arc<RwLock<HashMap<String, Arc<Mutex<TransferSession>>>>> = Arc::new(RwLock::new(HashMap::new()));
    let chunk_pool = Arc::new(ChunkPool::new(
        MAX_CHUNK_SIZE,
        MEMORY_POOL_SIZE,
    ));

    // Channel for server to send ACKs to individual client streams
    let (ack_sender, mut ack_receiver) = mpsc::channel::<Message>(ACK_BATCH_SIZE * config.max_clients);


    // Server-side ACK processing and sending task (per-session logic in handle_server_stream)
    // This `ack_sender` is passed into `handle_server_stream` which uses it to send ACKs specific to its session.
    // The previous implementation had a global ACK channel for server, this is refined.

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        info!("Accepted connection from: {}", peer_addr);

        let current_clients = active_clients.fetch_add(1, Ordering::SeqCst) + 1;
        if current_clients > config.max_clients {
            warn!("Max clients reached, rejecting connection from {}", peer_addr);
            // Optionally send an error message before closing
            let _ = stream.shutdown().await;
            active_clients.fetch_sub(1, Ordering::SeqCst);
            continue;
        }

        let sessions_clone = sessions.clone();
        let config_clone = Arc::new(config.clone());
        let chunk_pool_clone = chunk_pool.clone();
        let ack_sender_clone = ack_sender.clone(); // Clone for each handler

        tokio::spawn(async move {
            match handle_server_stream(
                stream,
                sessions_clone,
                config_clone,
                chunk_pool_clone,
                ack_sender_clone,
            )
            .await {
                Ok(_) => debug!("Stream handler for {} finished.", peer_addr),
                Err(e) => error!("Stream handler for {} encountered an error: {}", peer_addr, e),
            }
            active_clients.fetch_sub(1, Ordering::SeqCst);
            info!("Client {} disconnected. Active clients: {}", peer_addr, active_clients.load(Ordering::SeqCst));
        });
    }
}

async fn run_client(config: ClientConfig, file_path: PathBuf) -> Result<(), TransferError> {
    if !file_path.exists() {
        return Err(TransferError::FileNotFound(file_path));
    }

    info!("Starting client transfer of file: {}", file_path.display());

    let multi_progress = MultiProgress::new();
    let mut client_manager = ClientTransferManager::new(file_path, Arc::new(config), multi_progress).await?;

    let result = client_manager.run_transfer().await;

    // Ensure progress bars are cleared/finished even on error
    if let Some(pb) = client_manager.progress_bar {
        if result.is_err() {
            pb.abandon_with_message("❌ Transfer failed".to_string());
        } else {
            pb.finish_with_message("✅ Transfer complete".to_string());
        }
    }

    result
}

// Helper for converting SerializableResumeInfo to ResumeInfo
impl From<SerializableResumeInfo> for ResumeInfo {
    fn from(s: SerializableResumeInfo) -> Self {
        ResumeInfo {
            session_id: s.session_id,
            filename: s.filename,
            file_size: s.file_size,
            chunk_bitmap: s.chunk_bitmap,
            last_chunk_id: s.last_chunk_id,
            bytes_transferred: s.bytes_transferred,
            timestamp: s.timestamp,
            file_hash: s.file_hash,
            chunk_size: s.chunk_size,
            compression_enabled: s.compression_enabled,
        }
    }
}
