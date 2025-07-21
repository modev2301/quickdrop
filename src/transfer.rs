use crate::compression::{FileChunk, ResumeInfo, SerializableResumeInfo};
use crate::error::TransferError;
use crate::utils::{ChunkPool, RetryManager};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, Mutex};
use uuid::Uuid;
use indicatif::{ProgressBar, MultiProgress};

// Constants for transfer
const DEFAULT_BUFFER_SIZE: usize = 64 * 1024;
const MAX_RETRIES: u32 = 3;

// Custom serialization for Bytes
mod bytes_serde {
    use super::*;
    use serde::{Serializer, Deserializer};

    pub fn serialize<S>(bytes: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&bytes[..])
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Bytes, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        Ok(Bytes::from(bytes))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    Handshake {
        session_id: String,
        filename: String,
        file_size: u64,
        chunk_size: usize,
        parallel_streams: usize,
        compression_enabled: bool,
        auth_token: String,
        resume_info: Option<SerializableResumeInfo>,
    },
    FileChunk {
        chunk_id: u64,
        session_id: String,
        #[serde(with = "bytes_serde")]
        data: Bytes,
        is_last: bool,
        is_compressed: bool,
        checksum: u32,
        original_size: usize,
        sequence_number: u64,
        retry_count: u32,
    },
    ChunkAck {
        session_id: String,
        chunk_id: u64,
        success: bool,
        error_message: Option<String>,
    },
    EndOfStream {
        session_id: String,
        stream_id: u64,
    },
    Error {
        session_id: String,
        error_message: String,
    },
}

impl Message {
    pub async fn write_to_stream<T>(
        &self,
        writer: &mut T,
    ) -> Result<(), TransferError>
    where
        T: AsyncWrite + Unpin,
    {
        let json = serde_json::to_string(self)
            .map_err(|e| TransferError::Serialization(e))?;
        let length = json.len() as u32;
        
        writer.write_all(&length.to_be_bytes()).await
            .map_err(|e| TransferError::Io(e))?;
        writer.write_all(json.as_bytes()).await
            .map_err(|e| TransferError::Io(e))?;
        writer.flush().await
            .map_err(|e| TransferError::Io(e))?;
        
        Ok(())
    }

    pub async fn read_from_stream<T>(
        reader: &mut T,
    ) -> Result<Option<Self>, TransferError>
    where
        T: AsyncRead + Unpin,
    {
        let mut length_bytes = [0u8; 4];
        match reader.read_exact(&mut length_bytes).await {
            Ok(_) => {},
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(None);
            },
            Err(e) => return Err(TransferError::Io(e)),
        }
        
        let length = u32::from_be_bytes(length_bytes) as usize;
        let mut json_bytes = vec![0u8; length];
        reader.read_exact(&mut json_bytes).await
            .map_err(|e| TransferError::Io(e))?;
        
        let json = String::from_utf8(json_bytes)
            .map_err(|e| TransferError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())))?;
        
        let message = serde_json::from_str(&json)
            .map_err(|e| TransferError::Serialization(e))?;
        
        Ok(Some(message))
    }
}

pub struct TransferSession {
    pub session_id: String,
    pub filename: String,
    pub file_size: u64,
    pub enable_progress_bar: bool,
    pub chunk_size: usize,
    pub completion_tx: oneshot::Sender<Result<PathBuf, TransferError>>,
    pub parallel_streams: usize,
    pub resume_info: Option<ResumeInfo>,
    pub ack_tx: mpsc::Sender<Message>,
    pub chunk_pool: Arc<ChunkPool>,
    pub output_directory: PathBuf,
    
    // State
    pub received_chunks: HashMap<u64, FileChunk>,
    pub completed_streams: HashSet<u64>,
    pub total_streams: usize,
    pub is_completed: AtomicBool,
    pub bytes_received: AtomicU64,
    pub start_time: Instant,
}

impl TransferSession {
    pub fn new(
        session_id: String,
        filename: String,
        file_size: u64,
        enable_progress_bar: bool,
        chunk_size: usize,
        completion_tx: oneshot::Sender<Result<PathBuf, TransferError>>,
        parallel_streams: usize,
        resume_info: Option<ResumeInfo>,
        ack_tx: mpsc::Sender<Message>,
        chunk_pool: Arc<ChunkPool>,
        output_directory: PathBuf,
    ) -> Self {
        let _total_chunks = (file_size + chunk_size as u64 - 1) / chunk_size as u64;
        
        Self {
            session_id,
            filename,
            file_size,
            enable_progress_bar,
            chunk_size,
            completion_tx,
            parallel_streams,
            resume_info,
            ack_tx,
            chunk_pool,
            output_directory,
            received_chunks: HashMap::new(),
            completed_streams: HashSet::new(),
            total_streams: parallel_streams,
            is_completed: AtomicBool::new(false),
            bytes_received: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    pub async fn add_chunk(&mut self, mut chunk: FileChunk) -> Result<bool, TransferError> {
        // Validate chunk
        if !chunk.validate() {
            return Err(TransferError::ChunkValidationFailed { chunk_id: chunk.chunk_id });
        }

        // Decompress if needed
        if chunk.is_compressed {
            chunk.decompress()?;
        }

        // Store chunk
        self.received_chunks.insert(chunk.chunk_id, chunk.clone());
        
        // Update resume info if available
        if let Some(ref mut _resume_info) = self.resume_info {
            // Note: We removed mark_chunk_received method, so we'll handle this differently
            // For now, we'll just track completion
        }

        // Update progress
        self.bytes_received.fetch_add(chunk.size() as u64, Ordering::Relaxed);

        // Send acknowledgment
        let ack = Message::ChunkAck {
            session_id: chunk.session_id,
            chunk_id: chunk.chunk_id,
            success: true,
            error_message: None,
        };
        
        if let Err(_) = self.ack_tx.send(ack).await {
            tracing::warn!("Failed to send chunk ACK");
        }

        // Check if transfer is complete
        let total_chunks = (self.file_size + self.chunk_size as u64 - 1) / self.chunk_size as u64;
        if self.received_chunks.len() as u64 >= total_chunks {
            self.complete_transfer().await?;
            return Ok(true);
        }

        Ok(false)
    }

    pub fn handle_end_marker(&self) -> bool {
        // This would track stream completion
        // For now, return false to indicate not all streams are done
        false
    }

    async fn complete_transfer(&mut self) -> Result<(), TransferError> {
        // Reconstruct file from chunks
        let output_path = self.output_directory.join(&self.filename);
        
        // Sort chunks by ID and write to file
        let mut sorted_chunks: Vec<_> = self.received_chunks.values().collect();
        sorted_chunks.sort_by_key(|chunk| chunk.chunk_id);
        
        // Write chunks to file
        use tokio::fs::File;
        use tokio::io::AsyncWriteExt;
        
        let mut file = File::create(&output_path).await
            .map_err(|e| TransferError::Io(e))?;
        
        for chunk in sorted_chunks {
            file.write_all(&chunk.data).await
                .map_err(|e| TransferError::Io(e))?;
        }
        
        file.flush().await
            .map_err(|e| TransferError::Io(e))?;
        
        // Send completion signal
        let completion_tx = std::mem::replace(&mut self.completion_tx, 
            oneshot::channel().0); // Replace with a dummy sender
        let _ = completion_tx.send(Ok(output_path));
        
        self.is_completed.store(true, Ordering::Relaxed);
        Ok(())
    }
}

pub struct ClientTransferManager {
    pub file_path: PathBuf,
    pub config: Arc<crate::config::ClientConfig>,
    pub multi_progress: MultiProgress,
    pub session_id: String,
    pub file_size: u64,
    pub chunk_size: usize,
    pub parallel_streams: usize,
    pub progress_bar: Option<ProgressBar>,
    pub resume_info: Arc<Mutex<ResumeInfo>>,
    pub retry_manager: RetryManager,
    pub chunk_pool: Arc<ChunkPool>,
}

impl ClientTransferManager {
    pub async fn new(
        file_path: PathBuf,
        config: Arc<crate::config::ClientConfig>,
        multi_progress: MultiProgress,
    ) -> Result<Self, TransferError> {
        use tokio::fs;
        
        // Get file metadata
        let metadata = fs::metadata(&file_path).await
            .map_err(|e| TransferError::Io(e))?;
        let file_size = metadata.len();
        
        // Generate session ID
        let session_id = Uuid::new_v4().to_string();
        
        // Calculate optimal chunk size
        let chunk_size = config.chunk_size.unwrap_or_else(|| {
            crate::utils::get_optimal_chunk_size_for_file(file_size)
        });
        
        // Calculate parallel streams
        let parallel_streams = config.parallel_streams.unwrap_or_else(|| {
            crate::utils::calculate_parallel_streams(file_size, chunk_size as f64)
        });
        
        // Create progress bar
        let progress_bar = if config.progress_bar_enabled {
            let pb = multi_progress.add(ProgressBar::new(file_size));
            pb.set_style(
                indicatif::ProgressStyle::default_bar()
                    .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
                    .unwrap()
                    .progress_chars("#>-")
            );
            Some(pb)
        } else {
            None
        };
        
        // Create resume info
        let total_chunks = (file_size + chunk_size as u64 - 1) / chunk_size as u64;
        let resume_info = Arc::new(Mutex::new(ResumeInfo::new(
            session_id.clone(),
            file_path.clone(),
            total_chunks,
            file_size,
            chunk_size,
        )));
        
        // Create chunk pool
        let chunk_pool = Arc::new(ChunkPool::new(
            config.buffer_size.unwrap_or(DEFAULT_BUFFER_SIZE),
            2000,
        ));
        
        Ok(Self {
            file_path,
            config,
            multi_progress,
            session_id,
            file_size,
            chunk_size,
            parallel_streams,
            progress_bar,
            resume_info,
            retry_manager: RetryManager::new(MAX_RETRIES),
            chunk_pool,
        })
    }

    pub async fn run_transfer(&mut self) -> Result<(), TransferError> {
        // TODO: Implement actual file transfer logic
        // This would involve:
        // 1. Connecting to server
        // 2. Sending handshake
        // 3. Reading file in chunks
        // 4. Sending chunks with retry logic
        // 5. Handling acknowledgments
        // 6. Updating progress bar
        
        tracing::info!("Starting file transfer for {}", self.file_path.display());
        tracing::info!("File size: {} bytes", self.file_size);
        tracing::info!("Chunk size: {} bytes", self.chunk_size);
        tracing::info!("Parallel streams: {}", self.parallel_streams);
        
        // Placeholder implementation
        Ok(())
    }
} 