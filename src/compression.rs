use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::fs;
use crate::error::TransferError;
use crate::simd::{SimdChecksum, SimdProcessor};

#[derive(Debug, Clone)]
pub struct FileChunk {
    pub chunk_id: u64,
    pub session_id: String,
    pub data: Bytes,
    pub is_last: bool,
    pub is_compressed: bool,
    pub checksum: u32,
    pub original_size: usize,
    pub sequence_number: u64,
    pub retry_count: u32,
}

impl FileChunk {
    pub fn new(
        chunk_id: u64,
        session_id: String,
        data: Bytes,
        is_last: bool,
        sequence_number: u64,
    ) -> Self {
        let original_size = data.len();
        let is_compressed = false;
        let checksum = 0; // Will be calculated in compress()
        
        Self {
            chunk_id,
            session_id,
            data,
            is_last,
            is_compressed,
            checksum,
            original_size,
            sequence_number,
            retry_count: 0,
        }
    }

    /// Compress the chunk data using SIMD-optimized LZ4
    pub fn compress(&mut self) -> Result<(), TransferError> {
        if self.is_compressed {
            return Ok(());
        }

        let data_slice = &self.data[..];
        let compressed_data = SimdProcessor::compress_lz4_simd(data_slice)
            .map_err(|e| TransferError::CompressionError(e))?;

        // Only use compressed data if it's actually smaller
        if compressed_data.len() < self.data.len() {
            self.data = Bytes::from(compressed_data);
            self.is_compressed = true;
        }

        // Calculate SIMD-optimized checksum
        let mut checksum = SimdChecksum::new();
        self.checksum = checksum.calculate_crc32(&self.data[..]);

        Ok(())
    }

    /// Decompress the chunk data using SIMD-optimized LZ4
    pub fn decompress(&mut self) -> Result<(), TransferError> {
        if !self.is_compressed {
            return Ok(());
        }

        let data_slice = &self.data[..];
        let decompressed_data = SimdProcessor::decompress_lz4_simd(data_slice, self.original_size)
            .map_err(|e| TransferError::CompressionError(e))?;

        self.data = Bytes::from(decompressed_data);
        self.is_compressed = false;

        Ok(())
    }

    /// Validate the chunk using SIMD-optimized checksum verification
    pub fn validate(&self) -> bool {
        SimdProcessor::validate_data_simd(&self.data[..], self.checksum)
    }

    /// Calculate checksum using SIMD optimization
    pub fn calculate_checksum(&self) -> u32 {
        let mut checksum = SimdChecksum::new();
        checksum.calculate_crc32(&self.data[..])
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn compression_ratio(&self) -> f64 {
        if self.original_size == 0 {
            return 1.0;
        }
        self.data.len() as f64 / self.original_size as f64
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableResumeInfo {
    pub session_id: String,
    pub file_path: String,
    pub total_chunks: u64,
    pub completed_chunks: Vec<u64>,
    pub file_size: u64,
    pub chunk_size: usize,
    pub checksum: String,
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct ResumeInfo {
    pub session_id: String,
    pub file_path: PathBuf,
    pub total_chunks: u64,
    pub completed_chunks: std::collections::HashSet<u64>,
    pub file_size: u64,
    pub chunk_size: usize,
    pub checksum: String,
    pub timestamp: u64,
}

impl ResumeInfo {
    pub fn new(
        session_id: String,
        file_path: PathBuf,
        total_chunks: u64,
        file_size: u64,
        chunk_size: usize,
    ) -> Self {
        Self {
            session_id,
            file_path,
            total_chunks,
            completed_chunks: std::collections::HashSet::new(),
            file_size,
            chunk_size,
            checksum: String::new(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    pub fn save(&self, path: &PathBuf) -> Result<(), TransferError> {
        let serializable = self.to_serializable();
        let content = toml::to_string_pretty(&serializable)
            .map_err(|e| TransferError::TomlSerialization(e))?;
        
        fs::write(path, content)
            .map_err(|e| TransferError::Io(e))?;
        
        Ok(())
    }

    pub fn load(path: &PathBuf) -> Result<Self, TransferError> {
        let content = fs::read_to_string(path)
            .map_err(|e| TransferError::Io(e))?;
        
        let serializable: SerializableResumeInfo = toml::from_str(&content)
            .map_err(|e| TransferError::TomlDeserialization(e))?;
        
        Ok(serializable.into())
    }

    pub fn cleanup(&self, path: &PathBuf) -> Result<(), TransferError> {
        if path.exists() {
            fs::remove_file(path)
                .map_err(|e| TransferError::Io(e))?;
        }
        Ok(())
    }

    pub fn to_serializable(&self) -> SerializableResumeInfo {
        SerializableResumeInfo {
            session_id: self.session_id.clone(),
            file_path: self.file_path.to_string_lossy().to_string(),
            total_chunks: self.total_chunks,
            completed_chunks: self.completed_chunks.iter().cloned().collect(),
            file_size: self.file_size,
            chunk_size: self.chunk_size,
            checksum: self.checksum.clone(),
            timestamp: self.timestamp,
        }
    }
}

impl From<SerializableResumeInfo> for ResumeInfo {
    fn from(info: SerializableResumeInfo) -> Self {
        Self {
            session_id: info.session_id,
            file_path: PathBuf::from(info.file_path),
            total_chunks: info.total_chunks,
            completed_chunks: info.completed_chunks.into_iter().collect(),
            file_size: info.file_size,
            chunk_size: info.chunk_size,
            checksum: info.checksum,
            timestamp: info.timestamp,
        }
    }
} 