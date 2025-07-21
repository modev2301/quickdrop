pub mod config;
pub mod transfer;
pub mod network;
pub mod compression;
pub mod utils;
pub mod error;
pub mod simd;
pub mod zero_copy;
pub mod benchmark;

pub use config::Config;
pub use transfer::{TransferSession, ClientTransferManager};
pub use network::{ConnectionPool, NetworkStats};
pub use compression::FileChunk;
pub use error::TransferError;
pub use simd::{SimdChecksum, SimdProcessor, SimdOperation, SimdBenchmark};
pub use zero_copy::{ZeroCopyReader, ZeroCopyWriter, ZeroCopyBufferPool, ZeroCopyTransferManager};
pub use benchmark::{PerformanceBenchmark, BenchmarkResult, TestFile};

// Re-export commonly used types
pub use tokio;
pub use serde;
pub use bytes; 