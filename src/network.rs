use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use crate::error::TransferError;
use crate::config::{BASE_CHUNK_SIZE, CONNECTION_POOL_SIZE};

// Network constants
const MIN_CHUNK_SIZE: usize = 1024;
const MAX_CHUNK_SIZE: usize = 1024 * 1024;

#[derive(Debug, Clone)]
pub struct NetworkStats {
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub connections_established: u64,
    pub connections_failed: u64,
    pub average_latency_ms: f64,
    pub packet_loss_rate: f64,
    pub bandwidth_mbps: f64,
    pub start_time: Instant,
}

impl NetworkStats {
    pub fn new() -> Self {
        Self {
            bytes_sent: 0,
            bytes_received: 0,
            connections_established: 0,
            connections_failed: 0,
            average_latency_ms: 0.0,
            packet_loss_rate: 0.0,
            bandwidth_mbps: 0.0,
            start_time: Instant::now(),
        }
    }

    pub fn update_bandwidth(&mut self, bytes_transferred: u64, duration: Duration) {
        let seconds = duration.as_secs_f64();
        if seconds > 0.0 {
            let bytes_per_second = bytes_transferred as f64 / seconds;
            self.bandwidth_mbps = bytes_per_second * 8.0 / 1_000_000.0; // Convert to Mbps
        }
    }

    pub fn update_latency(&mut self, latency_ms: f64) {
        // Simple moving average
        self.average_latency_ms = (self.average_latency_ms + latency_ms) / 2.0;
    }

    pub fn record_connection_success(&mut self) {
        self.connections_established += 1;
    }

    pub fn record_connection_failure(&mut self) {
        self.connections_failed += 1;
    }

    pub fn record_bytes_sent(&mut self, bytes: u64) {
        self.bytes_sent += bytes;
    }

    pub fn record_bytes_received(&mut self, bytes: u64) {
        self.bytes_received += bytes;
    }
}

#[derive(Debug)]
pub struct AdaptiveChunking {
    pub current_size: usize,
    pub min_size: usize,
    pub max_size: usize,
    pub success_count: u32,
    pub failure_count: u32,
    pub last_adjustment: Instant,
    pub adjustment_interval: Duration,
}

impl AdaptiveChunking {
    pub fn new() -> Self {
        Self {
            current_size: BASE_CHUNK_SIZE,
            min_size: MIN_CHUNK_SIZE,
            max_size: MAX_CHUNK_SIZE,
            success_count: 0,
            failure_count: 0,
            last_adjustment: Instant::now(),
            adjustment_interval: Duration::from_secs(5),
        }
    }

    pub fn record_success(&mut self) {
        self.success_count += 1;
        self.check_adjustment();
    }

    pub fn record_failure(&mut self) {
        self.failure_count += 1;
        self.check_adjustment();
    }

    fn check_adjustment(&mut self) {
        if self.last_adjustment.elapsed() >= self.adjustment_interval {
            self.adjust_chunk_size();
            self.last_adjustment = Instant::now();
        }
    }

    fn adjust_chunk_size(&mut self) {
        let success_rate = if self.success_count + self.failure_count > 0 {
            self.success_count as f64 / (self.success_count + self.failure_count) as f64
        } else {
            0.5
        };

        if success_rate > 0.8 {
            // Increase chunk size
            self.current_size = std::cmp::min(
                self.current_size * 2,
                self.max_size
            );
        } else if success_rate < 0.5 {
            // Decrease chunk size
            self.current_size = std::cmp::max(
                self.current_size / 2,
                self.min_size
            );
        }

        // Reset counters
        self.success_count = 0;
        self.failure_count = 0;
    }

    pub fn get_current_size(&self) -> usize {
        self.current_size
    }
}

#[derive(Debug)]
pub struct ConnectionPool {
    connections: Arc<Mutex<HashMap<String, TcpStream>>>,
}

impl ConnectionPool {
    pub fn new(_max_connections: usize) -> Self {
        Self {
            connections: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn get_connection(&self, address: &str) -> Result<TcpStream, TransferError> {
        let _connections = self.connections.lock().await;
        
        // For now, we'll just create a new connection each time
        // In a real implementation, we'd check the pool first
        let stream = TcpStream::connect(address).await
            .map_err(|e| TransferError::NetworkError(e.to_string()))?;
        
        // Set TCP options
        stream.set_nodelay(true)
            .map_err(|e| TransferError::NetworkError(e.to_string()))?;
        
        Ok(stream)
    }

    pub async fn return_connection(&self, _address: &str, _stream: TcpStream) {
        // For now, we just drop the stream
        // In a real implementation, we'd add it back to the pool
    }

    pub async fn get_stats(&self) -> NetworkStats {
        NetworkStats::new()
    }

    pub async fn clear_pool(&self) {
        let mut connections = self.connections.lock().await;
        connections.clear();
    }
}

impl Default for ConnectionPool {
    fn default() -> Self {
        Self::new(CONNECTION_POOL_SIZE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_network_stats() {
        let mut stats = NetworkStats::new();
        
        stats.record_bytes_sent(1024);
        stats.record_bytes_received(2048);
        stats.record_connection_success();
        stats.update_latency(10.5);
        
        assert_eq!(stats.bytes_sent, 1024);
        assert_eq!(stats.bytes_received, 2048);
        assert_eq!(stats.connections_established, 1);
        // The latency calculation is (old + new) / 2, so with initial 0.0 and new 10.5
        assert_eq!(stats.average_latency_ms, 5.25);
    }

    #[test]
    fn test_adaptive_chunking() {
        let mut chunking = AdaptiveChunking::new();
        
        assert_eq!(chunking.get_current_size(), BASE_CHUNK_SIZE);
        
        // Record successes to increase chunk size
        for _ in 0..10 {
            chunking.record_success();
        }
        
        // Force adjustment by setting last_adjustment to old time
        chunking.last_adjustment = Instant::now() - Duration::from_secs(10);
        chunking.check_adjustment();
        
        // Should have increased chunk size
        assert!(chunking.get_current_size() > BASE_CHUNK_SIZE);
    }

    #[test]
    fn test_connection_pool() {
        let _pool = ConnectionPool::new(5);
        // Test that the pool is created successfully
        assert!(true);
    }

    #[test]
    fn test_bandwidth_calculation() {
        let mut stats = NetworkStats::new();
        let duration = Duration::from_secs(1);
        
        stats.update_bandwidth(1_000_000, duration); // 1MB in 1 second
        
        // Should be approximately 8 Mbps (1MB * 8 bits / 1 second)
        assert!(stats.bandwidth_mbps > 7.0 && stats.bandwidth_mbps < 9.0);
    }
} 