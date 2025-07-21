# QuickDrop

A high-performance, secure file transfer tool designed for enterprise environments and high-performance computing (HPC) scenarios. QuickDrop provides blazing-fast file transfers with advanced features like parallel streaming, adaptive chunking, compression, and transfer resumption.

## 🚀 Features

### High Performance
- **Parallel Streaming**: Multiple concurrent streams for maximum throughput
- **Adaptive Chunking**: Dynamically adjusts chunk sizes based on network conditions
- **Zero-Copy Operations**: Optimized memory usage for large file transfers
- **Connection Pooling**: Reuses connections for better performance
- **Compression**: Built-in LZ4 compression for faster transfers

### Reliability & Resilience
- **Transfer Resumption**: Resume interrupted transfers from where they left off
- **Automatic Retry**: Configurable retry logic with exponential backoff
- **Checksum Validation**: SHA-256 checksums ensure data integrity
- **Heartbeat Monitoring**: Connection health monitoring
- **Error Recovery**: Robust error handling and recovery mechanisms

### Enterprise Features
- **Progress Tracking**: Real-time progress bars with detailed statistics
- **Bandwidth Control**: Configurable speed limits
- **Session Management**: Unique session IDs for each transfer
- **Logging**: Comprehensive logging with tracing support
- **Configuration**: TOML-based configuration system

## 📦 Installation

### Prerequisites
- Rust 1.70+ and Cargo
- Network connectivity between client and server

### Build from Source
```bash
git clone https://github.com/modev2301/quickdrop.git
cd quickdrop
cargo build --release
```

The binary will be available at `target/release/quick_drop`.

## 🛠️ Configuration

QuickDrop uses a TOML configuration file. Here's an example `config.toml`:

```toml
[server]
address = "0.0.0.0"
port = 8080
max_clients = 10
enable_progress_bar = true
parallel_streams = 4
buffer_size = 65536
resume_directory = "./resume"

[client]
server_address = "127.0.0.1:8080"
enable_progress_bar = true
parallel_streams = 4
buffer_size = 65536
max_speed_mbps = 1000.0
resume_directory = "./resume"

[security]
encryption_enabled = false
auth_token = "optional-auth-token"

[performance]
adaptive_chunking = true
connection_pooling = true
memory_limit_mb = 1024
tcp_nodelay = true
```

## 🚀 Usage

### Running the Server

Start the file transfer server:

```bash
cargo run -- --config config.toml server
```

Or with the compiled binary:
```bash
./quick_drop --config config.toml server
```

### Running the Client

Transfer a file to the server:

```bash
cargo run -- --config config.toml client --file /path/to/your/file
```

Or with the compiled binary:
```bash
./quick_drop --config config.toml client --file /path/to/your/file
```

## 🔧 Advanced Configuration

### Performance Tuning

- **Parallel Streams**: Increase `parallel_streams` for better throughput
- **Buffer Size**: Adjust `buffer_size` based on available memory
- **Chunk Size**: Configure adaptive chunking for optimal performance
- **TCP Settings**: Enable `tcp_nodelay` for lower latency

### Network Optimization

- **Bandwidth Limiting**: Set `max_speed_mbps` to control transfer speed
- **Connection Pooling**: Reuse connections for better performance
- **Adaptive Chunking**: Automatically adjust chunk sizes based on network conditions

### Security

- **Authentication**: Optional token-based authentication
- **Encryption**: Placeholder for future encryption support
- **Session Management**: Secure session handling with unique IDs

## 📊 Performance Characteristics

- **Throughput**: Optimized for high-speed networks (1Gbps+)
- **Memory Usage**: Efficient memory management with configurable limits
- **CPU Usage**: Multi-threaded design for optimal CPU utilization
- **Network Efficiency**: Adaptive protocols for various network conditions

## 🏗️ Architecture

QuickDrop is built with Rust for maximum performance and safety:

- **Async/Await**: Non-blocking I/O operations
- **Multi-threading**: Parallel processing for high throughput
- **Memory Safety**: Rust's ownership system prevents memory errors
- **Error Handling**: Comprehensive error handling with custom error types

### Key Components

- **TransferSession**: Manages individual file transfer sessions
- **AdaptiveChunking**: Dynamically adjusts chunk sizes
- **ConnectionPool**: Manages connection reuse
- **RetryManager**: Handles retry logic with exponential backoff
- **ChunkPool**: Memory pool for efficient buffer management

## 🔍 Monitoring and Debugging

### Logging
QuickDrop uses the `tracing` framework for comprehensive logging:

```bash
RUST_LOG=debug cargo run -- --config config.toml server
```

### Progress Tracking
Real-time progress bars show:
- Transfer speed
- Completion percentage
- Estimated time remaining
- Bytes transferred

## 🛡️ Security Considerations

- **Data Integrity**: SHA-256 checksums verify file integrity
- **Session Isolation**: Each transfer uses unique session IDs
- **Error Handling**: Secure error messages without information leakage
- **Resource Limits**: Configurable limits prevent resource exhaustion

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For issues and questions:
- Check the configuration examples
- Review the logging output
- Ensure network connectivity between client and server
- Verify file permissions and disk space

---

**QuickDrop**: Fast, reliable, and secure file transfers for the modern enterprise.
