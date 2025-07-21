use clap::{Parser, Subcommand};
use quick_drop::{Config, ClientTransferManager, PerformanceBenchmark};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::info;
use indicatif::MultiProgress;

#[derive(Parser)]
#[command(name = "quick_drop")]
#[command(about = "High-performance file transfer tool with SIMD optimizations")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the transfer server
    Server {
        #[arg(short, long, default_value = "config.toml")]
        config: PathBuf,
    },
    /// Transfer a file to a server
    Transfer {
        #[arg(short, long)]
        file: PathBuf,
        #[arg(short, long, default_value = "localhost:8080")]
        server: String,
        #[arg(short, long, default_value = "config.toml")]
        config: PathBuf,
    },
    /// Run performance benchmarks
    Benchmark {
        #[arg(short, long, default_value = "./benchmark_results")]
        output_dir: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Server { config } => {
            info!("Starting server with config: {:?}", config);
            let _config = Config::load_or_create(&config)?;
            // TODO: Implement run_server function
            println!("Server mode not yet implemented");
        }
        Commands::Transfer { file, server, config } => {
            info!("Transferring file: {:?} to server: {}", file, server);
            let config = Config::load_or_create(&config)?;
            let multi_progress = MultiProgress::new();
            let mut client_manager = ClientTransferManager::new(file, Arc::new(config.client), multi_progress).await?;
            client_manager.run_transfer().await?;
        }
        Commands::Benchmark { output_dir } => {
            info!("Running performance benchmarks");
            run_benchmarks(&output_dir).await?;
        }
    }

    Ok(())
}

async fn run_benchmarks(output_dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    // Create output directory
    std::fs::create_dir_all(output_dir)?;
    
    let mut benchmark = PerformanceBenchmark::new();
    
    println!("🚀 Starting comprehensive performance benchmarks...");
    println!("📁 Test files will be created in: {:?}", output_dir);
    
    // Run the full benchmark suite
    let results = benchmark.run_full_benchmark(output_dir.to_str().unwrap())?;
    
    // Print results
    benchmark.print_results();
    
    // Save results to file
    let results_file = output_dir.join("benchmark_results.json");
    let json_results = serde_json::to_string_pretty(&results)?;
    std::fs::write(&results_file, json_results)?;
    println!("\n💾 Detailed results saved to: {:?}", results_file);
    
    Ok(())
}
