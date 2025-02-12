use clap::Parser;
use get_if_addrs::get_if_addrs;
use std::net::IpAddr;
use prometheus::Encoder;
use prometheus::TextEncoder;
use axum::response::IntoResponse;
use axum::http::StatusCode;
use axum::serve;
use tokio::net::TcpListener;

use blackhole_server::metrics::{init_metrics, REGISTRY, TOTAL_ERRORS};

/// Command-line arguments for the Flight server.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the database
    #[arg(long, default_value = "./test.db")]
    db_path: String,
    #[arg(long, default_value = "1")]
    shards: i16,
    /// Number of worker threads for Tokio runtime
    #[arg(long, default_value_t = 4)]
    num_workers: usize,
    /// Port to bind the server to
    #[arg(long, default_value = "8081")]
    port: u16,
    /// Whether to register the service in the service discovery directory
    #[arg(long, default_value = "")]
    cluster: String,
    /// Port for the metrics server
    #[arg(long, default_value = "9090")]
    metrics_port: u16,
}

/// Retrieves the first non-loopback IPv4 address of the host.
fn get_host_ip() -> Option<IpAddr> {
    if let Ok(ifaces) = get_if_addrs() {
        for iface in ifaces {
            if !iface.is_loopback() {
                match iface.addr {
                    get_if_addrs::IfAddr::V4(ipv4) => {
                        return Some(IpAddr::V4(ipv4.ip));
                    }
                    get_if_addrs::IfAddr::V6(_) => {}
                }
            }
        }
    }
    None
}

/// Handles incoming HTTP requests for metrics.
async fn metrics_handler() -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();

    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        buffer
    )
}

/// Starts the metrics HTTP server.
async fn start_metrics_server(addr: std::net::SocketAddr) {
    let app = axum::Router::new()
        .route("/metrics", axum::routing::get(metrics_handler));
    
    let listener = TcpListener::bind(addr).await.unwrap();
    println!("Metrics server running on http://{}", addr);
    
    serve(listener, app).await.unwrap();
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Parse command-line arguments
    let args = Args::parse();

    // Initialize Prometheus metrics
    init_metrics();

    // Retrieve the host IP address
    let host_ip = get_host_ip().unwrap_or_else(|| {
        println!("No non-loopback IPv4 address found. Falling back to 127.0.0.1.");
        "127.0.0.1".parse().unwrap()
    });

    // Define the address to bind the server to
    let addr: std::net::SocketAddr = format!("{}:{}", host_ip, args.port)
        .parse()
        .expect("Invalid IP address or port");

    // Define the address for the metrics server
    let metrics_addr: std::net::SocketAddr = format!("{}:{}", host_ip, args.metrics_port)
        .parse()
        .expect("Invalid metrics port");

    // Build Tokio runtime with specified number of worker threads
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.num_workers)
        .enable_all()
        .build()?;

    // Clone necessary variables for the metrics server
    let metrics_handle = rt.spawn(start_metrics_server(metrics_addr));

    // Start the main server
    rt.block_on(async {
        if let Err(e) = blackhole_server::start_server(addr, args.db_path, args.shards, &args.cluster).await {
            TOTAL_ERRORS.inc();
            eprintln!("Server error: {}", e);
        }
    });

    // Await the metrics server
    rt.block_on(metrics_handle)?;

    Ok(())
}
