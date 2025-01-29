use clap::Parser;
use get_if_addrs::get_if_addrs;
use std::net::IpAddr;

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
    ///whether to register the service in the service discovery directory
    #[arg(long, default_value = "false")]
    service_discovery: bool,
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

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    // Retrieve the host IP address
    let host_ip = get_host_ip().unwrap_or_else(|| {
        println!("No non-loopback IPv4 address found. Falling back to 127.0.0.1.");
        "127.0.0.1".parse().unwrap()
    });
    // Define the address to bind the server to
    let addr: std::net::SocketAddr = format!("{}:{}", host_ip, args.port)
        .parse()
        .expect("Invalid IP address or port");

    // Build Tokio runtime with specified number of worker threads
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.num_workers)
        .enable_all()
        .build()?;

    rt.block_on(blackhole_server::start_server(addr, args.db_path, args.shards, args.service_discovery))?;
    Ok(())
}
