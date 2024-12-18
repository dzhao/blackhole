use blackhole::common;
use blackhole::lmdb;
use blackhole::rocksdb;
use clap::Parser;
use std::sync::Arc;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Number of keys to use in benchmark
    #[arg(long, default_value_t = 4)]
    num_mkeys: u64,
    #[arg(long, default_value_t = false)]
    rocksdb_use_block_cache: bool,
}

fn main() {
    let args = Args::parse();

    // Create your database instance
    println!("Running rocksdb benchmark");
    let db = Arc::new(rocksdb::setup_rocks(args.rocksdb_use_block_cache));
    let num_keys = (args.num_mkeys * 1_000_000) as usize;
    common::run_concurrent_benchmark(db.clone(), true, num_keys);
    common::run_concurrent_benchmark(db, false, num_keys);

    println!("Running lmdb benchmark");
    let db = Arc::new(lmdb::setup_lmdb());
    // Just run the concurrent tests
    common::run_concurrent_benchmark(db.clone(), true, num_keys);
    common::run_concurrent_benchmark(db, false, num_keys);
} 