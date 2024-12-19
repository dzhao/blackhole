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
}

fn main() {
    let args = Args::parse();
    let num_keys = (args.num_mkeys * 1_000_000) as usize;

    // Create your database instance
    println!("Running rocksdb benchmark");
    let db = Arc::new(rocksdb::setup_rocks("rocksdb_bench1", 25));
    common::run_concurrent_benchmark(db.clone(), num_keys, 1);
    let db = Arc::new(rocksdb::setup_rocks("rocksdb_bench2", 21));
    common::run_concurrent_benchmark(db.clone(), num_keys, 20);
    println!("Running lmdb benchmark");
    let db = Arc::new(lmdb::setup_lmdb("lmdb_bench1"));
    common::run_concurrent_benchmark(db.clone(), num_keys, 1);
    let db = Arc::new(lmdb::setup_lmdb("lmdb_bench2"));
    common::run_concurrent_benchmark(db.clone(), num_keys, 20);

} 