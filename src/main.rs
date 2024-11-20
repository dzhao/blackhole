use blackhole::common;
use blackhole::lmdb;
use blackhole::rocksdb;

fn main() {
    // Create your database instance
    println!("Running lmdb benchmark");
    let db = lmdb::setup_lmdb();
    // Just run the concurrent tests
    common::run_concurrent_benchmark(db);

    println!("Running rocksdb benchmark");
    let db = rocksdb::setup_rocks();
    common::run_concurrent_benchmark(db);
}
