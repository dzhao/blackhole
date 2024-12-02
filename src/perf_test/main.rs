use blackhole::common;
use blackhole::lmdb;
use blackhole::rocksdb;
use std::sync::Arc;
fn main() {
    // Create your database instance
    println!("Running rocksdb benchmark");
    let db = Arc::new(rocksdb::setup_rocks());
    common::run_concurrent_benchmark(db.clone(), true);
    common::run_concurrent_benchmark(db, false);
    println!("Running lmdb benchmark");
    let db = Arc::new(lmdb::setup_lmdb());
    // Just run the concurrent tests
    common::run_concurrent_benchmark(db.clone(), true);
    common::run_concurrent_benchmark(db, false);

} 