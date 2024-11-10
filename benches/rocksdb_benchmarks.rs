#[path="common.rs"]
mod common;

use criterion::{criterion_group, criterion_main, Criterion};
use rocksdb::{DB, Options};
use common::DbInterface;
struct RocksDbWrapper(DB);

impl DbInterface for RocksDbWrapper {
    fn db_type(&self) -> String {
        "rocksdb".to_string()
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        self.0.put(key, value)?;
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        Ok(self.0.get(key)?)
    }

    fn close(self: Box<Self>) {
        drop(self.0)
    }
}

fn setup_rocks() -> Box<dyn DbInterface> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB
    opts.set_max_write_buffer_number(3);
    
    Box::new(RocksDbWrapper(DB::open(&opts, "/tmp/rocksdb_bench").unwrap()))
}

fn bench_rocks(c: &mut Criterion) {
    common::bench_reads_under_write(c, setup_rocks());
}

criterion_group!(benches, bench_rocks);
criterion_main!(benches); 