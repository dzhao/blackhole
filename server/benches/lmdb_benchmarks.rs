use criterion::{criterion_group, criterion_main, Criterion};
use blackhole_lib::lmdb::setup_lmdb;  // Update with your actual crate name
use blackhole_lib::common;

fn bench_lmdb(c: &mut Criterion) {
    common::bench_reads_under_write(c, setup_lmdb("lmdb_benchmarks"), 1000000, 100);
}

criterion_group!(benches, bench_lmdb);
criterion_main!(benches); 