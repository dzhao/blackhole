use criterion::{criterion_group, criterion_main, Criterion};
use blackhole::lmdb::setup_lmdb;  // Update with your actual crate name
use blackhole::common;

fn bench_lmdb(c: &mut Criterion) {
    common::bench_reads_under_write(c, setup_lmdb());
}

criterion_group!(benches, bench_lmdb);
criterion_main!(benches); 