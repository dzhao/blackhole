use criterion::{criterion_group, criterion_main, Criterion};
use blackhole_lib::common;
fn bench_rocks(c: &mut Criterion) {
    common::bench_reads_under_write(c, blackhole_lib::rocksdb::setup_rocks("rocksdb_benchmarks", 10), 1000000, 100);
}

criterion_group!(benches, bench_rocks);
criterion_main!(benches); 