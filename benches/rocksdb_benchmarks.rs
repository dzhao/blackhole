use criterion::{criterion_group, criterion_main, Criterion};
use blackhole::common;
fn bench_rocks(c: &mut Criterion) {
    common::bench_reads_under_write(c, blackhole::rocksdb::setup_rocks());
}

criterion_group!(benches, bench_rocks);
criterion_main!(benches); 