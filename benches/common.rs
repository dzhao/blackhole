use criterion::{Criterion, BenchmarkId};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use rand::Rng;
const EMBEDDING_SIZE:usize = 3*1024;
const READ_BATCH: usize = 1_00;
const NUM_KEYS: usize = 1_000_000;
pub trait DbInterface: Send + Sync {
    fn db_type(&self) -> String;
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn std::error::Error>>;
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>>;
    fn batch_put(&self, items: &[(Vec<u8>, Vec<u8>)]) -> Result<(), Box<dyn std::error::Error>>;
    fn close(&self) -> Result<(), Box<dyn std::error::Error>>;
}

pub fn writer_thread(db: Arc<Box<dyn DbInterface>>, should_stop: Arc<AtomicBool>, key_prefix: &str) -> Vec<Vec<u8>> {
    let value = "x".repeat(EMBEDDING_SIZE).into_bytes();
    let start_time = std::time::Instant::now();
    let mut idx = 0;
    let write_batch_size = 1000;
    let mut batch = vec![];
    let mut keys = vec![];
    while !should_stop.load(Ordering::Relaxed) && idx < NUM_KEYS {
        let key = format!("{}_{:010}", key_prefix, idx).into_bytes();
        keys.push(key.clone());
         if batch.len() == write_batch_size {
            db.batch_put(&batch).expect("Failed to batch put");
            batch.clear();
        }
        else {
            batch.push((key, value.clone()));
        }
        idx += 1;
    }
    if !batch.is_empty() {
        db.batch_put(&batch).expect("Failed to batch put");
    }

    let duration = start_time.elapsed();
    let throughput = idx as f64 / duration.as_secs_f64();
    println!(
        "Writer thread finished for {}. Total keys written: {}, Duration: {:.2?}, Throughput: {:.2} keys/sec",
        key_prefix, idx, duration, throughput
    );
    return keys;
}

pub fn bench_reads_under_write(c: &mut Criterion, db: Box<dyn DbInterface>) {
    let mut group = c.benchmark_group(format!("{}_reads", db.db_type()));
    let db = Arc::new(db);
    
    // Prepare test data 
    let sequential_keys = writer_thread(db.clone(), Arc::new(AtomicBool::new(false)), "prewrite");
    
    // Start background writer
    let should_stop = Arc::new(AtomicBool::new(false));
    let writer_db = Arc::clone(&db);
    let writer_stop = Arc::clone(&should_stop);
    
    let writer_handle = thread::spawn(move || {
        writer_thread(writer_db, writer_stop, "postwrite");
    });
    
    // Benchmark sequential reads
    group.bench_function(BenchmarkId::new("sequential_reads", ""), |b| {
        b.iter(|| {
            let mut not_found = 0;
            for key in &sequential_keys[0..READ_BATCH] {
                let v = db.get(key).expect("Read failed");
                if v.is_none() {
                    not_found += 1;
                }
            }
            if not_found > 0 {
                println!("Not found: {}", not_found);
            }
        });
    });

    fn batch_reads(db: Arc<Box<dyn DbInterface>>, keys: &[Vec<u8>]) {
        let mut rng = rand::thread_rng();
        for _ in 0..READ_BATCH { 
            let idx = rng.gen_range(0..keys.len());
            db.get(&keys[idx]).expect("Read failed");
        }
    } 
    // Benchmark random reads
    group.bench_function(BenchmarkId::new("random_reads", ""), |b| {
        b.iter(|| {
            batch_reads(db.clone(), &sequential_keys);
        });
    });
    
    // Benchmark multi-threaded reads
    let num_threads = 8;
    group.bench_function(BenchmarkId::new("parallel_reads", ""), |b| {
        b.iter(|| {
            let handles: Vec<_> = (0..num_threads)
                .map(|_| {
                    let db = Arc::clone(&db);
                    let keys = sequential_keys.clone();
                    thread::spawn(move || {
                        batch_reads(db, &keys);
                    })
                })
                .collect();
            
            for handle in handles {
                handle.join().expect("Reader thread panicked");
            }
        });
    });
    
    should_stop.store(true, Ordering::Relaxed);
    // Clean up
    if let Err(e) = writer_handle.join() {
        eprintln!("Writer thread panicked: {:?}", e);
    }
    if let Err(e) = db.close() {
        eprintln!("Failed to close database: {:?}", e);
    }
    group.finish();
} 