use criterion::{Criterion, BenchmarkId};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use rand::Rng;
use blackhole::DbInterface;
const EMBEDDING_SIZE:usize = 768;
const READ_BATCH: usize = 1_00;
const NUM_KEYS: usize = 4_000_000;

pub fn writer_thread(db: Arc<Box<dyn DbInterface>>, should_stop: Arc<AtomicBool>, key_prefix: &str) -> Vec<Vec<u8>> {
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
            batch.push((key, generate_random_embedding()));
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
                else {
                    assert_eq!(v.unwrap().len(), EMBEDDING_SIZE*4);
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

pub fn generate_random_embedding() -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let embeddings: Vec<f32> = (0..EMBEDDING_SIZE).map(|_| rng.gen::<f32>()).collect();
    embeddings.into_iter().map(|f| f.to_ne_bytes()).flatten().collect()
} 