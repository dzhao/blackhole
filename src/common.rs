use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use criterion::{BenchmarkId, Criterion};
use rand::Rng;
use crate::DbInterface;
use std::time::{Duration, Instant};
use std::sync::atomic::AtomicU64;
use histogram::Histogram;
use std::sync::Mutex;

const EMBEDDING_SIZE:usize = 1024;
const READ_BATCH: usize = 50;

pub fn generate_keys(num_keys: usize, num_per_key: usize) -> impl Iterator<Item=String> {
    assert!(num_per_key < 100);
    (0..num_keys).flat_map(move |i| (0..num_per_key).map(move|j| format!("{i:010}{j:02}")))
}
pub fn writer_thread(db: Arc<Box<dyn DbInterface>>, should_stop: Arc<AtomicBool>, throttle: bool, key_prefix: &str, num_keys: usize, num_per_key: usize) -> Vec<Vec<u8>> {
    println!("writing {}..", key_prefix);
    let start_time = std::time::Instant::now();
    let mut idx = 0;
    let write_batch_size = 1000;
    let mut batch = vec![];
    let mut keys = vec![];
    for suffix in generate_keys(num_keys, num_per_key) {
        if should_stop.load(Ordering::Relaxed) {
            break;
        }
        let key = format!("{key_prefix}.{suffix}").into_bytes();
        assert_eq!(key.len(), 22, "key len mismatch:{}, {}", key_prefix, idx);
        //only copy the first key, i.e., the one ends with 00. this is in get we only have one key per suffix,
        //in prefix seek we have multiple keys per suffix but we only need the first one for prefix
        if suffix.ends_with("00") {
            keys.push(key.clone());
        }
         if batch.len() == write_batch_size {
            db.batch_put(&batch).expect("Failed to batch put");
            batch.clear();
            //sleep 500 ms so we have 2k qps write
            if throttle {
                thread::sleep(Duration::from_millis(500));
            }
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

pub fn bench_reads_under_write(c: &mut Criterion, db: Box<dyn DbInterface>, num_keys: usize, num_per_key: usize) {
    let mut group = c.benchmark_group(format!("{}_reads", db.db_type()));
    let db = Arc::new(db);
    
    // Prepare test data 
    let sequential_keys = writer_thread(db.clone(), Arc::new(AtomicBool::new(false)), false, "pre_write", num_keys, num_per_key);
    
    // Start background writer
    let should_stop = Arc::new(AtomicBool::new(false));
    let writer_db = Arc::clone(&db);
    let writer_stop = Arc::clone(&should_stop);
    
    let writer_handle = thread::spawn(move || {
        writer_thread(writer_db, writer_stop, true, "postwrite", num_keys, num_per_key);
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

#[derive(Debug)]
pub struct ConcurrentTestResults {
    pub throughput: f64,
    pub latency_p50_ms: f64,
    pub latency_p95_ms: f64,
    pub latency_p99_ms: f64,
    pub latency_max_ms: f64,
    pub total_operations: u64,
    pub errors: u64,
}

pub struct ConcurrentTester {
    db: Arc<Box<dyn DbInterface>>,
    num_threads: usize,
    duration: Duration,
    keys: Arc<Vec<Vec<u8>>>,
    ops_counter: Arc<AtomicU64>,
    error_counter: Arc<AtomicU64>,
    histogram: Arc<Mutex<Histogram>>,
    should_stop: Arc<AtomicBool>,
}

impl ConcurrentTester {
    pub fn new(db: Arc<Box<dyn DbInterface>>, keys: Arc<Vec<Vec<u8>>>, num_threads: usize, duration: Duration) -> Self {
        let should_stop = Arc::new(AtomicBool::new(false));
        // Pre-generate test data

        Self {
            db: db,
            num_threads,
            duration,
            keys: keys,
            ops_counter: Arc::new(AtomicU64::new(0)),
            error_counter: Arc::new(AtomicU64::new(0)),
            // Configure histogram with microsecond precision
            histogram: Arc::new(Mutex::new(Histogram::new(
                10, 20  // min and max values in microseconds
            ).unwrap())),
            should_stop,
        }
    }

    pub fn run_test(&self, readonly: bool, num_keys: usize, num_per_key: usize) -> ConcurrentTestResults {
        let start = Instant::now();

        let db = self.db.clone();
        let should_stop = self.should_stop.clone();
        let writer_handle = thread::spawn(move || {
            if readonly {
                println!("readonly, no concurrent write")
            }
            else {
                writer_thread(db, should_stop, true, "concurren", num_keys, num_per_key);
            }
        });
        // Spawn reader threads
        let handles: Vec<_> = (0..self.num_threads)
            .map(|_| {
                let db = Arc::clone(&self.db);
                let keys = Arc::clone(&self.keys);
                let ops = Arc::clone(&self.ops_counter);
                let errors = Arc::clone(&self.error_counter);
                let hist = Arc::clone(&self.histogram);
                let should_stop = self.should_stop.clone();
                thread::spawn(move || {
                    let mut rng = rand::thread_rng();
                    
                    while !should_stop.load(Ordering::Relaxed) {
                        let idx = rng.gen_range(0..keys.len());
                        let op_start = Instant::now();
                        
                        match db.get(&keys[idx]) {
                            Ok(_) => {
                                let latency = op_start.elapsed().as_micros() as u64;
                                if let Ok(mut hist) = hist.lock() {
                                    let _ = hist.increment(latency);
                                }
                                ops.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(_) => {
                                errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                })
            })
            .collect();

        // Run for specified duration
        thread::sleep(self.duration);
        self.should_stop.store(true, Ordering::Relaxed);

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }
        writer_handle.join().unwrap();

        self.collect_results(start.elapsed())
    }

    fn collect_results(&self, duration: Duration) -> ConcurrentTestResults {
        let hist = self.histogram.lock().unwrap();
        let total_ops = self.ops_counter.load(Ordering::Relaxed);
        
        ConcurrentTestResults {
            throughput: total_ops as f64 / duration.as_secs_f64(),
            latency_p50_ms: hist.percentile(50.0).unwrap().unwrap().end() as f64 / 1000.0,
            latency_p95_ms: hist.percentile(95.0).unwrap().unwrap().end() as f64 / 1000.0,
            latency_p99_ms: hist.percentile(99.0).unwrap().unwrap().end() as f64 / 1000.0,
            latency_max_ms: hist.percentile(100.0).unwrap().unwrap().end() as f64 / 1000.0,
            total_operations: total_ops,
            errors: self.error_counter.load(Ordering::Relaxed),
        }
    }
} 

pub fn run_concurrent_benchmark(db: Arc<Box<dyn DbInterface>>, readonly: bool, num_keys:usize) {
    let configs = vec![
        (1, "single thread"),
        (4, "4 threads"),
        (8, "8 threads"),
        (16, "16 threads"),
    ];

    println!("\nRunning concurrent benchmarks for {}", db.db_type());
    println!("----------------------------------------");

    // let db = Arc::new(db);
    let keys = writer_thread(
        db.clone(), 
        Arc::new(AtomicBool::new(false)),
        false,
        "pre_write",
        num_keys,
        1
        );
    let keys = Arc::new(keys);
    for (thread_count, desc) in configs {
        let tester = ConcurrentTester::new(
            db.clone(),
            keys.clone(),
            thread_count,
            Duration::from_secs(30)
        );

        let results = tester.run_test(readonly, num_keys, 1);

        println!("\n{} results(readonly: {}):", desc, readonly);
        println!("  Throughput: {:.2} ops/sec", results.throughput);
        println!("  Latencies (ms):");
        println!("    p50: {:.3}", results.latency_p50_ms);
        println!("    p95: {:.3}", results.latency_p95_ms);
        println!("    p99: {:.3}", results.latency_p99_ms);
        println!("    max: {:.3}", results.latency_max_ms);
        println!("  Total operations: {}", results.total_operations);
        println!("  Errors: {}", results.errors);
    }
} 