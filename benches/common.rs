use criterion::{Criterion, BenchmarkId};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use rand::Rng;

pub trait DbInterface: Send + Sync {
    fn db_type(&self) -> String;
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn std::error::Error>>;
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>>;
    fn close(self: Box<Self>);
}

pub fn writer_thread(db: Arc<Box<dyn DbInterface>>, should_stop: Arc<AtomicBool>) {
    let mut rng = rand::thread_rng();
    
    while !should_stop.load(Ordering::Relaxed) {
        let key = rng.gen::<[u8; 8]>();
        let value = rng.gen::<[u8; 32]>();
        
        if let Err(e) = db.put(&key, &value) {
            eprintln!("Write error: {}", e);
        }
        
        // Small sleep to prevent overwhelming the system
        thread::sleep(std::time::Duration::from_micros(100));
    }
}

pub fn bench_reads_under_write(c: &mut Criterion, db: Box<dyn DbInterface>) {
    let mut group = c.benchmark_group(format!("{}_reads", db.db_type()));
    let db = Arc::new(db);
    
    // Prepare some initial data
    let test_key = b"test_key";
    let test_value = b"test_value";
    db.put(test_key, test_value).expect("Failed to insert test data");
    
    // Start background writer
    let should_stop = Arc::new(AtomicBool::new(false));
    let writer_db = Arc::clone(&db);
    let writer_stop = Arc::clone(&should_stop);
    
    let writer_handle = thread::spawn(move || {
        writer_thread(writer_db, writer_stop);
    });
    
    // Benchmark reads while writes are happening
    group.bench_function(BenchmarkId::new("read_under_load", ""), |b| {
        b.iter(|| {
            db.get(test_key).expect("Read failed");
        });
    });
    
    // Clean up
    should_stop.store(true, Ordering::Relaxed);
    writer_handle.join().expect("Writer thread panicked");
    group.finish();
} 