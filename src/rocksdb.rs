use rocksdb::{Options, SliceTransform, DB};
use crate::DbInterface;

pub struct RocksDbWrapper(DB);

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

    fn batch_put(&self, items: &[(Vec<u8>, Vec<u8>)]) -> Result<(), Box<dyn std::error::Error>> {
        let mut batch = rocksdb::WriteBatch::default();
        for (key, value) in items {
            batch.put(key, value);
        }
        self.0.write(batch)?;
        Ok(())
    }

    fn close(&self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}

pub fn setup_rocks() -> Box<dyn DbInterface> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_write_buffer_size(1024 * 1024 * 1024); // 64MB
    opts.set_max_write_buffer_number(3);
    
    Box::new(RocksDbWrapper(DB::open(&opts, "./rocksdb_bench").unwrap()))
} 