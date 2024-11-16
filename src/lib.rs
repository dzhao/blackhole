pub mod lmdb;
pub mod rocksdb;

pub trait DbInterface: Send + Sync {
    fn db_type(&self) -> String;
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn std::error::Error>>;
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>>;
    fn batch_put(&self, items: &[(Vec<u8>, Vec<u8>)]) -> Result<(), Box<dyn std::error::Error>>;
    fn close(&self) -> Result<(), Box<dyn std::error::Error>>;
}