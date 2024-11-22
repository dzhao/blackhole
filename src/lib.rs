pub mod lmdb;
pub mod rocksdb;
pub mod common;
pub enum DatabaseType {
    RocksDB,
    LMDB,
}

impl DatabaseType {
    pub fn create_db(&self) -> Box<dyn DbInterface> {
        match self {
            DatabaseType::RocksDB => rocksdb::open_rocks_readonly(),
            DatabaseType::LMDB => lmdb::setup_lmdb(),
        }
    }
}
pub trait DbInterface: Send + Sync {
    fn db_type(&self) -> String;
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn std::error::Error>>;
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>>;
    fn batch_put(&self, items: &[(Vec<u8>, Vec<u8>)]) -> Result<(), Box<dyn std::error::Error>>;
    fn close(&self) -> Result<(), Box<dyn std::error::Error>>;
    fn prefix_seek(&self, prefix: &str, start_ts: u16, end_ts: u16) -> Result<Vec<f32>, Box<dyn std::error::Error>>;
    
    fn reverse_encode(&self, prefix: &str, ts: u16) -> String {
        format!("{}:{:04}", prefix, u16::MAX - ts)
    }
    fn encode(&self, prefix: &str, ts: u16) -> String {
        format!("{}:{:04}", prefix, ts)
    }
    fn numpy_f32_vec(&self, bytes: &[u8]) -> Vec<f32> {
        bytes
            .chunks_exact(4)
            .map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap()))
            .collect()
    }
}
