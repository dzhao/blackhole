pub mod client;
pub mod common;
pub mod embedding_generated;
pub mod lmdb;
pub mod rocksdb;
pub enum DatabaseType {
    RocksDB,
    LMDB,
}

use crate::embedding_generated::embedding::{Ticket, TicketArgs};
use flatbuffers::FlatBufferBuilder;
impl DatabaseType {
    pub fn create_db(&self) -> Box<dyn DbInterface> {
        match self {
            DatabaseType::RocksDB => rocksdb::open_rocks_readonly(),
            DatabaseType::LMDB => lmdb::setup_lmdb("lmdb_db.test"),
        }
    }
    pub fn reverse_encode(prefix: &str, ts: u16) -> String {
        format!("{}.{:04x}", prefix, u16::MAX - ts)
    }
}
pub trait DbInterface: Send + Sync {
    fn db_type(&self) -> String;
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn std::error::Error>>;
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>>;
    fn batch_put(&self, items: &[(String, Vec<u8>)]) -> Result<(), Box<dyn std::error::Error>>;
    fn close(&self) -> Result<(), Box<dyn std::error::Error>>;
    fn prefix_seek(
        &self,
        prefix: &str,
        start_ts: u16,
        end_ts: u16,
    ) -> Result<Vec<Option<f32>>, Box<dyn std::error::Error>>;

    fn compact(&self) -> Result<(), Box<dyn std::error::Error>>;
}

pub struct DBUtil;

impl DBUtil {
    pub fn encode(prefix: &str, ts: u16) -> String {
        format!("{}.{:04x}", prefix, ts)
    }

    pub fn numpy_f32_vec(bytes: &[u8]) -> Vec<Option<f32>> {
        bytes
            .chunks_exact(4)
            .map(|chunk| Some(f32::from_le_bytes(chunk.try_into().unwrap())))
            .collect()
    }

    pub fn flatbuffer_f32_vec(bytes: &[u8]) -> Vec<Option<f32>> {
        use flatbuffers::root;
        if let Ok(embedding) =
            root::<crate::embedding_generated::embedding::EmbeddingFeatureDataFlat>(bytes)
        {
            embedding
                .float32_vector()
                .map(|v| v.iter().map(|x| Some(x)).collect())
                .unwrap_or_default()
        } else {
            Vec::new()
        }
    }
}


pub fn decode_fbs_ticket(
    ticket: &[u8],
) -> Result<(Vec<String>, Vec<(String, Option<i16>, Option<i16>)>), Box<dyn std::error::Error>> {
    // Verify the buffer and get the root Ticket
    let ticket = flatbuffers::root::<Ticket>(&ticket)
        .map_err(|e| format!("Failed to read ticket: {}", e))?;

    // Extract IDs
    let ids = ticket
        .ids()
        .ok_or("Missing IDs field")?
        .iter()
        .map(|id| id.to_string())
        .collect();

    // Extract features, starts, and ends
    let features = ticket.features().ok_or("Missing features field")?;
    let starts = ticket.start().ok_or("Missing start field")?;
    let ends = ticket.end().ok_or("Missing end field")?;

    // Combine into feature tuples
    let feature_tuples = features
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let start = if starts.get(i) == 0 {
                None
            } else {
                Some(starts.get(i) as i16)
            };
            let end = if ends.get(i) == 0 {
                None
            } else {
                Some(ends.get(i) as i16)
            };
            (name.to_string(), start, end)
        })
        .collect();

    Ok((ids, feature_tuples))
}
