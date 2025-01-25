pub mod common;
pub mod lmdb;
pub mod rocksdb;
pub enum DatabaseType {
    RocksDB,
    LMDB,
}

use blackhole_client::embedding_generated::embedding::Ticket;
use tonic::Status;
use murmur3::murmur3_x64_128;
impl DatabaseType {
    pub fn create_db(&self, db_path: &str) -> Box<dyn DbInterface> {
        match self {
            DatabaseType::RocksDB => rocksdb::open_rocks_readonly(db_path),
            DatabaseType::LMDB => lmdb::setup_lmdb(db_path),
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
            root::<blackhole_client::embedding_generated::embedding::EmbeddingFeatureDataFlat>(bytes)
        {
            embedding
                .float32_vector()
                .map(|v| v.iter().map(|x| Some(x)).collect())
                .unwrap_or_default()
        } else {
            Vec::new()
        }
    }
    pub fn hash_id(id: &str) -> Result<i64, Status> {
        let mut cursor = std::io::Cursor::new(id.as_bytes());
        let hash_bytes = murmur3_x64_128(&mut cursor, 0)?;
        Ok(i64::from_le_bytes(hash_bytes.to_le_bytes()[0..8].try_into().unwrap()))
    }
       /// Determines the shard number for a given ID using the mmh3_hash128 function.
    pub fn shard_for_id(id: &str, shards: i16) -> Result<i16, Status> {
        assert!(shards > 0, "Shards must be greater than 0");
        let hash_value = Self::hash_id(id)?;
        Ok((hash_value.rem_euclid(shards as i64 * 24) / 24) as i16)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_for_id() {
        let id = "test_id";
        let shards = 10;
        let shard = DBUtil::shard_for_id(id, shards).expect("Failed to compute shard");
        assert!(
            shard >= 0 && shard < shards,
            "Shard {} is out of range",
            shard
        );
    }

    #[test]
    fn test_shard_for_id_edge_case() {
        let id = "";
        let shards = 5;
        let shard = DBUtil::shard_for_id(id, shards).expect("Failed to compute shard");
        assert!(
            shard >= 0 && shard < shards,
            "Shard {} is out of range",
            shard
        );
    }

    #[test]
    fn test_shard_for_id_large_shards() {
        let id = "another_test_id";
        let shards = 1000;
        let shard = DBUtil::shard_for_id(id, shards).expect("Failed to compute shard");
        assert!(
            shard >= 0 && shard < shards,
            "Shard {} is out of range",
            shard
        );
    }
    #[test]
    fn test_hash_id() {
        let id = "1037828263";

        println!("{}", DBUtil::hash_id(id).unwrap());
        assert!(DBUtil::hash_id(id).unwrap() == -5387033467748846870);
        assert!(DBUtil::shard_for_id(id, 60).unwrap() == 0)
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
