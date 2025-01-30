pub mod common;
pub mod lmdb;
pub mod rocksdb;
pub mod server;
pub enum DatabaseType {
    RocksDB,
    LMDB,
}

use blackhole_client::embedding_generated::embedding::Ticket;
use tonic::Status;
use murmur3::murmur3_x64_128;
use std::fs::OpenOptions;
use std::io::Write;
use fs2::FileExt; // Import the fs2 traits for file locking
use serde::Serialize;
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
pub const SERVICE_DISCOVERY_DIR: &str = "SERVICE_DISCOVERY";
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
                .map(|v| v.iter().map(Some).collect())
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

#[derive(Serialize)]
pub struct ShardConfig {
    shards: i16,
    ip: String,
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
    let ticket = flatbuffers::root::<Ticket>(ticket)
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

fn find_shard_file(shards: i16, service_discovery_path: &str, addr: std::net::SocketAddr) -> Option<(std::fs::File, i16)> {
    for shard in 0..shards {
        let shard_file_path = format!("{}/{}.json", service_discovery_path, shard);
            
        // Attempt to open the shard file
        let mut shard_handle = match OpenOptions::new()
            .write(true)
            .create(true)
            .open(&shard_file_path)
        {
            Ok(file) => file,
            Err(e) => {
                eprintln!("Failed to open shard {}: {}", shard, e);
                continue; // Skip to next shard on error
            }
        };
            
        // Attempt to acquire an exclusive lock without blocking
        if let Err(e) = shard_handle.try_lock_exclusive() {
            eprintln!(
                "Shard {} is locked by another process. Skipping...{}",
                shard,
                e
            );
            continue; // Skip to next shard if it's locked
        }

        let json = match serde_json::to_string_pretty(&ShardConfig {
            shards, 
            ip: addr.to_string()
        }) {
            Ok(j) => j,
            Err(e) => {
                eprintln!("Failed to serialize config for shard {}: {}", shard, e);
                continue;
            }
        };
        // Proceed to write to the shard file
        if let Err(e) = shard_handle.write_all(json.as_bytes()) {
            eprintln!("Failed to write to shard {}: {}", shard, e);
            // Optionally, you might want to unlock the file here
            // shard_handle.unlock()?;
            continue; // Skip to next shard on write failure
        }
        else {
            println!("Shard {} written to {}", shard, shard_file_path);
            return Some((shard_handle, shard));
        }
    }
    None
}
pub async fn start_server(
    addr: std::net::SocketAddr,
    db_path: String,
    shards: i16,
    service_discovery: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    
    let shard_info = if service_discovery {
        let service_discovery_path = format!("{}/{}", db_path, SERVICE_DISCOVERY_DIR);
        if !std::path::Path::new(&service_discovery_path).exists() {
            std::fs::create_dir(&service_discovery_path)?;
        }
        find_shard_file(shards, &service_discovery_path, addr)
    } else {
        None
    };
    let db_path = match shard_info {
        Some((_, shard)) => format!("{}/{:0>3}", db_path, shard),
        _ => db_path
    };
    let start_time = std::time::Instant::now();
    let server = crate::server::FlightDbServer::new(DatabaseType::RocksDB, &db_path);
    println!(
        "Starting feature server at {} with DB path: {}, shards: {}",
        addr, db_path, shards
    );
    println!("Server created in {:?}", start_time.elapsed());
    
    tonic::transport::Server::builder()
        .add_service(arrow_flight::flight_service_server::FlightServiceServer::new(server))
        .serve(addr)
        .await
        .map_err(|e| e.into())
}
