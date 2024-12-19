use lmdb::{Cursor, Database, DatabaseFlags, Environment, EnvironmentFlags, Transaction, WriteFlags};
use crate::{DatabaseType, DbInterface};

pub struct LmdbWrapper {
    env: Environment,
    db: Database
}

impl DbInterface for LmdbWrapper {
    fn db_type(&self) -> String {
        "lmdb".to_string()
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        let mut txn = self.env.begin_rw_txn()?;
        txn.put(self.db, &key, &value, WriteFlags::default())?;
        txn.commit()?;
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        let txn = self.env.begin_ro_txn()?;
        match txn.get(self.db, &key.as_bytes()) {
            Ok(value) => {
                Ok(Some(value.to_vec()))
            }
            Err(lmdb::Error::NotFound) => {
                Ok(None)
            },
            Err(e) => Err(Box::new(e)),
        }
    }

    fn batch_put(&self, items: &[(String, Vec<u8>)]) -> Result<(), Box<dyn std::error::Error>> {
        let mut txn = self.env.begin_rw_txn()?;
        for (key, value) in items {
            txn.put(self.db, &key.as_bytes(), value, WriteFlags::default())?;
        }
        txn.commit()?;
        Ok(())
    }

    fn close(&self) -> Result<(), Box<dyn std::error::Error>> {
        let stat = self.env.stat().unwrap();
        println!("Closing LMDB environment, stats: {}, {}, {}", stat.page_size(), stat.depth(), stat.leaf_pages());
        Ok(())
    }
    
    fn prefix_seek(&self, prefix: &str, start_ts: u16, end_ts: u16) -> Result<Vec<Option<f32>>, Box<dyn std::error::Error>> {
        let txn = self.env.begin_ro_txn()?;
        let mut cursor = txn.open_ro_cursor(self.db)?;

        // Create the start key for the range
        let start_key = DatabaseType::reverse_encode(prefix, end_ts);
        let mut values = Vec::new();
        // Iterate through the range
        for (key, value) in cursor.iter_from(&start_key.as_bytes()) {
            if &*key > DatabaseType::reverse_encode(prefix, start_ts).as_bytes() {
                break;
            }
            values.extend_from_slice(&self.numpy_f32_vec(&value));
        }
        Ok(values)
        
    }
}

pub fn setup_lmdb(db_name: &str) -> Box<dyn DbInterface> {
    // let path = "./lmdb_bench";
    std::fs::create_dir_all(db_name).unwrap();
    let env = Environment::new()
        .set_map_size(80*1024_usize.pow(3)) // 1TB
        .set_max_dbs(1)
        .open(std::path::Path::new(db_name))
        .unwrap();
    let db = env.create_db(None, DatabaseFlags::default()).unwrap();
    Box::new(LmdbWrapper { env, db })
} 