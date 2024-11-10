mod common;
use criterion::{criterion_group, criterion_main, Criterion};
use lmdb::{Environment, Database, DatabaseFlags, WriteFlags, Transaction};
use common::DbInterface;

struct LmdbWrapper {
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

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        let txn = self.env.begin_ro_txn()?;
        match txn.get(self.db, &key) {
            Ok(value) => Ok(Some(value.to_vec())),
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(Box::new(e)),
        }
    }

    fn close(self: Box<Self>) {
        drop(self.env)
    }
}

fn setup_lmdb() -> Box<dyn DbInterface> {
    let env = Environment::new()
        .set_map_size(1024 * 1024 * 1024) // 1GB
        .set_max_dbs(1)
        .open(std::path::Path::new("/tmp/lmdb_bench"))
        .unwrap();
    let db = env.create_db(Some("default"), DatabaseFlags::default()).unwrap();
    Box::new(LmdbWrapper { env, db })
}
fn bench_lmdb(c: &mut Criterion) {
    common::bench_reads_under_write(c, setup_lmdb());
}

criterion_group!(benches, bench_lmdb);
criterion_main!(benches); 