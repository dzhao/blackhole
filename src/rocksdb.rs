use rocksdb::{Direction, IteratorMode, KeyEncodingType, Options, PlainTableFactoryOptions, SliceTransform, DB};
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

    fn prefix_seek(&self, prefix: &str, start_ts: u16, end_ts: u16) -> Result<Vec<Option<f32>>, Box<dyn std::error::Error>> {
        let mut values = Vec::new();
        let iter = self.0.iterator(
            IteratorMode::From(self.reverse_encode(prefix, end_ts).as_bytes(), Direction::Forward));
        for item in iter {
            let (key, value) = item?;
            if &*key > self.reverse_encode(prefix, start_ts).as_bytes() {
                break;
            }
            values.extend_from_slice(&self.numpy_f32_vec(&value));
        }
        Ok(values)
    }

}

// Add this struct and implementation before the open_rocks_readonly function
/* 
struct CustomPrefixTransform;

impl SliceTransform for CustomPrefixTransform {
    fn transform<'a>(&self, key: &'a [u8]) -> &'a [u8] {
        let prefix_len = std::cmp::min(key.len(), 64);
        &key[..prefix_len]
    }

    fn in_domain(&self, key: &[u8]) -> bool {
        !key.is_empty()
    }

    fn in_range(&self, _: &[u8]) -> bool {
        true
    }
}
*/

pub fn open_rocks_readonly() -> Box<dyn DbInterface> {
    let mut opts = Options::default();
    //minimize background jobs since we are only reading
    opts.set_max_background_jobs(0);
    opts.set_max_write_buffer_number(0);
    let factory_opts = PlainTableFactoryOptions {
        user_key_length: 0,
        bloom_bits_per_key: 20,
        hash_table_ratio: 0.75,
        index_sparseness: 16,
        huge_page_tlb_size: 0,
        encoding_type: KeyEncodingType::Plain,
        full_scan_mode: false,
        store_index_in_file: false,
    };
    opts.set_plain_table_factory(&factory_opts);
    opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(10));
    Box::new(RocksDbWrapper(DB::open(&opts, "./test.db").unwrap()))
}

pub fn setup_rocks() -> Box<dyn DbInterface> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_write_buffer_size(1024 * 1024 * 1024); // 64MB
    opts.set_max_write_buffer_number(3);
    /*
    let factory_opts = PlainTableFactoryOptions {
        user_key_length: 0,
        bloom_bits_per_key: 20,
        hash_table_ratio: 0.75,
        index_sparseness: 16,
        huge_page_tlb_size: 0,
        encoding_type: KeyEncodingType::Plain,
        full_scan_mode: false,
        store_index_in_file: false,
    };
    opts.set_plain_table_factory(&factory_opts);
    opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(20));
    */
    Box::new(RocksDbWrapper(DB::open(&opts, "./rocksdb_bench").unwrap()))
} 