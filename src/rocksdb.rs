use rocksdb::{BlockBasedOptions, Direction, IteratorMode, KeyEncodingType, Options, PlainTableFactoryOptions, SliceTransform, DB};
use crate::{DatabaseType, DbInterface};

pub struct RocksDbWrapper(DB);

impl DbInterface for RocksDbWrapper {
    fn db_type(&self) -> String {
        "rocksdb".to_string()
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        self.0.put(key, value)?;
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        let res = self.0.get(key.as_bytes())?;
        if res.is_none() {
            println!("key:{key}, value:None");
        }
        Ok(res)
    }

    fn batch_put(&self, items: &[(String, Vec<u8>)]) -> Result<(), Box<dyn std::error::Error>> {
        let mut batch = rocksdb::WriteBatch::default();
        for (key, value) in items {
            batch.put(key.as_bytes(), value);
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
            IteratorMode::From(DatabaseType::reverse_encode(prefix, end_ts).as_bytes(), Direction::Forward));
        for item in iter {
            let (key, value) = item?;
            if &*key > DatabaseType::reverse_encode(prefix, start_ts).as_bytes() {
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

pub fn setup_rocks(db_name: &str, prefix_len: usize) -> Box<dyn DbInterface> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    
    // Memory optimizations
    opts.set_write_buffer_size(128 * 1024 * 1024);  // 128MB, plaintableformat smaller than 31 bits
    opts.set_max_write_buffer_number(6);
    
    // let mut block_based_options = BlockBasedOptions::default();
    
    // if use_block_cache {
        // let cache = rocksdb::Cache::new_lru_cache(200 * 1024 * 1024 * 1024);  // 200GB
        // block_based_options.set_block_cache(&cache);
        // opts.set_block_based_table_factory(&block_based_options);
    // }
    
    // PlainTable configuration - good for memory-mapped files
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
    
    // Enable memory mapping for PlainTable
    opts.set_allow_mmap_reads(true);
    opts.set_allow_mmap_writes(true);
    
    // Prefix optimization (required for PlainTable)
    opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(prefix_len));
    
    Box::new(RocksDbWrapper(DB::open(&opts, db_name).unwrap()))
} 