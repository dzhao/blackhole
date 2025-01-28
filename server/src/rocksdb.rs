use rocksdb::{Direction, IteratorMode, KeyEncodingType, Options, PlainTableFactoryOptions, SliceTransform, WaitForCompactOptions, DB};
use crate::{DBUtil, DatabaseType, DbInterface};
use serde_json::Value;
use std::fs;

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
            values.extend_from_slice(&DBUtil::numpy_f32_vec(&value));
            if &*key > DatabaseType::reverse_encode(prefix, start_ts).as_bytes() {
                break;
            }
        }
        Ok(values)
    }

    fn compact(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("compacting rocksdb...");
        let opts = WaitForCompactOptions::default();
        self.0.wait_for_compact(&opts)?;
        Ok(())
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

fn read_rocks_config(db_path: &str) -> Result<Value, Box<dyn std::error::Error>> {
    let config_path = format!("{}/rocksdict-config.json", db_path);
    println!("config_path: {}", config_path);
    let config_str = fs::read_to_string(config_path)?;
    let config: Value = serde_json::from_str(&config_str)?;
    Ok(config)
}

fn apply_prefix_extractor(opts: &mut Options, config: &Value) {
    if let Some(Value::Object(extractor)) = config.get("prefix_extractors").and_then(|p| p.get("default")) {
        match extractor.iter().next() {
            Some((key, value)) if key.as_str() == "Fixed" && value.is_u64() => {
                println!("using fixed prefix extractor with length: {}", value.as_u64().unwrap());
                opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(value.as_u64().unwrap() as usize));
            },
            Some((key, value)) if key.as_str() == "Capped" && value.is_u64() => {
                println!("using capped prefix extractor with length: {}", value.as_u64().unwrap());
                opts.set_prefix_extractor(SliceTransform::create_capped_prefix(value.as_u64().unwrap() as usize));
            },
            _ => println!("unsupported prefix extractor configuration"),
        }
    }
}

pub fn open_rocks_readonly(db_path: &str) -> Box<dyn DbInterface> {
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
    // opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(10));
    // opts.set_prefix_extractor(SliceTransform::create_capped_prefix(64));
    let config = read_rocks_config(&db_path).unwrap();
    apply_prefix_extractor(&mut opts, &config);
    Box::new(RocksDbWrapper(DB::open_for_read_only(&opts, db_path, false).unwrap()))
}

pub fn setup_rocks(db_name: &str, prefix_len: usize) -> Box<dyn DbInterface> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    
    // Try to read config file
    if let Ok(config) = read_rocks_config(db_name) {
        apply_prefix_extractor(&mut opts, &config);
    } else {
        // Fallback to default prefix extractor
        opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(prefix_len));
    }
    
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
    
    Box::new(RocksDbWrapper(DB::open(&opts, db_name).unwrap()))
} 