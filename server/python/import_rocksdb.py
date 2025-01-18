from collections import defaultdict
import json
import rocksdict
import struct
import numpy as np
from feature_client import FeatureClient

NUM_EMBEDDINGS_PER_USER = 10  # Number of embeddings per user
DIM = 1024 
NUM_USERS = 1000
NUM_GOLDEN_USERS = 10
def create_sample_data(db_path: str):
    # Open Rocksdict
    options = rocksdict.Options(raw_mode=True)
    options.create_if_missing(True)
    options.set_plain_table_factory(rocksdict.PlainTableFactoryOptions())
    size = 256 * 1024 * 1024
    options.set_write_buffer_size(size)
    options.set_target_file_size_base(size)
    options.set_prefix_extractor(rocksdict.SliceTransform.create_fixed_prefix(10)) 
    # options.set_prefix_extractor(rocksdict.SliceTransform.create_capped_prefix(10)) 
    options.set_compression_type(rocksdict.DBCompressionType.none())
    # Create DB with options
    db = rocksdict.Rdict(db_path, options)
    
    # Sample data configuration
    ids = ["user1", "user2", "user3"]
    feature_name = "embeddings"
    try:
        # For each user
        output = defaultdict(lambda: [None] * NUM_EMBEDDINGS_PER_USER)
        for user_id_num in range(NUM_USERS):
            user_id = f"u{user_id_num:09d}"
            # Generate some random embedding values
            
            # Store each embedding with an index
            # for idx in np.random.permutation(range(num_embeddings)):
            for idx in np.random.permutation(range(NUM_EMBEDDINGS_PER_USER)):
                # Key format: "{id}:{feature_name}:{index}"
                for feature_name in ["", "f1", "f2"]:
                    embedding = np.random.randn(DIM).astype(np.float32)
                    prefix = user_id if feature_name == "" else f"{user_id}.{feature_name}"
                    ##use reverse encoding so latest coming in front
                    key = FeatureClient.encode(prefix, idx)
                    # key = f"{prefix}:{(-idx):04d}".encode()
                    # Use tobytes() directly instead of float_to_bytes
                    value = embedding.tobytes()
                    db[key] = value
                    if user_id_num < NUM_GOLDEN_USERS:
                        output[prefix][idx] = embedding.tolist()
                
            if user_id_num % 100 == 0:
                print(f"Added {NUM_EMBEDDINGS_PER_USER} embeddings for {user_id}")
        with open(f"{db_path}/sample_data.json", "w") as f:
            json.dump(output, f)
            
    finally:
        # Make sure to close the database
        db.close()
        

if __name__ == "__main__":
    print("Creating and populating RocksDB...")
    create_sample_data("test.db")
    print("Done!")