from collections import defaultdict
import json
import rocksdict
import struct
import numpy as np

def create_sample_data(db_path: str):
    # Open Rocksdict
    options = rocksdict.Options(raw_mode=True)
    options.create_if_missing(True)
    options.set_plain_table_factory(rocksdict.PlainTableFactoryOptions())
    options.set_write_buffer_size(1024 * 1024 * 1024)
    options.set_prefix_extractor(rocksdict.SliceTransform.create_fixed_prefix(10)) 
    # options.set_prefix_extractor(rocksdict.SliceTransform.create_capped_prefix(10)) 
    options.set_compression_type(rocksdict.DBCompressionType.none())
    # Create DB with options
    db = rocksdict.Rdict(db_path, options)
    
    # Sample data configuration
    ids = ["user1", "user2", "user3"]
    feature_name = "embeddings"
    num_embeddings = 1000  # Number of embeddings per user
    dim = 1024 
    num_users = 1000
    try:
        # For each user
        output = defaultdict(lambda: [None] * num_embeddings)
        for user_id_num in range(num_users):
            user_id = f"u{user_id_num:09d}"
            # Generate some random embedding values
            
            # Store each embedding with an index
            # for idx in np.random.permutation(range(num_embeddings)):
            for idx in np.random.permutation(range(num_embeddings)):
                # Key format: "{id}:{feature_name}:{index}"
                embedding = np.random.randn(dim).astype(np.float32)
                for feature_name in ["", "f1", "f2"]:
                    prefix = user_id if feature_name == "" else f"{user_id}.{feature_name}"
                    key = f"{prefix}:{idx:04d}".encode()
                    # Use tobytes() directly instead of float_to_bytes
                    value = embedding.tobytes()
                    db[key] = value
                    if user_id_num < 10:
                        output[prefix][idx] = embedding.tolist()
                
            if user_id_num % 100 == 0:
                print(f"Added {num_embeddings} embeddings for {user_id}")
        with open(f"{db_path}/sample_data.json", "w") as f:
            json.dump(output, f)
            
    finally:
        # Make sure to close the database
        db.close()
        

if __name__ == "__main__":
    print("Creating and populating RocksDB...")
    create_sample_data("test.db")
    print("Done!")