import json
import pyarrow as pa
import pyarrow.flight as flight
import base64
from feature_client import FeatureClient
import numpy as np
import tensorflow as tf
import argparse
import time

def main():
    # Add argument parser
    parser = argparse.ArgumentParser(description='Flight test client')
    parser.add_argument('--db_path', type=str, default='test.db',
                       help='Path to the test database directory')
    parser.add_argument('--perf_test', action='store_true',
                       help='Enable performance testing')
    args = parser.parse_args()

    # Update file path to use command line argument
    with open(f"{args.db_path}/sample_data.json", "r") as f:
        sample_data = json.load(f)
    
    # Create a client
    client = FeatureClient()
    
    try:
        # Example: Get data for a key
        test_start = time.time()
        cnt = 0
        batch_size = 5
        while True:
            ids = [f"u{int(id):09d}" for id in np.random.randint(10, size=batch_size)]
            features = [("embeddings", 1, 3)]
            feature, st, end = features[0]
            reader = client.get_data(
                ids, 
                features,
            )
        
        # Read all batches from the stream
            idx = 0
            for batch in reader:
                if not args.perf_test:
                    golden_data = sample_data[ids[idx]][st:end+1]
                    #flatten
                    golden_data = [e for l in golden_data for e in l]
                    retrieved_data = batch.data[feature].to_pylist()
                    assert golden_data == retrieved_data
                else:
                    nump_data = batch.data[feature].to_numpy()
                    tf_tensor = tf.convert_to_tensor(nump_data)
                idx += 1
            cnt +=1
            if cnt % 1000 == 0:
                dur = time.time() - test_start
                print(f"Processed {cnt} requests in {dur:.3f} seconds, qps: {cnt/dur:.3f}, vectors/s:{cnt*batch_size/dur:.3f}")
    except flight.FlightUnavailableError:
        print("Could not connect to Flight server")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
