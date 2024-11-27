import json
import pyarrow as pa
import pyarrow.flight as flight
import base64
from feature_client import FeatureClient
import numpy as np
import tensorflow as tf
import argparse
import time
import psutil
import os

def main():
    # Add argument parser
    parser = argparse.ArgumentParser(description='Flight test client')
    parser.add_argument('--db_path', type=str, default='test.db',
                       help='Path to the test database directory')
    parser.add_argument('--perf_test', action='store_true',
                       help='Enable performance testing')
    args = parser.parse_args()

    # Initialize performance monitoring
    process = psutil.Process(os.getpid())
    has_io_counters = hasattr(process, 'io_counters')
    if has_io_counters:
        initial_io = process.io_counters()
    start_time = time.time()

    # Update file path to use command line argument
    with open(f"{args.db_path}/sample_data.json", "r") as f:
        sample_data = json.load(f)
    
    # Create a client
    client = FeatureClient()
    
    try:
        # Example: Get data for a key
        test_start = time.time()
        cnt = 0
        batch_size = 50
        while True:
            ids = [f"u{int(id):09d}" for id in np.random.randint(10, size=batch_size)]
            features = [("", 2, 3), ("f1", 2, 3), ("f2", 1, 3)]
            # feature, st, end = features[0]
            reader = client.get_data(
                ids, 
                features,
            )
        
        # Read all batches from the stream
            offset = 0
            batch = next(reader)
            for idx in range(batch_size):
                if idx == offset+len(batch.data):
                    offset += len(batch.data)
                    batch = next(reader)
                for feature, st, end in features:
                    if not args.perf_test:
                        golden_data = sample_data[ids[idx]][st:end+1]
                        #flatten
                        golden_data = [e for l in golden_data for e in l]
                        retrieved_data = batch.data[feature][idx-offset].values.to_pylist()
                        assert golden_data == retrieved_data
                    else:
                        nump_data = batch.data[feature][idx-offset].values.to_numpy(zero_copy_only=True)
                        tf_tensor = tf.convert_to_tensor(nump_data)
                idx += 1
            cnt +=1
            if args.perf_test and cnt % 1000 == 0:
                dur = time.time() - test_start
                cpu_percent = process.cpu_percent()
                
                print(f"Performance Metrics:")
                print(f"- Processed {cnt} requests in {dur:.3f} seconds")
                print(f"- QPS: {cnt/dur:.3f}")
                print(f"- Vectors/s: {cnt*batch_size*sum(e-s+1 for f, s,e in features)/dur:.3f}")
                print(f"- CPU Usage: {cpu_percent:.1f}%")
                
                # Only show I/O metrics if available
                if has_io_counters:
                    current_io = process.io_counters()
                    io_read_rate = (current_io.read_bytes - initial_io.read_bytes) / dur / 1024 / 1024  # MB/s
                    io_write_rate = (current_io.write_bytes - initial_io.write_bytes) / dur / 1024 / 1024  # MB/s
                    print(f"- I/O Read Rate: {io_read_rate:.2f} MB/s")
                    print(f"- I/O Write Rate: {io_write_rate:.2f} MB/s")
    except flight.FlightUnavailableError:
        print("Could not connect to Flight server")

if __name__ == "__main__":
    main()
