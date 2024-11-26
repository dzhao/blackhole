import json
import pyarrow as pa
import pyarrow.flight as flight
import base64
from feature_client import FeatureClient
import numpy as np
import tensorflow as tf
def main():
    #load the sample data
    with open("test.db/sample_data.json", "r") as f:
        sample_data = json.load(f)
    
    # Create a client
    client = FeatureClient()
    
    try:
        # Example: Get data for a key
        while True:
            ids = [f"u{int(id):09d}" for id in np.random.randint(10, size=5)]
            features = [("embeddings", 1, 3)]
            feature, st, end = features[0]
            reader = client.get_data(
            ids, 
            features,
        )
        
        # Read all batches from the stream
            idx = 0
            for batch in reader:
                golden_data = sample_data[ids[idx]][st:end+1]
                #flatten
                golden_data = [e for l in golden_data for e in l]
                retrieved_data = batch.data[feature].to_pylist()
                assert golden_data == retrieved_data

                tf_tensor = tf.convert_to_tensor(batch.data[feature].to_numpy())
                idx += 1

            # If you need to process specific columns:
            # column = batch.column('column_name')
            
    except flight.FlightUnavailableError:
        print("Could not connect to Flight server")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
