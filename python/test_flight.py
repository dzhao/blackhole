import pyarrow as pa
import pyarrow.flight as flight
import base64

class FlightClient:
    def __init__(self, host="localhost", port=50051):
        self.client = flight.connect(f"grpc://{host}:{port}")
    
    def get_data(self, features: list[str], ids: list[str], start_ts: int, end_ts: int):
        """
        Retrieve data using a ticket containing two string lists and two scalar timestamps
        """
        # Create arrays
        features_array = pa.array([features], type=pa.list_(pa.string()))
        ids_array = pa.array([ids], type=pa.list_(pa.string()))
        start_ts_array = pa.array([start_ts], type=pa.int32())
        end_ts_array = pa.array([end_ts], type=pa.int32())
        
        # Create struct array with proper types
        struct_array = pa.StructArray.from_arrays(
            [features_array, ids_array, start_ts_array, end_ts_array],
            ['features', 'ids', 'start_ts', 'end_ts']
        )
        
        # Create a record batch with a single row (our struct)
        batch = pa.record_batch([struct_array], names=['data'])
        
        # Serialize to bytes
        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_stream(sink, batch.schema)
        writer.write_batch(batch)
        writer.close()
        
        # Create ticket with the serialized data
        ticket = flight.Ticket(sink.getvalue().to_pybytes())
        return self.client.do_get(ticket)

def main():
    # Create a client
    client = FlightClient()
    
    try:
        # Example: Get data for a key
        key = "test_key"
        reader = client.get_data(
            ["f1", "f2"], 
            ["key01", "key02"], 
            1,  # start timestamp (scalar)
            2   # end timestamp (scalar)
        )
        
        # Read all batches from the stream
        for batch in reader:
            print(f"Retrieved batch: {batch}")
            # If you need to process specific columns:
            # column = batch.column('column_name')
            
    except flight.FlightUnavailableError:
        print("Could not connect to Flight server")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
