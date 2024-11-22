import json
import pyarrow as pa
import pyarrow.flight as flight
import base64

class FlightClient:
    def __init__(self, host="localhost", port=50051):
        self.client = flight.connect(f"grpc://{host}:{port}")
    
    def get_data(self, ids: list[str], features: list[tuple]):
        """
        Retrieve data using a ticket containing feature tuples (name, start, end) and two scalar timestamps
        """
        # Create arrays
        features_array = pa.array([features], type=pa.list_(pa.struct([
            ('name', pa.string()),
            ('start', pa.int16()),
            ('end', pa.int16())
        ])))
        ids_array = pa.array([ids], type=pa.list_(pa.string()))
        
        # Create struct array with proper types
        struct_array = pa.StructArray.from_arrays(
            [ids_array, features_array],
            ['ids', 'features']
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
    #load the sample data
    with open("test.db/sample_data.json", "r") as f:
        sample_data = json.load(f)
    
    # Create a client
    client = FlightClient()
    
    try:
        # Example: Get data for a key
        key = "test_key"
        reader = client.get_data(
            ["user1", "user3"], 
            [("embeddings", 1, 3)],
        )
        
        # Read all batches from the stream
        for batch in reader:
            breakpoint()
            print(f"Retrieved batch: {batch}")
            # If you need to process specific columns:
            # column = batch.column('column_name')
            
    except flight.FlightUnavailableError:
        print("Could not connect to Flight server")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
