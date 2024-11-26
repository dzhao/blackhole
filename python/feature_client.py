import pyarrow as pa
import pyarrow.flight as flight
import grpc

class FeatureClient:
    def __init__(self, host="localhost", port=50051, wait_timeout=300):
        location = flight.Location.for_grpc_tcp(host, port)
        options = [
            ('grpc.enable_retries', 1),
            ('grpc.keepalive_timeout_ms', wait_timeout * 1000),
            ('grpc.service_config', '{"retryPolicy": { \
                "maxAttempts": 5, \
                "initialBackoff": "1s", \
                "maxBackoff": "10s", \
                "backoffMultiplier": 2, \
                "retryableStatusCodes": ["UNAVAILABLE"] \
            }}')
        ]
        self.client = flight.connect(location, generic_options=options)
    
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