import pyarrow as pa
import pyarrow.flight as flight
import grpc
import tensorflow as tf
class FeatureClient:
    def __init__(self, host="localhost", port=50051, wait_timeout=3600):
        location = flight.Location.for_grpc_tcp(host, port)
        options = [
            ('grpc.enable_retries', 1),
            ('grpc.keepalive_timeout_ms', wait_timeout * 1000),
            ('grpc.service_config', '{"methodConfig": [{ \
                "name": [{"service": "arrow.flight.protocol.FlightService"}], \
                "waitForReady": true, \
                "timeout": "3600s", \
                "retryPolicy": { \
                    "maxAttempts": 5, \
                    "initialBackoff": "1s", \
                    "maxBackoff": "10s", \
                    "backoffMultiplier": 2, \
                    "retryableStatusCodes": ["UNAVAILABLE"] \
                }}]}')
        ]
        self.client = flight.connect(location, generic_options=options)
    
    def _encode_ticket(self, ids: list[str], features: list[tuple]) -> flight.Ticket:
        """
        Encode ids and features into a Flight ticket
        
        Args:
            ids: List of string identifiers
            features: List of feature tuples (name, start, end)
            
        Returns:
            flight.Ticket: Encoded ticket ready for transport
        """
        # Create arrays
        ids_array = pa.array([ids], type=pa.list_(pa.string()))
        features_array = pa.array([features], type=pa.list_(pa.struct([
            ('name', pa.string()),
            ('start', pa.int16()),
            ('end', pa.int16())
        ])))
        
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
        
        return flight.Ticket(sink.getvalue().to_pybytes())

    def _get_data(self, ids: list[str], features: list[tuple]):
        """
        Retrieve data using a ticket containing feature tuples (name, start, end) and two scalar timestamps
        
        Example:
            client = FeatureClient()
            ids = ["id1", "id2"]
            features = [("feature1", 0, 10), ("feature2", 5, 15)]
            data = client.get_data(ids, features)
            # Process the data as needed
        """
        ticket = self._encode_ticket(ids, features)
        return self.client.do_get(ticket) 
    
    def get_tensor(self, ids: list[str], features: list[tuple]):
        reader = self._get_data(ids, features)
        all_tensors = [[None] * len(features) for _ in range(len(ids))]

        offset = 0
        for batch in reader:
            for f_idx in range(len(features)):
                for id_idx, feature_row in enumerate(batch.data[f_idx]):
                    all_tensors[id_idx + offset][f_idx] = tf.convert_to_tensor(feature_row.values.to_numpy(zero_copy_only=True))
            offset += len(batch.data)
        return all_tensors