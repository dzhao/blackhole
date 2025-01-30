import pyarrow as pa
import pyarrow.flight as flight
import grpc
import tensorflow as tf
from typing import List, Tuple
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

class FeatureClient:
    def __init__(self, host="localhost", port=8081):
        self.host = host
        self.port = port
        self._connect()
    
    def _connect(self):
        location = flight.Location.for_grpc_tcp(self.host, self.port)
        self.client = flight.connect(location)
    def _encode_ticket(self, ids: List[str], features: List[Tuple]) -> flight.Ticket:
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

    def _get_data(self, ids: List[str], features: List[Tuple]):
        """
        Retrieve data using a ticket containing feature tuples (name, start, end) and two scalar timestamps
        """
        ticket = self._encode_ticket(ids, features)
        return self.client.do_get(ticket)
    

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((grpc.RpcError, flight.FlightUnavailableError))
    )
    def get_tensor(self, ids: List[str], features: List[Tuple]):
        try:
            reader = self._get_data(ids, features)
            all_tensors = [[None] * len(features) for _ in range(len(ids))]

            offset = 0
            for batch in reader:
                for f_idx in range(len(features)):
                    for id_idx, feature_row in enumerate(batch.data[f_idx]):
                        all_tensors[id_idx + offset][f_idx] = tf.convert_to_tensor(feature_row.values.to_numpy(zero_copy_only=True))
                offset += len(batch.data)
            return all_tensors
        except (grpc.RpcError, flight.FlightUnavailableError):
            self._connect()  # Only reconnect when there's an error
            raise  # Re-raise the exception to trigger retry

    @classmethod
    def encode(cls, prefix, ts):
        return f"{prefix}.{65535-ts:04x}".encode("utf-8")