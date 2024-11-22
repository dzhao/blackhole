use arrow_flight::{
    encode::FlightDataEncoderBuilder, flight_service_server::{FlightService, FlightServiceServer}, Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket
};
use std::{ops::Deref, pin::Pin, sync::Arc};
use tonic::{Request, Response, Status, Streaming};
use futures::{stream::{self, BoxStream}, Stream};
use futures::{StreamExt, TryStreamExt};
use blackhole::{rocksdb, DatabaseType};
use blackhole::DbInterface;
use bytes::Bytes;
use arrow::array::{Int32Array, ListArray, RecordBatch, StringArray, StructArray, UInt8Array};
use arrow::ipc::reader::StreamReader;
use arrow::datatypes::{DataType, Field, Schema};


pub struct FlightDbServer {
    db: Box<dyn DbInterface>,
}

impl FlightDbServer {
    pub fn new(db_type: DatabaseType) -> Self {
        Self { db: db_type.create_db() }
    }

    fn decode_ticket(&self, ticket: &[u8]) -> Result<(Vec<String>, Vec<String>, i32, i32), Status> {
        // Create a stream reader
        let mut reader = StreamReader::try_new(ticket, None)
            .map_err(|e| Status::internal(format!("Failed to create reader: {}", e)))?;
        
        // Read the first (and only) batch
        let batch = reader
            .next()
            .ok_or_else(|| Status::internal("No record batch found"))?
            .map_err(|e| Status::internal(format!("Failed to read batch: {}", e)))?;

        // Get the struct array (first column)
        let struct_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| Status::internal("Failed to downcast to StructArray"))?;

        // Extract the fields
        let features_list = struct_array
            .column_by_name("features")
            .ok_or_else(|| Status::internal("features field not found"))?
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| Status::internal("Failed to downcast features to ListArray"))?;

        let ids_list = struct_array
            .column_by_name("ids")
            .ok_or_else(|| Status::internal("ids field not found"))?
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| Status::internal("Failed to downcast ids to ListArray"))?;

        let start_ts = struct_array
            .column_by_name("start_ts")
            .ok_or_else(|| Status::internal("start_ts field not found"))?
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| Status::internal("Failed to downcast start_ts to Int32Array"))?
            .value(0);

        let end_ts = struct_array
            .column_by_name("end_ts")
            .ok_or_else(|| Status::internal("end_ts field not found"))?
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| Status::internal("Failed to downcast end_ts to Int32Array"))?
            .value(0);

        // Convert lists to Vecs
        let features = features_list
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Status::internal("Failed to downcast features values to StringArray"))?
            .iter()
            .map(|s| s.unwrap_or_default().to_string())
            .collect();

        let ids = ids_list
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Status::internal("Failed to downcast ids values to StringArray"))?
            .iter()
            .map(|s| s.unwrap_or_default().to_string())
            .collect();

        Ok((features, ids, start_ts, end_ts))
    }
}
#[tonic::async_trait]
impl FlightService for FlightDbServer {
    type HandshakeStream = Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>;
    type ListFlightsStream = Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send>>;
    type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>;
    type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send>>;
    type DoExchangeStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>;
    
    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema is not implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange is not implemented"))
    }
    type DoActionStream = Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send>>;
    type ListActionsStream = Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        unimplemented!()
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        // Return empty list as we don't support listing
        let output = futures::stream::empty();
        Ok(Response::new(Box::pin(output)))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner().ticket;
        let (features, ids, start_ts, end_ts) = self.decode_ticket(&ticket)?;

        println!("Features: {:?}", features);
        println!("IDs: {:?}", ids);
        println!("Time range: {} to {}", start_ts, end_ts);
        
        // let result = self.db.get(&ticket.ticket)
            // .map_err(|e| Status::internal(e.to_string()))?;
        let result = Some(vec![1, 2, 3, 4, 5]);

        match result {
            Some(value) => {
                // Create an Arrow record batch or array
                let schema = Arc::new(Schema::new(vec![
                    Field::new("e1", DataType::UInt8, false)
                ]));
                
                // Create an array from our bytes
                let array = UInt8Array::from(value);
                
                // Create a record batch
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![std::sync::Arc::new(array)]
                ).map_err(|e| Status::internal(e.to_string()))?;
                
                let stream = stream::iter(vec![batch]).map(Ok);
                let fd = FlightDataEncoderBuilder::new()
                .with_schema(schema).build(stream)
                .map_err(|e| Status::internal(e.to_string()));
                Ok(Response::new(Box::pin(fd)))
            }
            None => Err(Status::not_found("Key not found in database")),
        }
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        // Return not implemented as we don't support putting data
        Err(Status::unimplemented("do_put not implemented"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        // Return not implemented as we don't support actions
        Err(Status::unimplemented("do_action not implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        // Return empty list as we don't support actions
        let output = futures::stream::empty();
        Ok(Response::new(Box::pin(output)))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // Return not implemented as we don't support flight info
        Err(Status::unimplemented("get_flight_info not implemented"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        unimplemented!()
    }
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting Flight server...");
    
    let server = FlightDbServer::new(DatabaseType::RocksDB);
    
    let addr = "[::1]:50051".parse().unwrap();
    tonic::transport::Server::builder()
        .add_service(FlightServiceServer::new(server))
        .serve(addr)
        .await?;
    
    Ok(())
} 