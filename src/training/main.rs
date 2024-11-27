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
use arrow::array::{Array, Float32Array, Int16Array, Int32Array, ListArray, RecordBatch, StringArray, StructArray, UInt8Array};
use arrow::ipc::reader::StreamReader;
use arrow::datatypes::{DataType, Field, Schema};


pub struct FlightDbServer {
    db: Box<dyn DbInterface>,
}

impl FlightDbServer {
    pub fn new(db_type: DatabaseType) -> Self {
        Self { db: db_type.create_db() }
    }

    fn decode_ticket(&self, ticket: &[u8]) -> Result<(Vec<String>, Vec<(String, Option<i16>, Option<i16>)>), Status> {
        // Create a stream reader
        let mut reader = StreamReader::try_new(ticket, None)
            .map_err(|e| Status::internal(format!("Failed to create reader: {}", e)))?;
        
        // Read the first (and only) batch
        let batch = reader
            .next()
            .ok_or_else(|| Status::internal("No record batch found"))?
            .map_err(|e| Status::internal(format!("Failed to read batch: {}", e)))?;

        // Get the struct array (first column)
        let data_struct = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| Status::internal("Failed to downcast to StructArray"))?;

        // Extract IDs
        let ids_list = data_struct
            .column_by_name("ids")
            .ok_or_else(|| Status::internal("ids field not found"))?
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| Status::internal("Failed to downcast ids to ListArray"))?;

        let ids: Vec<String> = ids_list
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Status::internal("Failed to downcast ids values to StringArray"))?
            .iter()
            .map(|s| s.unwrap_or_default().to_string())
            .collect();

        // Extract Features
        let features_list = data_struct
            .column_by_name("features")
            .ok_or_else(|| Status::internal("features field not found"))?
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| Status::internal("Failed to downcast features to ListArray"))?;

        let features_struct = features_list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| Status::internal("Failed to downcast features to StructArray"))?;

        let names = features_struct
            .column_by_name("name")
            .ok_or_else(|| Status::internal("name field not found"))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Status::internal("Failed to downcast name to StringArray"))?;

        let starts = features_struct
            .column_by_name("start")
            .ok_or_else(|| Status::internal("start field not found"))?
            .as_any()
            .downcast_ref::<Int16Array>()
            .ok_or_else(|| Status::internal("Failed to downcast start to Int32Array"))?;

        let ends = features_struct
            .column_by_name("end")
            .ok_or_else(|| Status::internal("end field not found"))?
            .as_any()
            .downcast_ref::<Int16Array>()
            .ok_or_else(|| Status::internal("Failed to downcast end to Int32Array"))?;

        let features: Vec<(String, Option<i16>, Option<i16>)> = (0..names.len())
            .map(|i| {
                (
                    names.value(i).to_string(),
                    if starts.is_null(i) { None } else { Some(starts.value(i)) },
                    if ends.is_null(i) { None } else { Some(ends.value(i)) },
                )
            })
            .collect();

        Ok((ids, features))
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
        let (ids, features) = self.decode_ticket(&ticket)?;
        assert_eq!(features.len(), 1);
        // let (feature_name, start, end) = &features[0];
        let schema = Arc::new(Schema::new(
            features.iter().map(|(feature_name, _, _)| Field::new(feature_name, DataType::Float32, false)).collect::<Vec<Field>>()
        ));

        println!("Features: {:?}", features);
        println!("IDs: {:?}", ids);
        
        // Collect all values for each ID using prefix seek
        let mut batches = Vec::new();
        for id in ids {
            let mut arrays = Vec::new();
            for (_, start, end) in &features {
            let values = self.db.prefix_seek(&id, start.unwrap() as u16, end.unwrap() as u16)
                .map_err(|e| Status::internal(e.to_string()))?;
            
            if values.is_empty() {
                return Err(Status::not_found("No matching data found in database"));
            }

            arrays.push(std::sync::Arc::new(Float32Array::from(values)) as Arc<dyn Array>);
        }
            let batch = RecordBatch::try_new(
                schema.clone(),
                arrays,
            ).map_err(|e| Status::internal(e.to_string()))?;
        
            batches.push(batch);
        }

        let stream = stream::iter(batches).map(Ok);
        let fd = FlightDataEncoderBuilder::new()
            .with_schema(schema).build(stream)
            .map_err(|e| Status::internal(e.to_string()));
        Ok(Response::new(Box::pin(fd)))
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