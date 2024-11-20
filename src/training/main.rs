use arrow_flight::{
    flight_service_server::{FlightService, FlightServiceServer}, Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket
};
use std::pin::Pin;
use tonic::{Request, Response, Status, Streaming};
use futures::Stream;
use blackhole::rocksdb;
use blackhole::DbInterface;
use bytes::Bytes;

pub struct FlightDbServer {
    db: Box<dyn DbInterface>,
}

impl FlightDbServer {
    pub fn new(db: Box<dyn DbInterface>) -> Self {
        Self { db }
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
        let ticket = request.into_inner();
        
        let result = self.db.get(&ticket.ticket)
            .map_err(|e| Status::internal(e.to_string()))?;
        
        match result {
            Some(value) => {
                let flight_data = FlightData {
                    flight_descriptor: Some(FlightDescriptor {
                        r#type: 0,
                        cmd: Bytes::from(vec![]),
                        path: vec![]
                    }),
                    data_header: Bytes::new(),
                    data_body: Bytes::from(value),
                    app_metadata: Bytes::new()
                };

                let output = futures::stream::once(async move {
                    Ok(flight_data)
                });

                Ok(Response::new(Box::pin(output)))
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
    
    let db = rocksdb::setup_rocks();
    let server = FlightDbServer::new(db);
    
    let addr = "[::1]:50051".parse().unwrap();
    tonic::transport::Server::builder()
        .add_service(FlightServiceServer::new(server))
        .serve(addr)
        .await?;
    
    Ok(())
} 