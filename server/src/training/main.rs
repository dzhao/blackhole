use arrow::array::{
    Array, Int16Array, ListArray, RecordBatch, StringArray, StructArray,
};
use arrow::datatypes::{DataType, Field, Float32Type, Schema};
use arrow::ipc::reader::StreamReader;
use arrow_flight::{
    encode::FlightDataEncoderBuilder,
    flight_service_server::{FlightService, FlightServiceServer},
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use blackhole_lib::{decode_fbs_ticket, DBUtil, DbInterface, DatabaseType};
use clap::Parser;
use futures::{
    stream::{self},
    Stream,
};
use get_if_addrs::get_if_addrs;
use tokio::io::{self, AsyncWriteExt};
use std::{pin::Pin, sync::Arc, net::IpAddr};
use tonic::{Request, Response, Status, Streaming};
use futures::{TryStreamExt, StreamExt};

/// Command-line arguments for the Flight server.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the database
    #[arg(long, default_value = "./test.db")]
    db_path: String,
    #[arg(long, default_value = "1")]
    shards: i16,
    /// Number of worker threads for Tokio runtime
    #[arg(long, default_value_t = 4)]
    num_workers: usize,
    /// Port to bind the server to
    #[arg(long, default_value = "8081")]
    port: u16,
}

pub struct FlightDbServer {
    dbs: Vec<Box<dyn DbInterface>>,
    shards: i16,
}

impl FlightDbServer {
    pub fn new(db_type: DatabaseType, db_path: &str, shards: i16) -> Self {
        Self {
            dbs: (0..shards).map(|shard| db_type.create_db(&format!("{}/{:0>3}", db_path, shard))).collect(),
            shards,
        }
    }

    fn decode_ticket(
        &self,
        ticket: &[u8],
    ) -> Result<(Vec<String>, Vec<(String, Option<i16>, Option<i16>)>), Status> {
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
                    if starts.is_null(i) {
                        None
                    } else {
                        Some(starts.value(i))
                    },
                    if ends.is_null(i) {
                        None
                    } else {
                        Some(ends.value(i))
                    },
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
        let (ids, features) = self.decode_ticket(&ticket)
            .or_else(|_| decode_fbs_ticket(&ticket)
                .map_err(|e| Status::internal(e.to_string())))?;

        // Create schema with List<Float32> type for each feature
        let schema = Arc::new(Schema::new(
            features
                .iter()
                .map(|(feature_name, _, _)| {
                    Field::new(
                        feature_name,
                        DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                        false,
                    )
                })
                .collect::<Vec<Field>>(),
        ));

        let mut array_arrays = features.iter().map(|_| vec![]).collect::<Vec<_>>();
        for id in ids {
            for (i, (feature_name, start, end)) in features.iter().enumerate() {
                let prefix = if feature_name.is_empty() {
                    &id
                } else {
                    &format!("{}.{}", id, feature_name)
                };
                let db = &self.dbs[DBUtil::shard_for_id(&id, self.shards)? as usize];
                let values = match (start, end) {
                    (Some(start), Some(end)) => {
                            db.prefix_seek(prefix, *start as u16, *end as u16)
                            .map_err(|e| Status::internal(e.to_string()))?
                    },
                    (Some(_), None) => {
                        return Err(Status::not_found("can't have start only"));
                    },
                    (None, Some(end)) => {
                            db.prefix_seek(prefix, *end as u16, *end as u16)
                            .map_err(|e| Status::internal(e.to_string()))?
                    },
                    (None, None) => {
                       DBUtil::flatbuffer_f32_vec(&db.get(&prefix).unwrap().unwrap())
                    }
                };
                if values.is_empty() {
                    return Err(Status::not_found("No matching data found in database"));
                }
                array_arrays[i].push(Some(values));
            }
        }
        let arrays = array_arrays
            .into_iter()
            .map(|array| {
                Arc::new(ListArray::from_iter_primitive::<Float32Type, _, _>(
                    array,
                )) as Arc<dyn Array>
            })
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| Status::internal(e.to_string()))?;
        let stream = stream::iter(vec![batch]).map(Ok);
        let fd = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream)
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

/// Retrieves the first non-loopback IPv4 address of the host.
fn get_host_ip() -> Option<IpAddr> {
    if let Ok(ifaces) = get_if_addrs() {
        for iface in ifaces {
            if !iface.is_loopback() {
                match iface.addr {
                    get_if_addrs::IfAddr::V4(ipv4) => {
                        return Some(IpAddr::V4(ipv4.ip));
                    }
                    get_if_addrs::IfAddr::V6(_) => {}
                }
            }
        }
    }
    None
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command-line arguments
    let args = Args::parse();

    // Retrieve the host IP address
    let host_ip = get_host_ip().unwrap_or_else(|| {
        println!("No non-loopback IPv4 address found. Falling back to 127.0.0.1.");
        "127.0.0.1".parse().unwrap()
    });

    // Define the address to bind the server to
    let addr = format!("{}:{}", host_ip, args.port)
        .parse()
        .expect("Invalid IP address or port");

    // Build Tokio runtime with specified number of worker threads
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.num_workers)
        .enable_all()
        .build()?;

    println!(
        "Starting Flight server at {} with DB path: {}, shards: {}, num_workers: {}",
        addr, args.db_path, args.shards, args.num_workers
    );

    rt.block_on(async {
        let start_time = std::time::Instant::now();
        let server = FlightDbServer::new(DatabaseType::RocksDB, &args.db_path, args.shards);
        println!("Server created in {:?}", start_time.elapsed());
        eprintln!("{}", addr);
        io::stderr().flush().await.unwrap();
        tonic::transport::Server::builder()
            .add_service(FlightServiceServer::new(server))
            .serve(addr)
            .await
    })?;

    Ok(())
}
