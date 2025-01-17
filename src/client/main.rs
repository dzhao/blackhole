use arrow::array::{Float32Array, ListArray, Array};
use arrow::record_batch::RecordBatch;
use futures::stream::TryStreamExt;
use arrow_flight::FlightClient;
use tonic::transport::Channel;
use blackhole::create_fbs_ticket;

// Move the client creation into a public function
pub async fn create_flight_client() -> Result<FlightClient<Channel>, Box<dyn std::error::Error>> {
    Ok(FlightClient::new(
        Channel::from_static("http://localhost:8081").connect_lazy()
    ))
}

// Make the feature fetching functionality public and reusable
pub async fn fetch_features(
    client: &mut FlightClient<Channel>,
    user_ids: Vec<String>,
    features: Vec<(String, Option<i32>, Option<i32>)>,
) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
    let ticket = create_fbs_ticket(user_ids, features)?;
    let ticket = arrow_flight::Ticket {
        ticket: ticket.into(),
    };
    
    let stream = client.do_get(ticket).await?;
    Ok(stream.try_collect().await?)
}

// Update main to use the new functions
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = create_flight_client().await?;
    
    let batches = fetch_features(
        &mut client,
        vec!["u000000289".to_string(), "u000000288".to_string()],
        vec![
            ("f1".to_string(), Some(1), Some(10)),
        ],
    ).await?;
    
    println!("{batches:?}");
    Ok(())
}

// Make print_batch public as well
pub fn print_batch(batch: &RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
    for (i, column) in batch.columns().iter().enumerate() {
        let name = batch.schema().field(i).name().to_string();
        println!("Column {}: {}", name, column.len());
        
        if let Some(list_array) = column.as_any().downcast_ref::<ListArray>() {
            for j in 0..list_array.len() {
                if let Some(values) = list_array.value(j).as_any().downcast_ref::<Float32Array>() {
                    println!("  Values: {:?}", values.values());
                }
            }
        }
    }
    Ok(())
}