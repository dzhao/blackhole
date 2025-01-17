use arrow::array::{Float32Array, ListArray, Array};
use arrow::record_batch::RecordBatch;
use blackhole_lib::client::{copy_record_batch, create_flight_client, fetch_features};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = create_flight_client().await?;
    
    let batches = fetch_features(
        &mut client,
        vec!["u000000289".to_string(), "u000000288".to_string()],
        vec![
            ("f1".to_string(), Some(1), Some(1)),
            ("f2".to_string(), Some(1), Some(1)),
        ],
    ).await?;
    
    println!("{batches:?}");
    let mut results = vec![];
    copy_record_batch(batches, |values| results.extend_from_slice(values)).await?;
    println!("{results:?}, {}", results.len());
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