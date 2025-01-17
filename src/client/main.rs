use arrow::array::{Float32Array, ListArray, Array};
use arrow::ipc::Feature;
use arrow::record_batch::RecordBatch;
use blackhole::embedding_generated::embedding::{Ticket, TicketArgs};
use futures::stream::TryStreamExt;
use arrow_flight::FlightClient;
use tonic::transport::Channel;
use flatbuffers::FlatBufferBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the Flight server
    let mut client = FlightClient::new(
        Channel::from_static("http://localhost:8081").connect_lazy()
    );

    // Create a sample ticket payload with IDs and features
    let ticket= create_fbs_ticket(
        vec!["sample_id1".to_string(), "sample_id2".to_string()],
        vec![
            ("feature1".to_string(), Some(0), Some(10)),
            ("feature2".to_string(), None, None),
        ],
    )?;

    let ticket = arrow_flight::Ticket {
        ticket: ticket.into(),
    };
    // Make the do_get request
    let stream = client.do_get(ticket).await?;

    // Process the response
    let batches: Vec<RecordBatch> = stream.try_collect()
        .await
        .expect("no stream errors");
    println!("{batches:?}");
    Ok(())
}

fn create_fbs_ticket(
    ids: Vec<String>,
    features: Vec<(String, Option<i16>, Option<i16>)>,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut builder = FlatBufferBuilder::new();
    
    // Convert IDs to flatbuffer string offsets
    let fb_ids: Vec<_> = ids.iter()
        .map(|id| builder.create_string(id))
        .collect();
    let fb_ids = builder.create_vector(&fb_ids);

    // Extract and convert feature names to flatbuffer string offsets
    let fb_features: Vec<_> = features.iter()
        .map(|(name, _, _)| builder.create_string(name))
        .collect();
    let fb_features = builder.create_vector(&fb_features);

    // Convert start values to u16 (using 0 for None)
    let fb_starts: Vec<u16> = features.iter()
        .map(|(_, start, _)| start.map(|v| v as u16).unwrap_or(0))
        .collect();
    let fb_starts = builder.create_vector(&fb_starts);

    // Convert end values to u16 (using 0 for None)
    let fb_ends: Vec<u16> = features.iter()
        .map(|(_, _, end)| end.map(|v| v as u16).unwrap_or(0))
        .collect();
    let fb_ends = builder.create_vector(&fb_ends);

    // Create the ticket
    let ticket = Ticket::create(&mut builder, &TicketArgs {
        ids: Some(fb_ids),
        features: Some(fb_features),
        start: Some(fb_starts),
        end: Some(fb_ends),
    });

    builder.finish(ticket, None);
    
    Ok(builder.finished_data().to_vec().into())
}

fn print_batch(batch: &RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
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

fn decode_fbs_ticket(ticket: Vec<u8>) -> Result<(Vec<String>, Vec<(String, Option<i16>, Option<i16>)>), Box<dyn std::error::Error>> {
    // Verify the buffer and get the root Ticket
    let ticket = flatbuffers::root::<Ticket>(&ticket)
        .map_err(|e| format!("Failed to read ticket: {}", e))?;
    
    // Extract IDs
    let ids = ticket.ids()
        .ok_or("Missing IDs field")?
        .iter()
        .map(|id| id.to_string())
        .collect();

    // Extract features, starts, and ends
    let features = ticket.features()
        .ok_or("Missing features field")?;
    let starts = ticket.start()
        .ok_or("Missing start field")?;
    let ends = ticket.end()
        .ok_or("Missing end field")?;

    // Combine into feature tuples
    let feature_tuples = features.iter()
        .enumerate()
        .map(|(i, name)| {
            let start = if starts.get(i) == 0 { None } else { Some(starts.get(i) as i16) };
            let end = if ends.get(i) == 0 { None } else { Some(ends.get(i) as i16) };
            (name.to_string(), start, end)
        })
        .collect();

    Ok((ids, feature_tuples))
}