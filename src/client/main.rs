use arrow::array::{ArrayRef, Float32Array, ListArray, Array};
use arrow::datatypes::{DataType, Field, Schema, Fields};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use futures::stream::TryStreamExt;
use arrow_flight::{
    Ticket,FlightClient
};
use tonic::transport::Channel;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the Flight server
    let mut client = FlightClient::new(
        Channel::from_static("http://localhost:8081").connect_lazy()
    );

    // Create a sample ticket payload with IDs and features
    let ticket_data = create_ticket_payload(
        vec!["sample_id1".to_string(), "sample_id2".to_string()],
        vec![
            ("feature1".to_string(), Some(0), Some(10)),
            ("feature2".to_string(), None, None),
        ],
    )?;

    // Create the ticket request
    let ticket = Ticket {
        ticket: ticket_data.into(),
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

fn create_ticket_payload(
    ids: Vec<String>,
    features: Vec<(String, Option<i16>, Option<i16>)>,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    // Create schema for the ticket
    let schema = Arc::new(Schema::new(Fields::from(vec![Field::new(
        "request",
        DataType::Struct(Fields::from(vec![
            Field::new(
                "ids",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                false,
            ),
            Field::new(
                "features",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(Fields::from(vec![
                        Field::new("name", DataType::Utf8, false),
                        Field::new("start", DataType::Int16, true),
                        Field::new("end", DataType::Int16, true),
                    ])),
                    false,
                ))),
                false,
            ),
        ])),
        false,
    )])));

    let feature_names: Vec<_> = features.iter().map(|(name, _, _)| name.as_str()).collect();
    let feature_starts: Vec<_> = features.iter().map(|(_, start, _)| *start).collect();
    let feature_ends: Vec<_> = features.iter().map(|(_, _, end)| *end).collect();

    // Create the ids list array
    let ids_list = arrow::array::StringArray::from(ids);

    // Create the feature struct array
    let feature_struct = arrow::array::StructArray::from(vec![
        (
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(arrow::array::StringArray::from(feature_names)) as ArrayRef,
        ),
        (
            Arc::new(Field::new("start", DataType::Int16, true)),
            Arc::new(arrow::array::Int16Array::from(feature_starts)) as ArrayRef,
        ),
        (
            Arc::new(Field::new("end", DataType::Int16, true)),
            Arc::new(arrow::array::Int16Array::from(feature_ends)) as ArrayRef,
        ),
    ]);

    let feature_struct_array = arrow::array::StructArray::from(vec![
        (
            Arc::new(Field::new(
                "ids",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                false,
            )),
            Arc::new(ids_list) as ArrayRef,
        ),
        (
            Arc::new(Field::new(
                "features",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(Fields::from(vec![
                        Field::new("name", DataType::Utf8, false),
                        Field::new("start", DataType::Int16, true),
                        Field::new("end", DataType::Int16, true),
                    ])),
                    false,
                ))),
                false,
            )),
            Arc::new(feature_struct) as ArrayRef,
        ),
    ]);

    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(feature_struct_array)])?;

    // Serialize the batch to bytes
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &schema)?;
    writer.write(&batch)?;
    writer.finish()?;

    Ok(buf)
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