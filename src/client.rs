use arrow::{array::{Array, Float32Array, ListArray}, record_batch::RecordBatch};
use arrow_flight::{FlightClient, Ticket};
use futures::stream::TryStreamExt;
use tonic::transport::Channel;
use flatbuffers::FlatBufferBuilder;
use crate::embedding_generated::embedding::{Ticket as FbsTicket, TicketArgs};

pub fn create_fbs_ticket(
    ids: Vec<String>,
    features: Vec<(String, Option<i16>, Option<i16>)>,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut builder = FlatBufferBuilder::new();

    // Convert IDs to flatbuffer string offsets
    let fb_ids: Vec<_> = ids.iter().map(|id| builder.create_string(id)).collect();
    let fb_ids = builder.create_vector(&fb_ids);

    // Extract and convert feature names to flatbuffer string offsets
    let fb_features: Vec<_> = features
        .iter()
        .map(|(name, _, _)| builder.create_string(name))
        .collect();
    let fb_features = builder.create_vector(&fb_features);

    // Convert start values to u16 (using 0 for None)
    let fb_starts: Vec<u16> = features
        .iter()
        .map(|(_, start, _)| start.map(|v| v as u16).unwrap_or(0))
        .collect();
    let fb_starts = builder.create_vector(&fb_starts);

    // Convert end values to u16 (using 0 for None)
    let fb_ends: Vec<u16> = features
        .iter()
        .map(|(_, _, end)| end.map(|v| v as u16).unwrap_or(0))
        .collect();
    let fb_ends = builder.create_vector(&fb_ends);

    // Create the ticket
    let ticket = FbsTicket::create(
        &mut builder,
        &TicketArgs {
            ids: Some(fb_ids),
            features: Some(fb_features),
            start: Some(fb_starts),
            end: Some(fb_ends),
        },
    );

    builder.finish(ticket, None);

    Ok(builder.finished_data().to_vec().into())
}

pub struct FeatureClient {
    client: FlightClient,
}

impl FeatureClient {
    pub async fn init(url: String) -> Result<Self, Box<dyn std::error::Error>> {
        let client = FlightClient::new(
            Channel::from_shared(url)?.connect_lazy()
        );
        Ok(Self { client })
    }

    pub async fn fetch_features_into<F>(
        &mut self,
        user_ids: Vec<String>,
        features: Vec<(String, Option<i16>, Option<i16>)>,
        mut callback: F,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        F: FnMut(&[f32]),
    {
        let ticket = create_fbs_ticket(user_ids, features)?;
        let ticket = Ticket {
            ticket: ticket.into(),
        };
        
        let stream = self.client.do_get(ticket).await?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;

        for rb in batches {
            for i in 0..rb.num_rows() {
                for field in rb.columns() {
                    if let Some(list_array) = field.as_any().downcast_ref::<ListArray>() {
                        if let Some(values) = list_array.value(i).as_any().downcast_ref::<Float32Array>() {
                            callback(values.values());
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
