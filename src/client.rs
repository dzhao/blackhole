pub mod ticket {
    use crate::embedding_generated::embedding::{Ticket, TicketArgs};
    use flatbuffers::FlatBufferBuilder;

    pub fn create_fbs_ticket(
        ids: Vec<String>,
        features: Vec<(String, Option<i16>, Option<i16>)>,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut builder = FlatBufferBuilder::new();
        
        let fb_ids: Vec<_> = ids.iter().map(|id| builder.create_string(id)).collect();
        let fb_ids = builder.create_vector(&fb_ids);

        let fb_features: Vec<_> = features.iter()
            .map(|(name, _, _)| builder.create_string(name))
            .collect();
        let fb_features = builder.create_vector(&fb_features);

        let fb_starts: Vec<u16> = features.iter()
            .map(|(_, start, _)| start.map(|v| v as u16).unwrap_or(0))
            .collect();
        let fb_starts = builder.create_vector(&fb_starts);

        let fb_ends: Vec<u16> = features.iter()
            .map(|(_, _, end)| end.map(|v| v as u16).unwrap_or(0))
            .collect();
        let fb_ends = builder.create_vector(&fb_ends);

        let ticket = Ticket::create(&mut builder, &TicketArgs {
            ids: Some(fb_ids),
            features: Some(fb_features),
            start: Some(fb_starts),
            end: Some(fb_ends),
        });

        builder.finish(ticket, None);
        Ok(builder.finished_data().to_vec())
    }
} 