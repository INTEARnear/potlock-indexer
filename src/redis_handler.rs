use async_trait::async_trait;
use redis::{streams::StreamMaxlen, AsyncCommands};

use crate::{
    DonationEvent, EventContext, PotDonationEvent, PotProjectDonationEvent, PotlockEventHandler,
};

pub struct PushToRedisStream<C: AsyncCommands + Sync> {
    connection: C,
    max_stream_size: usize,
}

impl<C: AsyncCommands + Sync> PushToRedisStream<C> {
    pub fn new(connection: C, max_stream_size: usize) -> Self {
        Self {
            connection,
            max_stream_size,
        }
    }
}

#[async_trait]
impl<C: AsyncCommands + Sync> PotlockEventHandler for PushToRedisStream<C> {
    async fn handle_pot_project_donation(
        &mut self,
        event: PotProjectDonationEvent,
        context: EventContext,
    ) {
        log::debug!("Pot project donation: {event:?}, context {context:?}");
        let response: String = self
            .connection
            .xadd_maxlen(
                "potlock_pot_project_donation",
                StreamMaxlen::Approx(self.max_stream_size),
                format!("{}-*", context.block_height),
                &[
                    (
                        "pot_project_donation",
                        serde_json::to_string(&event).unwrap(),
                    ),
                    ("context", serde_json::to_string(&context).unwrap()),
                ],
            )
            .await
            .unwrap();
        log::debug!("Adding to stream: {response}");
    }

    async fn handle_pot_donation(&mut self, event: PotDonationEvent, context: EventContext) {
        log::debug!("Pot donation: {event:?}, context {context:?}");
        let response: String = self
            .connection
            .xadd_maxlen(
                "potlock_pot_donation",
                StreamMaxlen::Approx(self.max_stream_size),
                format!("{}-*", context.block_height),
                &[
                    ("pot_donation", serde_json::to_string(&event).unwrap()),
                    ("context", serde_json::to_string(&context).unwrap()),
                ],
            )
            .await
            .unwrap();
        log::debug!("Adding to stream: {response}");
    }

    async fn handle_donation(&mut self, event: DonationEvent, context: EventContext) {
        log::debug!("Donation: {event:?}, context {context:?}");
        let response: String = self
            .connection
            .xadd_maxlen(
                "potlock_donation",
                StreamMaxlen::Approx(self.max_stream_size),
                format!("{}-*", context.block_height),
                &[
                    ("donation", serde_json::to_string(&event).unwrap()),
                    ("context", serde_json::to_string(&context).unwrap()),
                ],
            )
            .await
            .unwrap();
        log::debug!("Adding to stream: {response}");
    }
}
