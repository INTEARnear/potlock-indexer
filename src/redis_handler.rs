use async_trait::async_trait;
use chrono::DateTime;
use inevents_redis::RedisEventStream;
use intear_events::events::potlock::{potlock_donation::PotlockDonationEventData, potlock_pot_donation::PotlockPotDonationEventData, potlock_pot_project_donation::PotlockPotProjectDonationEventData};
use redis::aio::ConnectionManager;

use crate::{
    DonationEvent, EventContext, PotDonationEvent, PotProjectDonationEvent, PotlockEventHandler,
};

pub struct PushToRedisStream {
    donation_stream: RedisEventStream<PotlockDonationEventData>,
    pot_project_donation_stream: RedisEventStream<PotlockPotProjectDonationEventData>,
    pot_donation_stream: RedisEventStream<PotlockPotDonationEventData>,
    max_stream_size: usize,
}

impl PushToRedisStream {
    pub async fn new(connection: ConnectionManager, max_stream_size: usize) -> Self {
        Self {
            donation_stream: RedisEventStream::new(connection.clone(), "potlock_donation").await,
            pot_project_donation_stream: RedisEventStream::new(
                connection.clone(),
                "potlock_pot_project_donation",
            )
            .await,
            pot_donation_stream: RedisEventStream::new(connection.clone(), "potlock_pot_donation")
                .await,
            max_stream_size,
        }
    }
}

#[async_trait]
impl PotlockEventHandler for PushToRedisStream {
    async fn handle_donation(&mut self, event: DonationEvent, context: EventContext) {
        self.donation_stream
            .emit_event(context.block_height, PotlockDonationEventData {
                donation_id: event.donation_id,
                donor_id: event.donor_id,
                total_amount: event.total_amount,
                ft_id: event.ft_id,
                message: event.message,
                donated_at: DateTime::from_timestamp_millis(event.donated_at as i64).unwrap(),
                project_id: event.project_id,
                protocol_fee: event.protocol_fee,
                referrer_id: event.referrer_id,
                referrer_fee: event.referrer_fee,

                transaction_id: context.transaction_id,
                receipt_id: context.receipt_id,
                block_height: context.block_height,
                block_timestamp_nanosec: context.block_timestamp_nanosec,
            }, self.max_stream_size)
            .await
            .expect("Failed to emit donation event");
    }

    async fn handle_pot_project_donation(
        &mut self,
        event: PotProjectDonationEvent,
        context: EventContext,
    ) {
        self.pot_project_donation_stream
            .emit_event(context.block_height, PotlockPotProjectDonationEventData {
                donation_id: event.donation_id,
                pot_id: event.pot_id,
                donor_id: event.donor_id,
                total_amount: event.total_amount,
                net_amount: event.net_amount,
                message: event.message,
                donated_at: DateTime::from_timestamp_millis(event.donated_at as i64).unwrap(),
                project_id: event.project_id,
                protocol_fee: event.protocol_fee,
                referrer_id: event.referrer_id,
                referrer_fee: event.referrer_fee,
                chef_id: event.chef_id,
                chef_fee: event.chef_fee,

                transaction_id: context.transaction_id,
                receipt_id: context.receipt_id,
                block_height: context.block_height,
                block_timestamp_nanosec: context.block_timestamp_nanosec,
            }, self.max_stream_size)
            .await
            .expect("Failed to emit pot project donation event");
    }

    async fn handle_pot_donation(&mut self, event: PotDonationEvent, context: EventContext) {
        self.pot_donation_stream
            .emit_event(context.block_height, PotlockPotDonationEventData {
                donation_id: event.donation_id,
                pot_id: event.pot_id,
                donor_id: event.donor_id,
                total_amount: event.total_amount,
                net_amount: event.net_amount,
                message: event.message,
                donated_at: DateTime::from_timestamp_millis(event.donated_at as i64).unwrap(),
                protocol_fee: event.protocol_fee,
                referrer_id: event.referrer_id,
                referrer_fee: event.referrer_fee,
                chef_id: event.chef_id,
                chef_fee: event.chef_fee,

                transaction_id: context.transaction_id,
                receipt_id: context.receipt_id,
                block_height: context.block_height,
                block_timestamp_nanosec: context.block_timestamp_nanosec,
            }, self.max_stream_size)
            .await
            .expect("Failed to emit pot donation event");
    }
}
