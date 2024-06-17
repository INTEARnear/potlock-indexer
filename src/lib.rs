pub mod redis_handler;
#[cfg(test)]
mod tests;

use async_trait::async_trait;
use inindexer::near_indexer_primitives::types::AccountId;
use inindexer::near_indexer_primitives::types::Balance;
use inindexer::near_indexer_primitives::types::BlockHeight;
use inindexer::near_indexer_primitives::views::ActionView;
use inindexer::near_indexer_primitives::views::ExecutionStatusView;
use inindexer::near_indexer_primitives::views::ReceiptEnumView;
use inindexer::near_indexer_primitives::CryptoHash;
use inindexer::near_indexer_primitives::StreamerMessage;
use inindexer::near_utils::dec_format;
use inindexer::near_utils::EventLogData;
use inindexer::Indexer;
use inindexer::TransactionReceipt;
use inindexer::{CompleteTransaction, IncompleteTransaction};
use serde::Deserialize;

pub type DonationId = u64;
pub type ProjectId = AccountId;
pub type TimestampMs = u64;

#[async_trait]
pub trait PotlockEventHandler: Send + Sync {
    async fn handle_donation(&mut self, event: DonationEvent, context: EventContext);
    async fn handle_pot_project_donation(
        &mut self,
        event: PotProjectDonationEvent,
        context: EventContext,
    );
    async fn handle_pot_donation(&mut self, event: PotDonationEvent, context: EventContext);
}

pub struct PotlockIndexer<T: PotlockEventHandler>(pub T);

#[async_trait]
impl<T: PotlockEventHandler + 'static> Indexer for PotlockIndexer<T> {
    type Error = anyhow::Error;

    async fn on_transaction(
        &mut self,
        tx: &CompleteTransaction,
        block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        for receipt in tx.receipts.iter() {
            if let ReceiptEnumView::Action { actions, .. } = &receipt.receipt.receipt.receipt {
                for action in actions.iter() {
                    if let ActionView::FunctionCall { method_name, .. } = action {
                        if method_name == "donate"
                            && receipt
                                .receipt
                                .receipt
                                .receiver_id
                                .as_str()
                                .ends_with(".v1.potfactory.potlock.near")
                        {
                            if let Some(result) = get_result(receipt, tx) {
                                if let Ok(donation) =
                                    serde_json::from_slice::<PotDonationExternal>(result)
                                {
                                    let context = EventContext {
                                        transaction_id: tx.transaction.transaction.hash,
                                        receipt_id: receipt.receipt.receipt.receipt_id,
                                        block_height: block.block.header.height,
                                        block_timestamp_nanosec: block
                                            .block
                                            .header
                                            .timestamp_nanosec
                                            as u128,
                                    };
                                    if let Some(project_id) = donation.project_id {
                                        let event = PotProjectDonationEvent {
                                            donation_id: donation.id,
                                            pot_id: receipt.receipt.receipt.receiver_id.clone(),
                                            donor_id: donation.donor_id,
                                            total_amount: donation.total_amount,
                                            net_amount: donation.net_amount,
                                            message: donation.message.and_then(|msg| {
                                                if msg.is_empty() {
                                                    None
                                                } else {
                                                    Some(msg)
                                                }
                                            }),
                                            donated_at: donation.donated_at,
                                            project_id,
                                            referrer_id: donation.referrer_id,
                                            referrer_fee: donation.referrer_fee,
                                            protocol_fee: donation.protocol_fee,
                                            chef_id: donation.chef_id,
                                            chef_fee: donation.chef_fee,
                                        };
                                        self.0.handle_pot_project_donation(event, context).await;
                                    } else {
                                        let event = PotDonationEvent {
                                            donation_id: donation.id,
                                            pot_id: receipt.receipt.receipt.receiver_id.clone(),
                                            donor_id: donation.donor_id,
                                            total_amount: donation.total_amount,
                                            net_amount: donation.net_amount,
                                            message: donation.message.and_then(|msg| {
                                                if msg.is_empty() {
                                                    None
                                                } else {
                                                    Some(msg)
                                                }
                                            }),
                                            donated_at: donation.donated_at,
                                            referrer_id: donation.referrer_id,
                                            referrer_fee: donation.referrer_fee,
                                            protocol_fee: donation.protocol_fee,
                                            chef_id: donation.chef_id,
                                            chef_fee: donation.chef_fee,
                                        };
                                        self.0.handle_pot_donation(event, context).await;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn on_receipt(
        &mut self,
        receipt: &TransactionReceipt,
        tx: &IncompleteTransaction,
        _block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        if receipt.receipt.receipt.receiver_id == "donate.potlock.near" {
            for log in receipt.receipt.execution_outcome.outcome.logs.iter() {
                if let Ok(log) = EventLogData::<Vec<DonationLogWrapper>>::deserialize(log) {
                    if log.event == "donation" && log.standard == "potlock" {
                        for donation in log.data {
                            let donation = donation.donation;
                            let event = DonationEvent {
                                donation_id: donation.id,
                                donor_id: donation.donor_id,
                                total_amount: donation.total_amount,
                                ft_id: donation.ft_id,
                                message: donation.message.and_then(|msg| {
                                    if msg.is_empty() {
                                        None
                                    } else {
                                        Some(msg)
                                    }
                                }),
                                donated_at: donation.donated_at_ms,
                                project_id: donation.recipient_id,
                                protocol_fee: donation.protocol_fee,
                                referrer_id: donation.referrer_id,
                                referrer_fee: donation.referrer_fee,
                            };
                            let context = EventContext {
                                transaction_id: tx.transaction.transaction.hash,
                                receipt_id: receipt.receipt.receipt.receipt_id,
                                block_height: receipt.block_height,
                                block_timestamp_nanosec: receipt.block_timestamp_nanosec,
                            };
                            self.0.handle_donation(event, context).await;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[derive(Deserialize, Debug)]
pub struct DonationLogWrapper {
    pub donation: DonationExternal,
}

/// https://github.com/PotLock/core/blob/cda438fd3f7a0aea06a4e435d7ecebfeb6e172a5/contracts/donation/src/donations.rs#L86
#[derive(Deserialize, Debug)]
pub struct DonationExternal {
    /// Unique identifier for the donation
    pub id: DonationId,
    /// ID of the donor
    pub donor_id: AccountId,
    /// Amount donated
    #[serde(with = "dec_format")]
    pub total_amount: Balance,
    /// FT id (e.g. "near")
    pub ft_id: AccountId,
    /// Optional message from the donor
    pub message: Option<String>,
    /// Timestamp when the donation was made
    pub donated_at_ms: TimestampMs,
    /// ID of the account receiving the donation
    pub recipient_id: AccountId,
    /// Protocol fee
    #[serde(with = "dec_format")]
    pub protocol_fee: Balance,
    /// Referrer ID
    pub referrer_id: Option<AccountId>,
    /// Referrer fee
    #[serde(with = "dec_format")]
    pub referrer_fee: Option<Balance>,
}

/// https://github.com/PotLock/core/blob/cda438fd3f7a0aea06a4e435d7ecebfeb6e172a5/contracts/pot/src/donations.rs#L51
#[derive(Deserialize, Debug)]
pub struct PotDonationExternal {
    /// ID of the donation
    pub id: DonationId,
    /// ID of the donor
    pub donor_id: AccountId,
    /// Amount donated
    #[serde(with = "dec_format")]
    pub total_amount: Balance,
    /// Amount after all fees/expenses (incl. storage)
    #[serde(with = "dec_format")]
    pub net_amount: Balance,
    /// Optional message from the donor
    pub message: Option<String>,
    /// Timestamp when the donation was made
    pub donated_at: TimestampMs,
    /// ID of the project receiving the donation, if applicable (matching pool donations will contain `None`)
    pub project_id: Option<ProjectId>,
    /// Referrer ID
    pub referrer_id: Option<AccountId>,
    /// Referrer fee
    #[serde(with = "dec_format")]
    pub referrer_fee: Option<Balance>,
    /// Protocol fee
    #[serde(with = "dec_format")]
    pub protocol_fee: Balance,
    /// Indicates whether this is matching pool donation
    pub matching_pool: bool,
    /// Chef ID
    pub chef_id: Option<AccountId>,
    /// Chef fee
    #[serde(with = "dec_format")]
    pub chef_fee: Option<u128>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DonationEvent {
    /// ID of the donation
    pub donation_id: DonationId,
    /// ID of the donor
    pub donor_id: AccountId,
    /// Amount donated
    pub total_amount: Balance,
    /// FT id (e.g. "near")
    pub ft_id: AccountId,
    /// Optional message from the donor
    pub message: Option<String>,
    /// Timestamp when the donation was made
    pub donated_at: TimestampMs,
    /// ID of the project receiving the donation
    pub project_id: AccountId,
    /// Protocol fee
    pub protocol_fee: Balance,
    /// Referrer ID
    pub referrer_id: Option<AccountId>,
    /// Referrer fee
    pub referrer_fee: Option<Balance>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PotProjectDonationEvent {
    /// ID of the donation
    pub donation_id: DonationId,
    /// ID of the pot
    pub pot_id: AccountId,
    /// ID of the donor
    pub donor_id: AccountId,
    /// Amount donated
    pub total_amount: Balance,
    /// Amount after all fees/expenses (incl. storage)
    pub net_amount: Balance,
    /// Optional message from the donor
    pub message: Option<String>,
    /// Timestamp when the donation was made
    pub donated_at: TimestampMs,
    /// ID of the project receiving the donation
    pub project_id: AccountId,
    /// Referrer ID
    pub referrer_id: Option<AccountId>,
    /// Referrer fee
    pub referrer_fee: Option<Balance>,
    /// Protocol fee
    pub protocol_fee: Balance,
    /// Chef ID
    pub chef_id: Option<AccountId>,
    /// Chef fee
    pub chef_fee: Option<Balance>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PotDonationEvent {
    /// ID of the donation
    pub donation_id: DonationId,
    /// ID of the pot
    pub pot_id: AccountId,
    /// ID of the donor
    pub donor_id: AccountId,
    /// Amount donated
    pub total_amount: Balance,
    /// Amount after all fees/expenses (incl. storage)
    pub net_amount: Balance,
    /// Optional message from the donor
    pub message: Option<String>,
    /// Timestamp when the donation was made
    pub donated_at: TimestampMs,
    /// Referrer ID
    pub referrer_id: Option<AccountId>,
    /// Referrer fee
    pub referrer_fee: Option<Balance>,
    /// Protocol fee
    pub protocol_fee: Balance,
    /// Chef ID
    pub chef_id: Option<AccountId>,
    /// Chef fee
    pub chef_fee: Option<Balance>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EventContext {
    pub transaction_id: CryptoHash,
    pub receipt_id: CryptoHash,
    /// In the event of pot or pot project donation, represents the block when the
    /// transaction was completed, not the donation receipt
    pub block_height: BlockHeight,
    pub block_timestamp_nanosec: u128,
}

fn get_result<'a>(
    receipt: &'a TransactionReceipt,
    tx: &'a CompleteTransaction,
) -> Option<&'a Vec<u8>> {
    match &receipt.receipt.execution_outcome.outcome.status {
        ExecutionStatusView::Failure(_) | ExecutionStatusView::Unknown => None,
        ExecutionStatusView::SuccessReceiptId(receipt_id) => get_result(
            tx.receipts
                .iter()
                .find(|r| r.receipt.receipt.receipt_id == *receipt_id)?,
            tx,
        ),
        ExecutionStatusView::SuccessValue(value) => Some(value),
    }
}
