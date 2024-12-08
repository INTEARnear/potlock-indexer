use std::collections::HashMap;

use async_trait::async_trait;
use inindexer::{
    near_indexer_primitives::types::{AccountId, BlockHeight},
    neardata::NeardataProvider,
    run_indexer, BlockIterator, IndexerOptions, PreprocessTransactionsSettings,
};

use crate::{
    DonationEvent, EventContext, PotDonationEvent, PotProjectDonationEvent, PotlockEventHandler,
    PotlockIndexer,
};

#[tokio::test]
async fn detects_pot_project_donations() {
    struct TestHandler {
        events: HashMap<AccountId, Vec<(PotProjectDonationEvent, EventContext)>>,
    }

    #[async_trait]
    impl PotlockEventHandler for TestHandler {
        async fn handle_pot_project_donation(
            &mut self,
            event: PotProjectDonationEvent,
            context: EventContext,
        ) {
            self.events
                .entry(event.donor_id.clone())
                .or_default()
                .push((event, context));
        }

        async fn handle_pot_donation(&mut self, _event: PotDonationEvent, _context: EventContext) {}

        async fn handle_donation(&mut self, _event: DonationEvent, _context: EventContext) {}

        async fn flush_events(&mut self, _block_height: BlockHeight) {}
    }

    let handler = TestHandler {
        events: HashMap::new(),
    };

    let mut indexer = PotlockIndexer(handler);

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(118_091_724..=118_091_729),
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(
        *indexer
            .0
            .events
            .get(&"slimedragon.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![(
            PotProjectDonationEvent {
                donation_id: 61,
                pot_id: "oss.v1.potfactory.potlock.near".parse().unwrap(),
                donor_id: "slimedragon.near".parse().unwrap(),
                total_amount: 100000000000000000000000,
                net_amount: 0,
                message: None,
                donated_at: 1714655073614,
                project_id: "nearcatalog.near".parse().unwrap(),
                referrer_id: None,
                referrer_fee: None,
                protocol_fee: 2000000000000000000000,
                chef_id: None,
                chef_fee: None
            },
            EventContext {
                transaction_id: "8iTg4kPTnLQneAmrPDF1iURs5UjrrZEHxsXkLbcJA4r3"
                    .parse()
                    .unwrap(),
                receipt_id: "DT7KfdougMFLjmNorQH9difBWRbnKzo9q1nL5oTDaFpS"
                    .parse()
                    .unwrap(),
                block_height: 118091729,
                block_timestamp_nanosec: 1714655076382528345,
            }
        )]
    );
}

#[tokio::test]
async fn detects_pot_donations() {
    struct TestHandler {
        events: HashMap<AccountId, Vec<(PotDonationEvent, EventContext)>>,
    }

    #[async_trait]
    impl PotlockEventHandler for TestHandler {
        async fn handle_pot_project_donation(
            &mut self,
            _event: PotProjectDonationEvent,
            _context: EventContext,
        ) {
        }

        async fn handle_pot_donation(&mut self, event: PotDonationEvent, context: EventContext) {
            self.events
                .entry(event.donor_id.clone())
                .or_default()
                .push((event, context));
        }

        async fn handle_donation(&mut self, _event: DonationEvent, _context: EventContext) {}

        async fn flush_events(&mut self, _block_height: BlockHeight) {}
    }

    let handler = TestHandler {
        events: HashMap::new(),
    };

    let mut indexer = PotlockIndexer(handler);

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(118_159_852..=118_159_854),
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(
        *indexer
            .0
            .events
            .get(&"slimedragon.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![(
            PotDonationEvent {
                donation_id: 83,
                pot_id: "oss.v1.potfactory.potlock.near".parse().unwrap(),
                donor_id: "slimedragon.near".parse().unwrap(),
                total_amount: 10000000000000000000000,
                net_amount: 0,
                message: Some("Testing gh/INTEARnear/potlock-indexer because it's hard to find existing transactions to test on".to_owned()),
                donated_at: 1714741415342, referrer_id: None, referrer_fee: None, protocol_fee: 0, chef_id: None, chef_fee: None
            },
            EventContext {
                transaction_id: "mGdDKMFHj7omhumVA8exQEvSZSjZNCDud7eBprVR87c".parse().unwrap(),
                receipt_id: "3CatRDqy1ZbL3Uo3p1GwVch4STyCooHNGUzzSLH9dQCa".parse().unwrap(),
                block_height: 118159854,
                block_timestamp_nanosec: 1714741416164254763,
            }
        )]
    );
}

#[tokio::test]
async fn detects_direct_donations() {
    struct TestHandler {
        events: HashMap<AccountId, Vec<(DonationEvent, EventContext)>>,
    }

    #[async_trait]
    impl PotlockEventHandler for TestHandler {
        async fn handle_pot_project_donation(
            &mut self,
            _event: PotProjectDonationEvent,
            _context: EventContext,
        ) {
        }

        async fn handle_pot_donation(&mut self, _event: PotDonationEvent, _context: EventContext) {}

        async fn handle_donation(&mut self, event: DonationEvent, context: EventContext) {
            self.events
                .entry(event.donor_id.clone())
                .or_default()
                .push((event, context));
        }

        async fn flush_events(&mut self, _block_height: BlockHeight) {}
    }

    let handler = TestHandler {
        events: HashMap::new(),
    };

    let mut indexer = PotlockIndexer(handler);

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(118_100_096..=118_100_103),
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(
        *indexer
            .0
            .events
            .get(&"xslymn.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![(
            DonationEvent {
                donation_id: 2211,
                donor_id: "xslymn.near".parse().unwrap(),
                total_amount: 500000000000000000000000,
                ft_id: "near".parse().unwrap(),
                message: None,
                donated_at: 1714665141842,
                project_id: "indexers.intear.near".parse().unwrap(),
                protocol_fee: 12500000000000000000000,
                referrer_id: None,
                referrer_fee: None
            },
            EventContext {
                transaction_id: "3VRcmqbc73KKNhPHaSWyaYyNz57c2QjDVTkTDRTXR6L7"
                    .parse()
                    .unwrap(),
                receipt_id: "DcTBNA52876rEw73fFaxZqWzkagatQj8MpWn2cQZU33X"
                    .parse()
                    .unwrap(),
                block_height: 118100099,
                block_timestamp_nanosec: 1714665143709556816,
            }
        )]
    );
}
