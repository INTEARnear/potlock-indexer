use inindexer::neardata::NeardataProvider;

use inindexer::redb_storage::RedbStorage;
use inindexer::storage::IndexerStorage;
use inindexer::{run_indexer, AutoContinue, BlockRange, IndexerOptions};
use potlock_indexer::redis_handler::PushToRedisStream;
use redis::aio::ConnectionManager;

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .with_module_level("inindexer::performance", log::LevelFilter::Debug)
        .init()
        .unwrap();

    let client = redis::Client::open(
        std::env::var("REDIS_URL").expect("No $REDIS_URL environment variable set"),
    )
    .unwrap();
    let connection = ConnectionManager::new(client).await.unwrap();

    let mut indexer =
        potlock_indexer::PotlockIndexer(PushToRedisStream::new(connection, 1_000).await);

    // A debugging range runs on its own, so it keeps nothing between runs
    let (range, storage): (_, Option<Box<dyn IndexerStorage>>) = if std::env::args().len() > 1 {
        let msg = "Usage: `indexer` or `indexer [start-block] [end-block]`";
        let range = BlockRange::Range {
            start_inclusive: std::env::args()
                .nth(1)
                .expect(msg)
                .replace(['_', ',', ' ', '.'], "")
                .parse()
                .expect(msg),
            end_exclusive: Some(
                std::env::args()
                    .nth(2)
                    .expect(msg)
                    .replace(['_', ',', ' ', '.'], "")
                    .parse()
                    .expect(msg),
            ),
        };
        (range, None)
    } else {
        (
            BlockRange::AutoContinue(AutoContinue::default()),
            Some(Box::new(RedbStorage::new("transaction-trees.redb"))),
        )
    };

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            storage,
            warm_up_blocks: 100,
            ..IndexerOptions::default_with_range(range)
        },
    )
    .await
    .expect("Indexer run failed");
}
