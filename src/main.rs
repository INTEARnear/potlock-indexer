use inindexer::neardata::NeardataProvider;

use inindexer::{
    run_indexer, AutoContinue, BlockRange, IndexerOptions, PreprocessTransactionsSettings,
};
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

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 20,
                postfetch_blocks: 20,
            }),
            ..IndexerOptions::default_with_range(if std::env::args().len() > 1 {
                // For debugging
                let msg = "Usage: `indexer` or `indexer [start-block] [end-block]`";
                BlockRange::Range {
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
                }
            } else {
                BlockRange::AutoContinue(AutoContinue::default())
            })
        },
    )
    .await
    .expect("Indexer run failed");
}
