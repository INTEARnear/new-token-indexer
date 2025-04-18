use inindexer::neardata::NeardataProvider;

use inindexer::{
    run_indexer, AutoContinue, BlockRange, IndexerOptions, PreprocessTransactionsSettings,
};
use near_min_api::RpcClient;
use new_token_indexer::{
    redis_handler::PushToRedisStream, txt_file_storage::TxtFileStorage, NewTokenIndexer,
};
use redis::aio::ConnectionManager;

pub const RPC_URL: &str = "https://archival-rpc.mainnet.near.org";

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
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

    let mut indexer = NewTokenIndexer::new(
        PushToRedisStream::new(connection, 1_000).await,
        RpcClient::new([std::env::var("RPC_URL").unwrap_or(RPC_URL.to_string())]),
        TxtFileStorage::new("known_tokens.txt").await,
        TxtFileStorage::new("known_nft_tokens.txt").await,
    );

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
