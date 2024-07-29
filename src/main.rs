use inindexer::neardata_server::NeardataServerProvider;

use inindexer::{
    run_indexer, AutoContinue, BlockIterator, IndexerOptions, PreprocessTransactionsSettings,
};
use near_jsonrpc_client::JsonRpcClient;
use new_token_indexer::{
    redis_handler::PushToRedisStream, txt_file_storage::TxtFileStorage, NewTokenIndexer, RPC_URL,
};
use redis::aio::ConnectionManager;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .with_module_level("inindexer::performance", log::LevelFilter::Debug)
        .init()
        .unwrap();

    let is_testnet = std::env::var("TESTNET").is_ok();
    let client = redis::Client::open(
        std::env::var("REDIS_URL").expect("No $REDIS_URL environment variable set"),
    )
    .unwrap();
    let connection = ConnectionManager::new(client).await.unwrap();

    let mut indexer = NewTokenIndexer::new(
        PushToRedisStream::new(connection, 1_000, is_testnet).await,
        JsonRpcClient::connect(std::env::var("RPC_URL").unwrap_or(RPC_URL.to_string())),
        TxtFileStorage::new("known_tokens.txt").await,
    );

    run_indexer(
        &mut indexer,
        if is_testnet {
            NeardataServerProvider::testnet()
        } else {
            NeardataServerProvider::mainnet()
        },
        IndexerOptions {
            range: if std::env::args().len() > 1 {
                let msg =
                    "Usage: `contract-indexer` or `contract-indexer [start-block] [end-block]`";
                BlockIterator::iterator(
                    std::env::args()
                        .nth(1)
                        .expect(msg)
                        .replace(['_', ',', ' ', '.'], "")
                        .parse()
                        .expect(msg)
                        ..=std::env::args()
                            .nth(2)
                            .expect(msg)
                            .replace(['_', ',', ' ', '.'], "")
                            .parse()
                            .expect(msg),
                )
            } else {
                BlockIterator::AutoContinue(AutoContinue::default())
            },
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 20,
                postfetch_blocks: 20,
            }),
            ..Default::default()
        },
    )
    .await
    .expect("Indexer run failed");
}
