use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use inindexer::{
    near_indexer_primitives::types::{AccountId, BlockHeight},
    neardata::NeardataProvider,
    run_indexer, BlockIterator, IndexerOptions, PreprocessTransactionsSettings,
};
use near_jsonrpc_client::JsonRpcClient;
use tokio::sync::{Mutex, RwLock};

pub const RPC_URL: &str = "https://archival-rpc.mainnet.near.org";

use crate::new_nep171::HandledNep171TokensStorage;
use crate::{ContractEventHandler, EventContext, HandledNep141TokensStorage, NewTokenIndexer};

#[derive(Default)]
struct TestHandler {
    nep141_events: Mutex<HashMap<AccountId, Vec<EventContext>>>,
    nep171_events: Mutex<HashMap<AccountId, Vec<EventContext>>>,
}

#[async_trait]
impl ContractEventHandler for TestHandler {
    async fn handle_new_nep141(&self, account_id: AccountId, context: EventContext) {
        self.nep141_events
            .lock()
            .await
            .entry(account_id)
            .or_default()
            .push(context);
    }

    async fn handle_new_nep171(&self, account_id: AccountId, context: EventContext) {
        self.nep171_events
            .lock()
            .await
            .entry(account_id)
            .or_default()
            .push(context);
    }

    async fn flush_events(&self, _block_height: BlockHeight) {}
}

#[derive(Default)]
struct TestStorage {
    handled_accounts: RwLock<HashSet<AccountId>>,
}

#[async_trait]
impl HandledNep141TokensStorage for TestStorage {
    async fn is_already_indexed(&self, account_id: &AccountId) -> bool {
        self.handled_accounts.read().await.contains(account_id)
    }

    async fn mark_handled(&self, account_id: AccountId) {
        self.handled_accounts.write().await.insert(account_id);
    }
}

#[async_trait]
impl HandledNep171TokensStorage for TestStorage {
    async fn is_already_indexed(&self, account_id: &AccountId) -> bool {
        self.handled_accounts.read().await.contains(account_id)
    }

    async fn mark_handled(&self, account_id: AccountId) {
        self.handled_accounts.write().await.insert(account_id);
    }
}

#[tokio::test]
async fn detects_tkn_factory() {
    let handler = TestHandler::default();

    let mut indexer = NewTokenIndexer::new(
        handler,
        JsonRpcClient::connect(RPC_URL),
        TestStorage::default(),
        TestStorage::default(),
    );

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(114_625_047..=114_625_058),
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
            .handler
            .nep141_events
            .lock()
            .await
            .get(&"intel.tkn.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![
            (EventContext {
                transaction_id: "9SUSdf3rMfQi96znJ5DbjyMqhLud9G9bhVyMvogFaoNK"
                    .parse()
                    .unwrap(),
                receipt_id: "7MiLFpVunJQKKjzY6o2b58GDqyi1wG3W8f51QFBa83fm"
                    .parse()
                    .unwrap(),
                block_height: 114625057,
                block_timestamp_nanosec: 1710328781107609847,
            })
        ]
    );
}

#[tokio::test]
async fn detects_custom_token_contracts() {
    let handler = TestHandler::default();

    let mut indexer = NewTokenIndexer::new(
        handler,
        JsonRpcClient::connect(RPC_URL),
        TestStorage::default(),
        TestStorage::default(),
    );

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(124_593_976..=124_593_979),
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
            .handler
            .nep141_events
            .lock()
            .await
            .get(&"angry.tfactory.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![
            (EventContext {
                transaction_id: "AakvpHzUZkeHtXZuWV9La6Y3FTwPmDrjqUjyJfTWVyHD"
                    .parse()
                    .unwrap(),
                receipt_id: "EGv4bscgetNtNEi25eRHG5fFykAa4ZCS8AMVZK8iNz4K"
                    .parse()
                    .unwrap(),
                block_height: 124593978,
                block_timestamp_nanosec: 1722328121254503873,
            })
        ]
    );
}

#[tokio::test]
async fn does_not_detect_non_ft_contrats() {
    let handler = TestHandler::default();

    let mut indexer = NewTokenIndexer::new(
        handler,
        JsonRpcClient::connect(RPC_URL),
        TestStorage::default(),
        TestStorage::default(),
    );

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(116_538_111..=116_538_112),
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let mut events = indexer.handler.nep141_events.lock().await;
    events.retain(|token, _| token != "game.hot.tg" && token != "token.sweat");
    assert!(events.is_empty());
}

#[tokio::test]
async fn detects_mitte_meme() {
    let handler = TestHandler {
        ..Default::default()
    };

    let mut indexer = NewTokenIndexer::new(
        handler,
        JsonRpcClient::connect(RPC_URL),
        TestStorage::default(),
        TestStorage::default(),
    );

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(124_682_797..=124_682_800),
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
            .handler
            .nep141_events
            .lock()
            .await
            .get(&"catrump.token0.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![EventContext {
            transaction_id: "HE8m7RMcFADqV1HJ9PVa3xdtzYHYzSQMBFA2JwAzt7ZZ"
                .parse()
                .unwrap(),
            receipt_id: "96P7qPrKjhpsSS1tKctojRTUmPyTWEdr2nMmR8wULybW"
                .parse()
                .unwrap(),
            block_height: 124682799,
            block_timestamp_nanosec: 1722427998479776694
        }]
    );
}

#[tokio::test]
async fn detects_by_events() {
    let handler = TestHandler {
        ..Default::default()
    };

    let mut indexer = NewTokenIndexer::new(
        handler,
        JsonRpcClient::connect(RPC_URL),
        TestStorage::default(),
        TestStorage::default(),
    );

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(124_689_355..=124_689_357),
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
            .handler
            .nep141_events
            .lock()
            .await
            .get(&"token.honeybot.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![EventContext {
            transaction_id: "HJm31U2yLZ1WGPwokvkRWNZCp4yik6rMqREKJti625sq"
                .parse()
                .unwrap(),
            receipt_id: "GCzZi4thGK4XbiiALkDhquz3rqfnnJ81K2KNrFyUnAnP"
                .parse()
                .unwrap(),
            block_height: 124689356,
            block_timestamp_nanosec: 1722435140007941002
        }]
    );
}

#[tokio::test]
async fn detects_nep171() {
    let handler = TestHandler {
        ..Default::default()
    };

    let mut indexer = NewTokenIndexer::new(
        handler,
        JsonRpcClient::connect(RPC_URL),
        TestStorage::default(),
        TestStorage::default(),
    );

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(132_278_273..=132_278_275),
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
            .handler
            .nep171_events
            .lock()
            .await
            .get(&"nearvember-nft.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![EventContext {
            transaction_id: "6yUHqvocviZn4NC8dX4XSbTsnoVN8Av2PDZWjYpTWha6"
                .parse()
                .unwrap(),
            receipt_id: "24FqRrjxTDLsnVQLsFZUdxZ1wScMmb6NBqCjZDHGRVNo"
                .parse()
                .unwrap(),
            block_height: 132278273,
            block_timestamp_nanosec: 1731127836703239828
        }]
    );
}
