use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use inindexer::{
    near_indexer_primitives::types::AccountId, neardata_server::NeardataServerProvider,
    run_indexer, BlockIterator, IndexerOptions, PreprocessTransactionsSettings,
};
use near_jsonrpc_client::JsonRpcClient;
use tokio::sync::RwLock;

use crate::{
    meme_cooking::MemeCookingCreateMemeEvent, ContractEventHandler, EventContext,
    HandledTokensStorage, NewTokenIndexer, RPC_URL,
};

#[derive(Default)]
struct TestHandler {
    nep141_events: HashMap<AccountId, Vec<EventContext>>,
    memecooking_events: HashMap<u64, Vec<(MemeCookingCreateMemeEvent, EventContext)>>,
    testnet: bool,
}

#[async_trait]
impl ContractEventHandler for TestHandler {
    async fn handle_new_nep141(&mut self, account_id: AccountId, context: EventContext) {
        self.nep141_events
            .entry(account_id)
            .or_default()
            .push(context);
    }

    async fn handle_meme_cooking_new_meme(
        &mut self,
        event: MemeCookingCreateMemeEvent,
        context: EventContext,
    ) {
        self.memecooking_events
            .entry(event.meme_id)
            .or_default()
            .push((event, context));
    }

    fn is_testnet(&self) -> bool {
        self.testnet
    }
}

#[derive(Default)]
struct TestStorage {
    handled_accounts: RwLock<HashSet<AccountId>>,
}

#[async_trait]
impl HandledTokensStorage for TestStorage {
    async fn is_already_indexed(&self, account_id: &AccountId) -> bool {
        self.handled_accounts.read().await.contains(account_id)
    }

    async fn mark_handled(&mut self, account_id: AccountId) {
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
    );

    run_indexer(
        &mut indexer,
        NeardataServerProvider::mainnet(),
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
    );

    run_indexer(
        &mut indexer,
        NeardataServerProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(117_103_395..=117_103_395),
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
            .get(&"token.axisorder.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![
            (EventContext {
                transaction_id: "AZRtBbBR56JvVsXz1LKVtujZMBgL1Wks1qgHbxvB5k9k"
                    .parse()
                    .unwrap(),
                receipt_id: "HYtRzbAoj3Roupo5hLCTsVZUvL985uvgN4tkxFLCp4k3"
                    .parse()
                    .unwrap(),
                block_height: 117103395,
                block_timestamp_nanosec: 1713441692153402000,
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
    );

    run_indexer(
        &mut indexer,
        NeardataServerProvider::mainnet(),
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

    assert!(indexer.handler.nep141_events.is_empty());
}

#[tokio::test]
async fn detects_meme_cooking() {
    let handler = TestHandler {
        testnet: true,
        ..Default::default()
    };

    let mut indexer = NewTokenIndexer::new(
        handler,
        JsonRpcClient::connect(RPC_URL),
        TestStorage::default(),
    );

    run_indexer(
        &mut indexer,
        NeardataServerProvider::testnet(),
        IndexerOptions {
            range: BlockIterator::iterator(170_108_186..=170_108_188),
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
        *indexer.handler.memecooking_events.get(&51).unwrap(),
        vec![(
            MemeCookingCreateMemeEvent {
                meme_id: 51,
                owner: "hardflour4957.testnet".parse().unwrap(),
                end_timestamp_ms: 1722185767939,
                name: "faew".to_string(),
                symbol: "gwefw".to_string(),
                decimals: 24,
                total_supply: 1000000000000000000000000000000000,
                reference: "Qmcq2f32ECkN8FSVVSYVEcVmxVRHXUL4JFLUccBhaWcGWJ".to_string(),
                reference_hash: "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=".to_string(),
                deposit_token_id: "wrap.testnet".parse().unwrap()
            },
            EventContext {
                transaction_id: "DJnFGNg7TXD7nGVWy1GtwpJHRzDLJWHEpXnS6aTREzUW"
                    .parse()
                    .unwrap(),
                receipt_id: "HQfrrEu2j2RJbtwN9phumUhtnWYE1UFPZJvC8BvQVDKq"
                    .parse()
                    .unwrap(),
                block_height: 170108187,
                block_timestamp_nanosec: 1722185467939074943
            }
        )]
    );
}
