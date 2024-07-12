use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use inindexer::{
    near_indexer_primitives::types::AccountId, neardata_server::NeardataServerProvider,
    run_indexer, BlockIterator, IndexerOptions, PreprocessTransactionsSettings,
};
use near_jsonrpc_client::JsonRpcClient;
use tokio::sync::RwLock;

use crate::{ContractEventHandler, ContractIndexer, EventContext, HandledTokensStorage, RPC_URL};

#[derive(Default)]
struct TestHandler {
    events: HashMap<AccountId, Vec<EventContext>>,
}

#[async_trait]
impl ContractEventHandler for TestHandler {
    async fn handle_new_nep141(&mut self, account_id: AccountId, context: EventContext) {
        self.events.entry(account_id).or_default().push(context);
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

    async fn mark_handled(&self, account_id: AccountId) {
        self.handled_accounts.write().await.insert(account_id);
    }
}

#[tokio::test]
async fn detects_tkn_factory() {
    let handler = TestHandler::default();

    let mut indexer = ContractIndexer::new(
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
            .events
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

    let mut indexer = ContractIndexer::new(
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
            .events
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

    let mut indexer = ContractIndexer::new(
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

    assert!(indexer.handler.events.is_empty());
}
