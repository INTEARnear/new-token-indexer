use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use inindexer::{
    near_indexer_primitives::{
        types::{AccountId, BlockHeight, BlockId, BlockReference},
        views::{ActionView, QueryRequest, ReceiptEnumView},
        StreamerMessage,
    },
    near_utils::{EventLogData, FtBurnLog, FtMintLog, FtTransferLog},
    IncompleteTransaction, TransactionReceipt,
};
use near_jsonrpc_client::{methods, JsonRpcClient};

use crate::{ContractEventHandler, EventContext};

pub struct Nep141Indexer {
    storage: Arc<dyn HandledTokensStorage>,
    rpc_client: JsonRpcClient,
    last_checked_event: HashMap<AccountId, Instant>,
}

impl Nep141Indexer {
    pub fn new(rpc_client: JsonRpcClient, storage: impl HandledTokensStorage + 'static) -> Self {
        Self {
            rpc_client,
            storage: Arc::new(storage),
            last_checked_event: HashMap::new(),
        }
    }

    pub async fn detect_nep141<T: ContractEventHandler + 'static>(
        &mut self,
        receipt: &TransactionReceipt,
        tx: &IncompleteTransaction,
        block: &StreamerMessage,
        handler: Arc<T>,
    ) {
        if let ReceiptEnumView::Action { actions, .. } = &receipt.receipt.receipt.receipt {
            for action in actions.iter() {
                if let ActionView::DeployContract { .. } = action {
                    if !self
                        .storage
                        .is_already_indexed(&receipt.receipt.receipt.receiver_id)
                        .await
                    {
                        let storage = Arc::clone(&self.storage);
                        let handler = Arc::clone(&handler);
                        let rpc_client = self.rpc_client.clone();
                        let context = EventContext {
                            transaction_id: tx.transaction.transaction.hash,
                            receipt_id: receipt.receipt.receipt.receipt_id,
                            block_height: block.block.header.height,
                            block_timestamp_nanosec: block.block.header.timestamp_nanosec as u128,
                        };
                        let token_id = receipt.receipt.receipt.receiver_id.clone();
                        if is_nep141(&token_id, context.block_height, &rpc_client).await {
                            log::info!("Found NEP141: {token_id}");
                            storage.mark_handled(token_id.clone()).await;
                            handler.handle_new_nep141(token_id.clone(), context).await;
                        } else {
                            tokio::spawn(async move {
                                // Give RPC some time to catch up
                                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                                if !storage.is_already_indexed(&token_id).await
                                    && is_nep141(&token_id, context.block_height, &rpc_client).await
                                {
                                    log::info!("Found NEP141 with delay: {token_id}");
                                    storage.mark_handled(token_id.clone()).await;
                                    handler.handle_new_nep141(token_id.clone(), context).await;
                                }
                            });
                        }
                    }
                }
            }
        }

        if let Some(last_checked) = self
            .last_checked_event
            .get(&receipt.receipt.receipt.receiver_id)
        {
            const EVENT_CHECK_INTERVAL: Duration = Duration::from_secs(30 * 60);

            if last_checked.elapsed() < EVENT_CHECK_INTERVAL {
                return;
            }
        }

        for log in receipt.receipt.execution_outcome.outcome.logs.iter() {
            if EventLogData::<FtTransferLog>::deserialize(log).is_ok()
                || EventLogData::<FtBurnLog>::deserialize(log).is_ok()
                || EventLogData::<FtMintLog>::deserialize(log).is_ok()
            {
                self.last_checked_event
                    .insert(receipt.receipt.receipt.receiver_id.clone(), Instant::now());

                if !self
                    .storage
                    .is_already_indexed(&receipt.receipt.receipt.receiver_id)
                    .await
                    && is_nep141(
                        &receipt.receipt.receipt.receiver_id,
                        block.block.header.height,
                        &self.rpc_client,
                    )
                    .await
                {
                    self.storage
                        .mark_handled(receipt.receipt.receipt.receiver_id.clone())
                        .await;
                    let context = EventContext {
                        transaction_id: tx.transaction.transaction.hash,
                        receipt_id: receipt.receipt.receipt.receipt_id,
                        block_height: block.block.header.height,
                        block_timestamp_nanosec: block.block.header.timestamp_nanosec as u128,
                    };
                    handler
                        .handle_new_nep141(receipt.receipt.receipt.receiver_id.clone(), context)
                        .await;
                }
            }
        }
    }
}

async fn is_nep141(
    account_id: &AccountId,
    block_height: BlockHeight,
    rpc_client: &JsonRpcClient,
) -> bool {
    let metadata = rpc_client
        .call(methods::query::RpcQueryRequest {
            block_reference: BlockReference::BlockId(BlockId::Height(block_height)),
            request: QueryRequest::CallFunction {
                account_id: account_id.clone(),
                method_name: "ft_metadata".to_string(),
                args: serde_json::to_vec(&serde_json::json!({})).unwrap().into(),
            },
        })
        .await;
    metadata.is_ok()
}

#[async_trait]
pub trait HandledTokensStorage: Send + Sync {
    async fn is_already_indexed(&self, account_id: &AccountId) -> bool;
    async fn mark_handled(&self, account_id: AccountId);
}
