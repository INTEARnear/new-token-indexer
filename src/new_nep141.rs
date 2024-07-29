use async_trait::async_trait;
use inindexer::{
    near_indexer_primitives::{
        types::{AccountId, BlockHeight, BlockId, BlockReference, Finality},
        views::{ActionView, QueryRequest, ReceiptEnumView},
        StreamerMessage,
    },
    IncompleteTransaction, TransactionReceipt,
};
use near_jsonrpc_client::{methods, JsonRpcClient};

use crate::{ContractEventHandler, EventContext};

pub struct Nep141Indexer {
    storage: Box<dyn HandledTokensStorage>,
    rpc_client: JsonRpcClient,
}

impl Nep141Indexer {
    pub fn new(rpc_client: JsonRpcClient, storage: impl HandledTokensStorage + 'static) -> Self {
        Self {
            rpc_client,
            storage: Box::new(storage),
        }
    }

    pub async fn detect_nep141(
        &mut self,
        receipt: &TransactionReceipt,
        tx: &IncompleteTransaction,
        block: &StreamerMessage,
        handler: &mut dyn ContractEventHandler,
    ) {
        if let ReceiptEnumView::Action { actions, .. } = &receipt.receipt.receipt.receipt {
            for action in actions.iter() {
                if let ActionView::DeployContract { .. } = action {
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
                        log::info!("Found NEP141: {}", receipt.receipt.receipt.receiver_id);
                        let context = EventContext {
                            transaction_id: tx.transaction.transaction.hash,
                            receipt_id: receipt.receipt.receipt.receipt_id,
                            block_height: block.block.header.height,
                            block_timestamp_nanosec: block.block.header.timestamp_nanosec as u128,
                        };
                        self.storage
                            .mark_handled(receipt.receipt.receipt.receiver_id.clone())
                            .await;
                        handler
                            .handle_new_nep141(receipt.receipt.receipt.receiver_id.clone(), context)
                            .await;
                    }
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
            block_reference: if cfg!(test) || std::env::var("LATEST_BLOCK_META").is_ok() {
                // Old blocks may be garbage collected
                BlockReference::Finality(Finality::Final)
            } else {
                // If the block is relatively recent, it can't be garbage collected
                BlockReference::BlockId(BlockId::Height(block_height))
            },
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
    async fn mark_handled(&mut self, account_id: AccountId);
}
