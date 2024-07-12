#[cfg(test)]
mod ft_tests;
pub mod redis_handler;
pub mod txt_file_storage;

use async_trait::async_trait;
use inindexer::near_indexer_primitives::types::AccountId;
use inindexer::near_indexer_primitives::types::BlockHeight;
use inindexer::near_indexer_primitives::types::BlockId;
use inindexer::near_indexer_primitives::types::BlockReference;
use inindexer::near_indexer_primitives::types::Finality;
use inindexer::near_indexer_primitives::views::ActionView;
use inindexer::near_indexer_primitives::views::ExecutionStatusView;
use inindexer::near_indexer_primitives::views::QueryRequest;
use inindexer::near_indexer_primitives::views::ReceiptEnumView;
use inindexer::near_indexer_primitives::CryptoHash;
use inindexer::near_indexer_primitives::StreamerMessage;
use inindexer::IncompleteTransaction;
use inindexer::Indexer;
use inindexer::TransactionReceipt;
use near_jsonrpc_client::methods;
use near_jsonrpc_client::JsonRpcClient;

pub const RPC_URL: &str = "https://free.rpc.fastnear.com";

#[async_trait]
pub trait ContractEventHandler: Send + Sync {
    async fn handle_new_nep141(&mut self, account_id: AccountId, context: EventContext);
}

#[async_trait]
pub trait HandledTokensStorage: Send + Sync {
    async fn is_already_indexed(&self, account_id: &AccountId) -> bool;
    async fn mark_handled(&self, account_id: AccountId);
}

pub struct ContractIndexer<T: ContractEventHandler, S: HandledTokensStorage> {
    pub handler: T,
    rpc_client: JsonRpcClient,
    storage: S,
}

impl<T: ContractEventHandler, S: HandledTokensStorage> ContractIndexer<T, S> {
    pub fn new(handler: T, rpc_client: JsonRpcClient, handled_accounts: S) -> Self {
        Self {
            handler,
            rpc_client,
            storage: handled_accounts,
        }
    }
}

#[async_trait]
impl<T: ContractEventHandler + 'static, S: HandledTokensStorage + 'static> Indexer
    for ContractIndexer<T, S>
{
    type Error = anyhow::Error;

    async fn on_receipt(
        &mut self,
        receipt: &TransactionReceipt,
        tx: &IncompleteTransaction,
        block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        if !matches!(
            receipt.receipt.execution_outcome.outcome.status,
            ExecutionStatusView::SuccessReceiptId(_) | ExecutionStatusView::SuccessValue(_)
        ) {
            return Ok(());
        }
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
                        self.handler
                            .handle_new_nep141(receipt.receipt.receipt.receiver_id.clone(), context)
                            .await;
                    }
                }
            }
        }

        Ok(())
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

#[derive(Clone, Debug, PartialEq)]
pub struct EventContext {
    pub transaction_id: CryptoHash,
    pub receipt_id: CryptoHash,
    pub block_height: BlockHeight,
    pub block_timestamp_nanosec: u128,
}
