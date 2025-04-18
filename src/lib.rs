pub mod new_nep141;
pub mod new_nep171;
pub mod redis_handler;
#[cfg(test)]
mod tests;
pub mod txt_file_storage;

use std::sync::Arc;

use async_trait::async_trait;
use inindexer::near_indexer_primitives::types::AccountId;
use inindexer::near_indexer_primitives::types::BlockHeight;
use inindexer::near_indexer_primitives::views::ExecutionStatusView;
use inindexer::near_indexer_primitives::CryptoHash;
use inindexer::near_indexer_primitives::StreamerMessage;
use inindexer::IncompleteTransaction;
use inindexer::Indexer;
use inindexer::TransactionReceipt;
use near_min_api::RpcClient;
use new_nep141::HandledNep141TokensStorage;
use new_nep141::Nep141Indexer;
use new_nep171::HandledNep171TokensStorage;
use new_nep171::Nep171Indexer;

#[async_trait]
pub trait ContractEventHandler: Send + Sync {
    async fn handle_new_nep141(&self, account_id: AccountId, context: EventContext);
    async fn handle_new_nep171(&self, account_id: AccountId, context: EventContext);

    /// Called after each block
    async fn flush_events(&self, block_height: BlockHeight);
}

pub struct NewTokenIndexer<T: ContractEventHandler> {
    pub handler: Arc<T>,
    pub nep141_indexer: Nep141Indexer,
    pub nep171_indexer: Nep171Indexer,
}

impl<T: ContractEventHandler> NewTokenIndexer<T> {
    pub fn new(
        handler: T,
        rpc_client: RpcClient,
        handled_nep141_accounts: impl HandledNep141TokensStorage + 'static,
        handled_nep171_accounts: impl HandledNep171TokensStorage + 'static,
    ) -> Self {
        Self {
            handler: Arc::new(handler),
            nep141_indexer: Nep141Indexer::new(rpc_client.clone(), handled_nep141_accounts),
            nep171_indexer: Nep171Indexer::new(rpc_client.clone(), handled_nep171_accounts),
        }
    }
}

#[async_trait]
impl<T: ContractEventHandler + 'static> Indexer for NewTokenIndexer<T> {
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

        self.nep141_indexer
            .detect_nep141(receipt, tx, block, Arc::clone(&self.handler))
            .await;

        self.nep171_indexer
            .detect_nep171(receipt, tx, block, Arc::clone(&self.handler))
            .await;

        Ok(())
    }

    async fn process_block_end(&mut self, block: &StreamerMessage) -> Result<(), Self::Error> {
        self.handler.flush_events(block.block.header.height).await;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct EventContext {
    pub transaction_id: CryptoHash,
    pub receipt_id: CryptoHash,
    pub block_height: BlockHeight,
    pub block_timestamp_nanosec: u128,
}
