pub mod meme_cooking;
pub mod new_nep141;
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
use meme_cooking::MemeCookingCreateMemeEvent;
use meme_cooking::MemeCookingIndexer;
use near_jsonrpc_client::JsonRpcClient;
use new_nep141::HandledTokensStorage;
use new_nep141::Nep141Indexer;

#[async_trait]
pub trait ContractEventHandler: Send + Sync {
    async fn handle_new_nep141(&self, account_id: AccountId, context: EventContext);
    async fn handle_meme_cooking_new_meme(
        &self,
        event: MemeCookingCreateMemeEvent,
        context: EventContext,
    );
    fn is_testnet(&self) -> bool;
}

pub struct NewTokenIndexer<T: ContractEventHandler> {
    pub handler: Arc<T>,
    pub nep141_indexer: Nep141Indexer,
    pub meme_cooking_indexer: MemeCookingIndexer,
}

impl<T: ContractEventHandler> NewTokenIndexer<T> {
    pub fn new(
        handler: T,
        rpc_client: JsonRpcClient,
        handled_accounts: impl HandledTokensStorage + 'static,
    ) -> Self {
        Self {
            handler: Arc::new(handler),
            nep141_indexer: Nep141Indexer::new(rpc_client, handled_accounts),
            meme_cooking_indexer: MemeCookingIndexer,
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

        self.meme_cooking_indexer
            .detect_meme_cooking(receipt, tx, block, Arc::clone(&self.handler))
            .await;

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
