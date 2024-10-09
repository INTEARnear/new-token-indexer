const MEME_COOKING_CONTRACT_TESTNET: &str = "factory.v10.meme-cooking.testnet";
const MEME_COOKING_CONTRACT: &str = "meme-cooking.near";

use std::sync::Arc;

use inindexer::{
    near_indexer_primitives::{
        types::{AccountId, Balance},
        StreamerMessage,
    },
    near_utils::{dec_format, EventLogData},
    IncompleteTransaction, TransactionReceipt,
};
use serde::Deserialize;

use crate::{ContractEventHandler, EventContext};

pub struct MemeCookingIndexer;

impl MemeCookingIndexer {
    pub async fn detect_meme_cooking<T: ContractEventHandler>(
        &mut self,
        receipt: &TransactionReceipt,
        tx: &IncompleteTransaction,
        block: &StreamerMessage,
        handler: Arc<T>,
    ) {
        let meme_cooking_contract = if handler.is_testnet() {
            MEME_COOKING_CONTRACT_TESTNET
        } else {
            MEME_COOKING_CONTRACT
        };
        if receipt.receipt.receipt.receiver_id == meme_cooking_contract {
            for log in receipt.receipt.execution_outcome.outcome.logs.iter() {
                if let Ok(event) = EventLogData::<MemeCookingCreateMemeEvent>::deserialize(log) {
                    if event.standard == "meme-cooking" && event.event == "create_meme" {
                        let context = EventContext {
                            transaction_id: tx.transaction.transaction.hash,
                            receipt_id: receipt.receipt.receipt.receipt_id,
                            block_height: block.block.header.height,
                            block_timestamp_nanosec: block.block.header.timestamp_nanosec as u128,
                        };
                        handler
                            .handle_meme_cooking_new_meme(event.data, context)
                            .await;
                    }
                }
                if let Ok(event) = EventLogData::<MemeCookingCreateTokenEvent>::deserialize(log) {
                    if event.standard == "meme-cooking" && event.event == "create_token" {
                        let context = EventContext {
                            transaction_id: tx.transaction.transaction.hash,
                            receipt_id: receipt.receipt.receipt.receipt_id,
                            block_height: block.block.header.height,
                            block_timestamp_nanosec: block.block.header.timestamp_nanosec as u128,
                        };
                        handler
                            .handle_meme_cooking_new_token(event.data, context)
                            .await;
                    }
                }
            }
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct MemeCookingCreateMemeEvent {
    pub meme_id: u64,
    pub owner: AccountId,
    #[serde(with = "dec_format")]
    pub end_timestamp_ms: u64,
    pub name: String,
    pub symbol: String,
    pub decimals: u32,
    #[serde(with = "dec_format")]
    pub total_supply: Balance,
    pub reference: String,
    pub reference_hash: String,
    pub deposit_token_id: AccountId,
    #[serde(with = "dec_format")]
    pub soft_cap: Balance,
    #[serde(with = "dec_format")]
    pub hard_cap: Option<Balance>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct MemeCookingCreateTokenEvent {
    pub meme_id: u64,
    pub token_id: AccountId,
    #[serde(with = "dec_format")]
    pub total_supply: Balance,
    pub pool_id: u64,
}
