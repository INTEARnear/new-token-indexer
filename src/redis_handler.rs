use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use async_trait::async_trait;
use inevents_redis::RedisEventStream;
use inindexer::near_indexer_primitives::types::AccountId;
use intear_events::events::newcontract::meme_cooking_token::{
    NewMemeCookingTokenEvent, NewMemeCookingTokenEventData,
};
use intear_events::events::newcontract::{
    meme_cooking_meme::{NewMemeCookingMemeEvent, NewMemeCookingMemeEventData},
    nep141::{NewContractNep141Event, NewContractNep141EventData},
};
use redis::aio::ConnectionManager;

use crate::meme_cooking::MemeCookingCreateTokenEvent;
use crate::{meme_cooking::MemeCookingCreateMemeEvent, ContractEventHandler, EventContext};

pub struct PushToRedisStream {
    nep141_stream: RedisEventStream<NewContractNep141EventData>,
    meme_cooking_meme_stream: RedisEventStream<NewMemeCookingMemeEventData>,
    meme_cooking_token_stream: RedisEventStream<NewMemeCookingTokenEventData>,
    max_stream_size: usize,
    testnet: bool,
    // We sometimes give RPC 5 seconds to catch up, but if another token is created in the meantime, we don't
    // want to have "The ID specified in XADD is equal or smaller than the target stream top item" error
    latest_nep141_block: Arc<AtomicU64>,
}

impl PushToRedisStream {
    pub async fn new(connection: ConnectionManager, max_stream_size: usize, testnet: bool) -> Self {
        Self {
            nep141_stream: RedisEventStream::new(
                connection.clone(),
                if testnet {
                    format!("{}_testnet", NewContractNep141Event::ID)
                } else {
                    NewContractNep141Event::ID.to_string()
                },
            ),
            meme_cooking_meme_stream: RedisEventStream::new(
                connection.clone(),
                if testnet {
                    format!("{}_testnet", NewMemeCookingMemeEvent::ID)
                } else {
                    NewMemeCookingMemeEvent::ID.to_string()
                },
            ),
            meme_cooking_token_stream: RedisEventStream::new(
                connection.clone(),
                if testnet {
                    format!("{}_testnet", NewMemeCookingTokenEvent::ID)
                } else {
                    NewMemeCookingTokenEvent::ID.to_string()
                },
            ),
            max_stream_size,
            testnet,
            latest_nep141_block: Arc::new(AtomicU64::new(0)),
        }
    }
}

#[async_trait]
impl ContractEventHandler for PushToRedisStream {
    async fn handle_new_nep141(&self, account_id: AccountId, context: EventContext) {
        let latest_handled = self.latest_nep141_block.load(Ordering::Relaxed);
        self.nep141_stream
            .emit_event(
                if context.block_height >= latest_handled {
                    self.latest_nep141_block
                        .store(context.block_height, Ordering::Relaxed);
                    context.block_height
                } else {
                    latest_handled
                },
                NewContractNep141EventData {
                    account_id,

                    transaction_id: context.transaction_id,
                    receipt_id: context.receipt_id,
                    block_height: context.block_height,
                    block_timestamp_nanosec: context.block_timestamp_nanosec,
                },
                self.max_stream_size,
            )
            .await
            .expect("Failed to emit nep141 creation event");
    }

    async fn handle_meme_cooking_new_meme(
        &self,
        event: MemeCookingCreateMemeEvent,
        context: EventContext,
    ) {
        self.meme_cooking_meme_stream
            .emit_event(
                context.block_height,
                NewMemeCookingMemeEventData {
                    transaction_id: context.transaction_id,
                    receipt_id: context.receipt_id,
                    block_height: context.block_height,
                    block_timestamp_nanosec: context.block_timestamp_nanosec,

                    meme_id: event.meme_id,
                    owner: event.owner,
                    end_timestamp_ms: event.end_timestamp_ms,
                    name: event.name,
                    symbol: event.symbol,
                    decimals: event.decimals,
                    total_supply: event.total_supply,
                    reference: event.reference,
                    reference_hash: event.reference_hash,
                    deposit_token_id: event.deposit_token_id,
                },
                self.max_stream_size,
            )
            .await
            .expect("Failed to emit meme cooking event");
    }

    async fn handle_meme_cooking_new_token(
        &self,
        event: MemeCookingCreateTokenEvent,
        context: EventContext,
    ) {
        self.meme_cooking_token_stream
            .emit_event(
                context.block_height,
                NewMemeCookingTokenEventData {
                    transaction_id: context.transaction_id,
                    receipt_id: context.receipt_id,
                    block_height: context.block_height,
                    block_timestamp_nanosec: context.block_timestamp_nanosec,

                    meme_id: event.meme_id,
                    token_id: event.token_id,
                    total_supply: event.total_supply,
                    pool_id: event.pool_id,
                },
                self.max_stream_size,
            )
            .await
            .expect("Failed to emit meme cooking event");
    }

    fn is_testnet(&self) -> bool {
        self.testnet
    }
}
