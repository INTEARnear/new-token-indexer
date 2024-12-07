use std::sync::Arc;

use async_trait::async_trait;
use inevents_redis::RedisEventStream;
use inindexer::near_indexer_primitives::types::{AccountId, BlockHeight};
use intear_events::events::newcontract::meme_cooking_token::NewMemeCookingTokenEvent;
use intear_events::events::newcontract::{
    meme_cooking_meme::NewMemeCookingMemeEvent, nep141::NewContractNep141Event,
};
use redis::aio::ConnectionManager;
use tokio::sync::Mutex as TokioMutex;

use crate::meme_cooking::MemeCookingCreateTokenEvent;
use crate::{meme_cooking::MemeCookingCreateMemeEvent, ContractEventHandler, EventContext};

pub struct PushToRedisStream {
    nep141_stream: Arc<TokioMutex<RedisEventStream<NewContractNep141Event>>>,
    meme_cooking_meme_stream: Arc<TokioMutex<RedisEventStream<NewMemeCookingMemeEvent>>>,
    meme_cooking_token_stream: Arc<TokioMutex<RedisEventStream<NewMemeCookingTokenEvent>>>,
    max_stream_size: usize,
    testnet: bool,
}

impl PushToRedisStream {
    pub async fn new(connection: ConnectionManager, max_stream_size: usize, testnet: bool) -> Self {
        Self {
            nep141_stream: Arc::new(TokioMutex::new(RedisEventStream::new(
                connection.clone(),
                if testnet {
                    format!("{}_testnet", NewContractNep141Event::ID)
                } else {
                    NewContractNep141Event::ID.to_string()
                },
            ))),
            meme_cooking_meme_stream: Arc::new(TokioMutex::new(RedisEventStream::new(
                connection.clone(),
                if testnet {
                    format!("{}_testnet", NewMemeCookingMemeEvent::ID)
                } else {
                    NewMemeCookingMemeEvent::ID.to_string()
                },
            ))),
            meme_cooking_token_stream: Arc::new(TokioMutex::new(RedisEventStream::new(
                connection.clone(),
                if testnet {
                    format!("{}_testnet", NewMemeCookingTokenEvent::ID)
                } else {
                    NewMemeCookingTokenEvent::ID.to_string()
                },
            ))),
            max_stream_size,
            testnet,
        }
    }
}

#[async_trait]
impl ContractEventHandler for PushToRedisStream {
    async fn handle_new_nep141(&self, account_id: AccountId, context: EventContext) {
        self.nep141_stream
            .lock()
            .await
            .add_event(NewContractNep141Event {
                account_id,
                transaction_id: context.transaction_id,
                receipt_id: context.receipt_id,
                block_height: context.block_height,
                block_timestamp_nanosec: context.block_timestamp_nanosec,
            });
    }

    async fn handle_meme_cooking_new_meme(
        &self,
        event: MemeCookingCreateMemeEvent,
        context: EventContext,
    ) {
        self.meme_cooking_meme_stream
            .lock()
            .await
            .add_event(NewMemeCookingMemeEvent {
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
                soft_cap: event.soft_cap,
                hard_cap: event.hard_cap,
            });
    }

    async fn handle_meme_cooking_new_token(
        &self,
        event: MemeCookingCreateTokenEvent,
        context: EventContext,
    ) {
        self.meme_cooking_token_stream
            .lock()
            .await
            .add_event(NewMemeCookingTokenEvent {
                transaction_id: context.transaction_id,
                receipt_id: context.receipt_id,
                block_height: context.block_height,
                block_timestamp_nanosec: context.block_timestamp_nanosec,

                meme_id: event.meme_id,
                token_id: event.token_id,
                total_supply: event.total_supply,
                pool_id: event.pool_id,
            });
    }

    fn is_testnet(&self) -> bool {
        self.testnet
    }

    async fn flush_events(&self, block_height: BlockHeight) {
        self.nep141_stream
            .lock()
            .await
            .flush_events(block_height, self.max_stream_size)
            .await
            .expect("Failed to flush nep141 stream");
        self.meme_cooking_meme_stream
            .lock()
            .await
            .flush_events(block_height, self.max_stream_size)
            .await
            .expect("Failed to flush meme cooking meme stream");
        self.meme_cooking_token_stream
            .lock()
            .await
            .flush_events(block_height, self.max_stream_size)
            .await
            .expect("Failed to flush meme cooking token stream");
    }
}
