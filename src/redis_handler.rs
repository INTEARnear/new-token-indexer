use async_trait::async_trait;
use inevents_redis::RedisEventStream;
use inindexer::near_indexer_primitives::types::AccountId;
use intear_events::events::newcontract::{
    meme_cooking::{NewMemeCookingMemeEvent, NewMemeCookingMemeEventData},
    nep141::{NewContractNep141Event, NewContractNep141EventData},
};
use redis::aio::ConnectionManager;

use crate::{meme_cooking::MemeCookingCreateMemeEvent, ContractEventHandler, EventContext};

pub struct PushToRedisStream {
    nep141_stream: RedisEventStream<NewContractNep141EventData>,
    meme_cooking_stream: RedisEventStream<NewMemeCookingMemeEventData>,
    max_stream_size: usize,
    testnet: bool,
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
            meme_cooking_stream: RedisEventStream::new(
                connection,
                if testnet {
                    format!("{}_testnet", NewMemeCookingMemeEvent::ID)
                } else {
                    NewMemeCookingMemeEvent::ID.to_string()
                },
            ),
            max_stream_size,
            testnet,
        }
    }
}

#[async_trait]
impl ContractEventHandler for PushToRedisStream {
    async fn handle_new_nep141(&self, account_id: AccountId, context: EventContext) {
        self.nep141_stream
            .emit_event(
                context.block_height,
                NewContractNep141EventData {
                    account_id,

                    transaction_id: context.transaction_id,
                    receipt_id: context.receipt_id,
                    block_height: context.block_height,
                    block_timestamp_nanosec: context.block_timestamp_nanosec,
                },
                self.max_stream_size,
            )
            .expect("Failed to emit nep141 creation event");
    }

    async fn handle_meme_cooking_new_meme(
        &self,
        event: MemeCookingCreateMemeEvent,
        context: EventContext,
    ) {
        log::warn!("Meme cooking event handling is not implemented");
        self.meme_cooking_stream
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
            .expect("Failed to emit meme cooking event");
    }

    fn is_testnet(&self) -> bool {
        self.testnet
    }
}
