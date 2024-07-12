use async_trait::async_trait;
use inevents_redis::RedisEventStream;
use inindexer::near_indexer_primitives::types::AccountId;
use intear_events::events::newcontract::nep141::{
    NewContractNep141Event, NewContractNep141EventData,
};
use redis::aio::ConnectionManager;

use crate::{ContractEventHandler, EventContext};

pub struct PushToRedisStream {
    nep141_stream: RedisEventStream<NewContractNep141EventData>,
    max_stream_size: usize,
}

impl PushToRedisStream {
    pub async fn new(connection: ConnectionManager, max_stream_size: usize) -> Self {
        Self {
            nep141_stream: RedisEventStream::new(connection.clone(), NewContractNep141Event::ID),
            max_stream_size,
        }
    }
}

#[async_trait]
impl ContractEventHandler for PushToRedisStream {
    async fn handle_new_nep141(&mut self, account_id: AccountId, context: EventContext) {
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
}
