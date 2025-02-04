use std::sync::Arc;

use async_trait::async_trait;
use inevents_redis::RedisEventStream;
use inindexer::near_indexer_primitives::types::{AccountId, BlockHeight};
use intear_events::events::newcontract::{
    nep141::NewContractNep141Event, nep171::NewContractNep171Event,
};
use redis::aio::ConnectionManager;
use tokio::sync::Mutex as TokioMutex;

use crate::{ContractEventHandler, EventContext};

pub struct PushToRedisStream {
    nep141_stream: Arc<TokioMutex<RedisEventStream<NewContractNep141Event>>>,
    nep171_stream: Arc<TokioMutex<RedisEventStream<NewContractNep171Event>>>,
    max_stream_size: usize,
}

impl PushToRedisStream {
    pub async fn new(connection: ConnectionManager, max_stream_size: usize) -> Self {
        Self {
            nep141_stream: Arc::new(TokioMutex::new(RedisEventStream::new(
                connection.clone(),
                NewContractNep141Event::ID,
            ))),
            nep171_stream: Arc::new(TokioMutex::new(RedisEventStream::new(
                connection.clone(),
                NewContractNep171Event::ID,
            ))),
            max_stream_size,
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

    async fn handle_new_nep171(&self, account_id: AccountId, context: EventContext) {
        self.nep171_stream
            .lock()
            .await
            .add_event(NewContractNep171Event {
                account_id,
                transaction_id: context.transaction_id,
                receipt_id: context.receipt_id,
                block_height: context.block_height,
                block_timestamp_nanosec: context.block_timestamp_nanosec,
            });
    }

    async fn flush_events(&self, block_height: BlockHeight) {
        self.nep141_stream
            .lock()
            .await
            .flush_events(block_height, self.max_stream_size)
            .await
            .expect("Failed to flush nep141 stream");
    }
}
