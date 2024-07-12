use crate::HandledTokensStorage;

use async_trait::async_trait;
use inindexer::near_indexer_primitives::types::AccountId;
use std::collections::HashSet;
use std::path::PathBuf;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::RwLock;

pub struct TxtFileStorage {
    path: PathBuf,
    handled_accounts: RwLock<HashSet<AccountId>>,
}

impl TxtFileStorage {
    pub async fn new(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        let mut set = HashSet::new();
        if let Ok(file) = File::open(&path).await {
            let mut reader = BufReader::new(file);
            let mut line = String::new();
            while reader.read_line(&mut line).await.unwrap() != 0 {
                set.insert(line.trim().parse().unwrap());
                line.clear();
            }
        }
        Self {
            path,
            handled_accounts: RwLock::new(set),
        }
    }
}

#[async_trait]
impl HandledTokensStorage for TxtFileStorage {
    async fn is_already_indexed(&self, account_id: &AccountId) -> bool {
        self.handled_accounts.read().await.contains(account_id)
    }

    async fn mark_handled(&self, account_id: AccountId) {
        self.handled_accounts
            .write()
            .await
            .insert(account_id.clone());
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&self.path)
            .await
            .unwrap();
        file.write_all((account_id.to_string() + "\n").as_bytes())
            .await
            .unwrap();
        file.flush().await.unwrap();
    }
}
