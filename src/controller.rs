use std::{io::Result, sync::Arc};

use tokio::{sync::{Mutex, RwLock}, task::JoinSet};

use crate::{Database, DatabaseAdmin, DatabaseImpl, Value};

pub struct Controller {
    db: Arc<RwLock<DatabaseImpl>>,
    flush_threshold: usize,
    workers: Mutex<JoinSet<()>>
}

impl Controller {
    pub fn new(inner: DatabaseImpl, flush_threshold: usize) -> Controller {
        let db: Arc<RwLock<DatabaseImpl>> = Arc::new(RwLock::new(inner));

        Controller {
            db,
            flush_threshold,
            workers: Mutex::new(JoinSet::new())
        }
    }

    pub async fn shutdown(self: Arc<Self>) {
        while let Some(res) = self.workers.lock().await.join_next().await {
            if let Err(e) = res {
                log::warn!("Connection handler exited with error: {:?}", e)
            }
        }
    }

    pub async fn get(&self, key: &str) -> Result<Option<Value>> {
        self.db.read().await.get(&key).await
    }

    pub async fn set(&self, key: String, value: Value) -> Result<()> {
        let mut db = self.db.write().await;
        db.set(key, value).await?;

        if db.current_size > self.flush_threshold {
            let db_clone = self.db.clone();
            self.workers.lock().await.spawn(async move {
                let _ = db_clone.write().await.flush().await;
            });
        }

        Ok(())
    }

    pub async fn delete(&self, key: String) -> Result<()> {
        let mut db = self.db.write().await;
        db.delete(key).await?;

        if db.current_size > self.flush_threshold {
            let db_clone = self.db.clone();
            self.workers.lock().await.spawn(async move {
                let _ = db_clone.write().await.flush().await;
            });
        }

        Ok(())
    }
}
