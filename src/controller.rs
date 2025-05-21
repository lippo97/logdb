use std::{io::Result, sync::{atomic::{AtomicBool, Ordering}, Arc}};

use tokio::{sync::{Mutex, RwLock}, task::JoinSet};

use crate::{Database, DatabaseAdmin, DatabaseImpl, Value};

pub struct Controller {
    db: Arc<RwLock<DatabaseImpl>>,
    flush_threshold: usize,
    workers: Mutex<JoinSet<()>>,
    is_shutdown: AtomicBool,
}

impl Drop for Controller {
    fn drop(&mut self) {
        if !self.is_shutdown.load(Ordering::SeqCst) {
            log::warn!("Database dropped without shutdown. Resources may have leaked!");
        }
    }
}

impl Controller {
    pub fn new(inner: DatabaseImpl, flush_threshold: usize) -> Controller {
        let db: Arc<RwLock<DatabaseImpl>> = Arc::new(RwLock::new(inner));

        Controller {
            db,
            flush_threshold,
            workers: Mutex::new(JoinSet::new()),
            is_shutdown: AtomicBool::new(false),
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        // Check if controller is already shut down.
        if self.is_shutdown.swap(true, Ordering::SeqCst) {
            log::warn!("Double shutdown attempt.");
            return Ok(())
        }

        let mut workers = self.workers.lock().await;
        let len = workers.len();

        if len > 0 {
            log::info!("Stopping {len} jobs...");
            while let Some(res) = workers.join_next().await {
                if let Err(e) = res {
                    log::warn!("Connection handler exited with error: {:?}", e)
                }
            }
            log::info!("Done.")
        } 

        let mut db = self.db.write().await;

        if db.memtable.len() > 0 {
            db.flush().await?;
        }

        Ok(())
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
