use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tracing::{debug, error, info};

use smdb_storage::StorageEngine;

pub struct Dispatcher {
    storage: Arc<dyn StorageEngine>,
    interval: Duration,
    shutdown: watch::Receiver<bool>,
}

impl Dispatcher {
    pub fn new(
        storage: Arc<dyn StorageEngine>,
        interval: Duration,
        shutdown: watch::Receiver<bool>,
    ) -> Self {
        Self {
            storage,
            interval,
            shutdown,
        }
    }

    pub async fn run(&mut self) {
        info!("dispatcher started, polling every {:?}", self.interval);

        loop {
            tokio::select! {
                _ = self.shutdown.changed() => {
                    if *self.shutdown.borrow() {
                        info!("dispatcher shutting down, draining remaining effects");
                        self.drain().await;
                        return;
                    }
                }
                _ = tokio::time::sleep(self.interval) => {
                    self.tick().await;
                }
            }
        }
    }

    async fn tick(&self) {
        match self.storage.get_pending_effects(100) {
            Ok(effects) => {
                for effect in effects {
                    debug!(effect_id = %effect.id, effect_name = %effect.effect_name, "publishing effect");
                    if let Err(e) = self.storage.mark_effect_published(&effect.id) {
                        error!(effect_id = %effect.id, error = %e, "failed to mark effect published");
                        let _ = self.storage.mark_effect_failed(&effect.id);
                    }
                }
            }
            Err(e) => {
                error!(error = %e, "failed to fetch pending effects");
            }
        }
    }

    async fn drain(&self) {
        loop {
            match self.storage.get_pending_effects(100) {
                Ok(effects) if effects.is_empty() => break,
                Ok(effects) => {
                    for effect in effects {
                        let _ = self.storage.mark_effect_published(&effect.id);
                    }
                }
                Err(_) => break,
            }
        }
        info!("dispatcher drained");
    }
}
