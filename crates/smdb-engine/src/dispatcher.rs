use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tracing::{debug, error, info};

use crate::engine::Engine;

/// Background worker that turns the committed transition log into change-stream
/// deliveries. It owns the single delivery path: on each wake-up (a commit
/// notification, the poll interval, or shutdown) it fans newly-committed records
/// out to every subscriber and drains the effect outbox. All storage access runs
/// on a blocking worker so the async reactor is never stalled.
pub struct Dispatcher {
    engine: Arc<Engine>,
    interval: Duration,
    shutdown: watch::Receiver<bool>,
}

impl Dispatcher {
    pub fn new(engine: Arc<Engine>, interval: Duration, shutdown: watch::Receiver<bool>) -> Self {
        Self {
            engine,
            interval,
            shutdown,
        }
    }

    pub async fn run(&mut self) {
        info!("dispatcher started, polling every {:?}", self.interval);
        let notify = self.engine.commit_notify();

        loop {
            tokio::select! {
                _ = self.shutdown.changed() => {
                    if *self.shutdown.borrow() {
                        info!("dispatcher shutting down, draining remaining records");
                        self.dispatch().await;
                        return;
                    }
                }
                _ = notify.notified() => {
                    self.dispatch().await;
                }
                _ = tokio::time::sleep(self.interval) => {
                    self.dispatch().await;
                }
            }
        }
    }

    /// Fan out new change records, then mark the corresponding effects published.
    async fn dispatch(&self) {
        let engine = Arc::clone(&self.engine);
        match tokio::task::spawn_blocking(move || {
            let delivered = engine.dispatch_pass();

            // Drain the effect outbox: once a transition's change record is on
            // the stream, its effects are considered published.
            let storage = engine.storage();
            if let Ok(effects) = storage.get_pending_effects(256) {
                for effect in effects {
                    if let Err(e) = storage.mark_effect_published(&effect.id) {
                        error!(effect_id = %effect.id, error = %e, "failed to mark effect published");
                        let _ = storage.mark_effect_failed(&effect.id);
                    }
                }
            }
            delivered
        })
        .await
        {
            Ok(n) if n > 0 => debug!(delivered = n, "dispatched change records"),
            Ok(_) => {}
            Err(e) => error!(error = %e, "dispatch task panicked"),
        }
    }
}
