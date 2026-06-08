use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use smdb_core::prelude::ChangeRecord;
use tokio::sync::mpsc;

/// An active subscription that receives [`ChangeRecord`] events pushed by the
/// server.  Created by [`Client::subscribe`].
pub struct Subscription {
    /// Opaque subscription ID assigned by this client.
    pub id: String,
    pub(crate) receiver: mpsc::UnboundedReceiver<ChangeRecord>,
}

impl Subscription {
    /// Receive the next change record.  Returns `None` when the subscription
    /// channel is closed (e.g. after a disconnect).
    pub async fn next(&mut self) -> Option<ChangeRecord> {
        self.receiver.recv().await
    }
}

impl Stream for Subscription {
    type Item = ChangeRecord;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}
