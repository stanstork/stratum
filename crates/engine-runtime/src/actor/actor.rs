use crate::error::ActorError;
use async_trait::async_trait;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct ActorContext {
    name: Arc<str>,
}

impl ActorContext {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Arc::from(name.into()),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Core actor trait used by the Stratum Agent.
///
/// Each actor processes a single message type `M` on a dedicated mailbox.
/// The runtime will:
///   * call `on_start` once,
///   * then call `handle` for every incoming message,
///   * and finally call `on_stop` before shutdown.
#[async_trait]
pub trait Actor<M>: Send + 'static
where
    M: Send + Debug + 'static,
{
    /// Called once when the actor is started.
    async fn on_start(&mut self, _ctx: &ActorContext) -> Result<(), ActorError> {
        Ok(())
    }

    /// Handle a single incoming message.
    async fn handle(&mut self, msg: M, ctx: &ActorContext) -> Result<(), ActorError>;

    /// Called once when the mailbox is closed and the actor is about to stop.
    async fn on_stop(&mut self, _ctx: &ActorContext) -> Result<(), ActorError> {
        Ok(())
    }
}

/// Handle used by other components to send messages to an actor.
#[derive(Debug)]
pub struct ActorRef<M>
where
    M: Send + Debug + 'static,
{
    name: Arc<str>,
    tx: mpsc::Sender<M>,
}

impl<M> Clone for ActorRef<M>
where
    M: Send + Debug + 'static,
{
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            tx: self.tx.clone(),
        }
    }
}

impl<M> ActorRef<M>
where
    M: Send + Debug + 'static,
{
    pub fn new(name: impl Into<String>, tx: mpsc::Sender<M>) -> Self {
        Self {
            name: Arc::from(name.into()),
            tx,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Asynchronously send a message to the actor.
    pub async fn send(&self, msg: M) -> Result<(), ActorError> {
        self.tx
            .send(msg)
            .await
            .map_err(|_| ActorError::MailboxClosed)
    }

    /// Try to send a message without waiting if the channel is full.
    pub fn try_send(&self, msg: M) -> Result<(), ActorError> {
        self.tx.try_send(msg).map_err(|_| ActorError::MailboxClosed)
    }

    /// Get a clone of the underlying sender.
    pub fn sender(&self) -> mpsc::Sender<M> {
        self.tx.clone()
    }
}
