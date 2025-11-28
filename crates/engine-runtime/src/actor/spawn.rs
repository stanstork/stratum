use crate::actor::{Actor, ActorContext, ActorRef};
use std::fmt::Debug;
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::error;

/// Spawns a Tokio task that runs the actor event loop and returns an `ActorRef` and `JoinHandle`.
pub fn spawn_actor<M, A>(
    name: impl Into<String>,
    mailbox_capacity: usize,
    mut actor: A,
) -> (ActorRef<M>, JoinHandle<()>)
where
    A: Actor<M>,
    M: Send + Debug + 'static,
{
    let name_str = name.into();
    let ctx = ActorContext::new(name_str.clone());
    let (tx, mut rx) = mpsc::channel::<M>(mailbox_capacity);
    let actor_ref = ActorRef::new(name_str.clone(), tx);

    let handle = tokio::spawn(async move {
        if let Err(e) = actor.on_start(&ctx).await {
            tracing::error!(actor = %ctx.name(), ?e, "actor on_start failed");
            return;
        }

        while let Some(msg) = rx.recv().await {
            if let Err(e) = actor.handle(msg, &ctx).await {
                error!(actor = %ctx.name(), ?e, "actor handle failed");
            }
        }

        if let Err(e) = actor.on_stop(&ctx).await {
            error!(actor = %ctx.name(), ?e, "actor on_stop failed");
        }
    });

    (actor_ref, handle)
}
