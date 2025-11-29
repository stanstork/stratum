use model::events::Event;
use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::Arc,
};
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tracing::{debug, warn};

/// A subscription handle that can be used to unsubscribe from events.
#[derive(Debug, Clone)]
pub struct Subscription {
    event_type_id: TypeId,
    subscriber_id: u64,
}

#[derive(Clone)]
pub struct EventBus {
    // Map of Event TypeID -> (Map of SubscriberID -> Sender)
    subscribers: Arc<RwLock<HashMap<TypeId, HashMap<u64, Box<dyn Any + Send + Sync>>>>>,
    next_id: Arc<RwLock<u64>>,
}

impl EventBus {
    pub fn new() -> Self {
        EventBus {
            subscribers: Arc::new(RwLock::new(HashMap::new())),
            next_id: Arc::new(RwLock::new(0)),
        }
    }

    pub async fn subscribe<E>(&self, sender: mpsc::Sender<Arc<E>>) -> Subscription
    where
        E: Event + Clone,
    {
        let event_type_id = TypeId::of::<E>();

        // Generate a unique subscriber ID
        let subscriber_id = {
            let mut id_lock = self.next_id.write().await;
            let id = *id_lock;
            *id_lock += 1;
            id
        };

        let mut subscribers = self.subscribers.write().await;
        let entry = subscribers
            .entry(event_type_id)
            .or_insert_with(HashMap::new);

        entry.insert(subscriber_id, Box::new(sender));

        debug!(
            event_type = std::any::type_name::<E>(),
            subscriber_id = subscriber_id,
            "Subscribed to event"
        );

        Subscription {
            event_type_id,
            subscriber_id,
        }
    }

    pub async fn publish<E>(&self, event: E)
    where
        E: Event + Clone,
    {
        let event_type_id = TypeId::of::<E>();
        let event_arc = Arc::new(event);
        let subscribers = self.subscribers.read().await;

        if let Some(type_subscribers) = subscribers.get(&event_type_id) {
            debug!(
                event_type = std::any::type_name::<E>(),
                subscriber_count = type_subscribers.len(),
                "Publishing event"
            );

            // Send to all subscribers
            for (subscriber_id, boxed_sender) in type_subscribers.iter() {
                // Downcast back to the specific Sender type
                if let Some(sender) = boxed_sender.downcast_ref::<mpsc::Sender<Arc<E>>>() {
                    let event_clone = event_arc.clone();

                    // try_send is non-blocking. If channel is full, it returns error.
                    if let Err(e) = sender.try_send(event_clone) {
                        warn!(
                            event_type = std::any::type_name::<E>(),
                            subscriber_id = subscriber_id,
                            error = ?e,
                            "Dropped event for slow subscriber (channel full)"
                        );
                    }
                } else {
                    warn!(
                        event_type = std::any::type_name::<E>(),
                        subscriber_id = subscriber_id,
                        "Failed to downcast sender for subscriber"
                    );
                }
            }
        } else {
            debug!(
                event_type = std::any::type_name::<E>(),
                "No subscribers for event"
            );
        }
    }

    pub async fn unsubscribe(&self, subscription: Subscription) {
        let mut subscribers = self.subscribers.write().await;

        if let Some(type_subscribers) = subscribers.get_mut(&subscription.event_type_id) {
            type_subscribers.remove(&subscription.subscriber_id);

            debug!(
                subscriber_id = subscription.subscriber_id,
                "Unsubscribed from event"
            );

            // Clean up empty type maps to save memory
            if type_subscribers.is_empty() {
                subscribers.remove(&subscription.event_type_id);
            }
        }
    }

    pub async fn subscriber_count<E>(&self) -> usize
    where
        E: Event,
    {
        let event_type_id = TypeId::of::<E>();
        let subscribers = self.subscribers.read().await;

        subscribers
            .get(&event_type_id)
            .map(|subs| subs.len())
            .unwrap_or(0)
    }

    pub async fn clear(&self) {
        let mut subscribers = self.subscribers.write().await;
        subscribers.clear();
        debug!("Cleared all subscriptions from EventBus");
    }
}
