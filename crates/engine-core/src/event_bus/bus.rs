use model::events::Event;
use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::Arc,
};
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tracing::{debug, warn};

// Map of Event TypeID -> (Map of SubscriberID -> Sender)
type Subscribers = Arc<RwLock<HashMap<TypeId, HashMap<u64, Box<dyn Any + Send + Sync>>>>>;

/// A subscription handle that can be used to unsubscribe from events.
#[derive(Debug, Clone)]
pub struct Subscription {
    event_type_id: TypeId,
    subscriber_id: u64,
}

#[derive(Clone)]
pub struct EventBus {
    subscribers: Subscribers,
    next_id: Arc<RwLock<u64>>,
}

impl std::fmt::Debug for EventBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventBus")
            .field("subscribers", &"<RwLock<HashMap>>")
            .finish()
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
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

            for (subscriber_id, boxed_sender) in type_subscribers.iter() {
                // Downcast back to the specific Sender type
                if let Some(sender) = boxed_sender.downcast_ref::<mpsc::Sender<Arc<E>>>() {
                    let event_clone = event_arc.clone();

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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone)]
    struct TestEvent {
        message: String,
    }

    impl Event for TestEvent {
        fn event_type(&self) -> &'static str {
            "test.event"
        }
    }

    #[derive(Debug, Clone)]
    struct AnotherEvent {
        value: i32,
    }

    impl Event for AnotherEvent {
        fn event_type(&self) -> &'static str {
            "another.event"
        }
    }

    #[tokio::test]
    async fn test_subscribe_and_publish() {
        let bus = EventBus::new();
        let (tx, mut rx) = mpsc::channel(10);

        let _sub = bus.subscribe::<TestEvent>(tx).await;

        bus.publish(TestEvent {
            message: "Hello".to_string(),
        })
        .await;

        let received = rx.recv().await.unwrap();
        assert_eq!(received.message, "Hello");
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let bus = EventBus::new();
        let (tx1, mut rx1) = mpsc::channel(10);
        let (tx2, mut rx2) = mpsc::channel(10);

        let _sub1 = bus.subscribe::<TestEvent>(tx1).await;
        let _sub2 = bus.subscribe::<TestEvent>(tx2).await;

        bus.publish(TestEvent {
            message: "Broadcast".to_string(),
        })
        .await;

        let received1 = rx1.recv().await.unwrap();
        let received2 = rx2.recv().await.unwrap();

        assert_eq!(received1.message, "Broadcast");
        assert_eq!(received2.message, "Broadcast");
    }

    #[tokio::test]
    async fn test_unsubscribe() {
        let bus = EventBus::new();
        let (tx, mut rx) = mpsc::channel(10);

        let sub = bus.subscribe::<TestEvent>(tx).await;
        assert_eq!(bus.subscriber_count::<TestEvent>().await, 1);

        // First verify we can receive events
        bus.publish(TestEvent {
            message: "Before unsubscribe".to_string(),
        })
        .await;

        let received = rx.recv().await.unwrap();
        assert_eq!(received.message, "Before unsubscribe");

        // Now unsubscribe
        bus.unsubscribe(sub).await;
        assert_eq!(bus.subscriber_count::<TestEvent>().await, 0);

        // Publish after unsubscribe
        bus.publish(TestEvent {
            message: "After unsubscribe".to_string(),
        })
        .await;

        // Should not receive anything
        tokio::select! {
            result = rx.recv() => {
                if result.is_some() {
                    panic!("Should not receive event after unsubscribe");
                }
            },
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {}
        }
    }

    #[tokio::test]
    async fn test_different_event_types() {
        let bus = EventBus::new();
        let (tx1, mut rx1) = mpsc::channel(10);
        let (tx2, mut rx2) = mpsc::channel(10);

        let _sub1 = bus.subscribe::<TestEvent>(tx1).await;
        let _sub2 = bus.subscribe::<AnotherEvent>(tx2).await;

        bus.publish(TestEvent {
            message: "Test".to_string(),
        })
        .await;

        bus.publish(AnotherEvent { value: 42 }).await;

        let received1 = rx1.recv().await.unwrap();
        let received2 = rx2.recv().await.unwrap();

        assert_eq!(received1.message, "Test");
        assert_eq!(received2.value, 42);
    }
}
