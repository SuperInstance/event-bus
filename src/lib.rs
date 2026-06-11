//! event-bus: A lightweight event bus for publish/subscribe messaging.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

type HandlerFn = Arc<dyn Fn(&str) + Send + Sync>;

pub struct EventBus {
    handlers: Arc<Mutex<HashMap<String, Vec<HandlerFn>>>>,
}

impl EventBus {
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn subscribe<F>(&self, topic: &str, handler: F)
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        self.handlers
            .lock()
            .unwrap()
            .entry(topic.to_string())
            .or_default()
            .push(Arc::new(handler));
    }

    pub fn publish(&self, topic: &str, message: &str) {
        if let Some(handlers) = self.handlers.lock().unwrap().get(topic) {
            println!("[event-bus] Publishing to '{}': {}", topic, message);
            for handler in handlers {
                handler(message);
            }
        }
    }

    pub fn topic_count(&self) -> usize {
        self.handlers.lock().unwrap().len()
    }

    pub fn handler_count(&self, topic: &str) -> usize {
        self.handlers
            .lock()
            .unwrap()
            .get(topic)
            .map(|v| v.len())
            .unwrap_or(0)
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_create_bus() {
        let bus = EventBus::new();
        assert_eq!(bus.topic_count(), 0);
    }

    #[test]
    fn test_subscribe_and_publish() {
        let bus = EventBus::new();
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();
        bus.subscribe("user.created", move |_msg| {
            count_clone.fetch_add(1, Ordering::SeqCst);
        });
        bus.publish("user.created", "user:42");
        bus.publish("user.created", "user:43");
        assert_eq!(bus.topic_count(), 1);
        assert_eq!(bus.handler_count("user.created"), 1);
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }
}
