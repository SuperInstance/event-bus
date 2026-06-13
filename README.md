# Event Bus

**event-bus** is a lightweight, in-process publish/subscribe messaging system for Rust. It allows decoupled components to communicate through named topics without direct references to each other, using thread-safe handler registration and synchronous dispatch.

## Why It Matters

The publish/subscribe pattern is fundamental to decoupled software architecture. When a user registers, the auth system shouldn't need to know about the email service, the analytics pipeline, or the welcome-message sender — it just publishes `"user.created"` and moves on. This library provides that pattern in 100 lines of dependency-free Rust, making it ideal for plugins, game engines, embedded systems, and any application where components need to react to events without compile-time coupling.

## How It Works

The `EventBus` maintains a `HashMap<String, Vec<HandlerFn>>` mapping topic names to handler lists. Each handler is an `Arc<dyn Fn(&str) + Send + Sync>` — a thread-safe closure that receives the message as a string.

**Subscription** (`subscribe`) acquires a mutex lock on the map, inserts the handler into the topic's vector, and releases the lock. This is O(1) amortized.

**Publication** (`publish`) acquires the mutex lock, iterates over the topic's handlers, and calls each one synchronously. Dispatch is O(H) where H = number of handlers for that topic.

**Concurrency model:** The bus uses `Arc<Mutex<HashMap<...>>>`. The lock is held only during iteration setup; handlers execute while holding the lock, so handlers must not call `publish` or `subscribe` on the same bus (this would deadlock). For recursive publish patterns, queue messages in a channel and drain after unlock.

The trade-off versus async message queues (tokio broadcast, crossbeam): this bus has zero allocation overhead per message and nanosecond-latency dispatch, but it blocks the publisher until all handlers complete.

## Quick Start

```rust
use event_bus::EventBus;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

fn main() {
    let bus = EventBus::new();

    // Subscribe multiple handlers to the same topic
    let counter = Arc::new(AtomicUsize::new(0));
    let c1 = counter.clone();
    bus.subscribe("user.created", move |msg| {
        println!("  Analytics: processing {}", msg);
        c1.fetch_add(1, Ordering::SeqCst);
    });

    bus.subscribe("user.created", move |msg| {
        println!("  Email: sending welcome for {}", msg);
    });

    // Publish events
    bus.publish("user.created", "user:42");
    bus.publish("user.created", "user:43");

    assert_eq!(bus.handler_count("user.created"), 2);
    assert_eq!(counter.load(Ordering::SeqCst), 2);
}
```

## API

### `EventBus`

| Method | Signature | Complexity |
|--------|-----------|------------|
| `new()` | `→ EventBus` | O(1) |
| `subscribe(topic, handler)` | where `handler: Fn(&str) + Send + Sync + 'static` | O(1) amortized |
| `publish(topic, message)` | Calls all handlers for topic | O(H) |
| `topic_count()` | `→ usize` | O(1) |
| `handler_count(topic)` | `→ usize` | O(1) |

Implements `Default`.

### Handler Requirements

Handlers must be `Fn(&str) + Send + Sync + 'static`. To capture mutable state, use `Arc<AtomicUsize>` or `Arc<Mutex<T>>`.

## Architecture Notes

This crate provides the in-process event routing for SuperInstance's component communication. The bus serves as the η (eta) layer's notification mechanism: when the γ (gamma) layer commits a state change, the bus distributes the update to all subscribed components, maintaining the γ + η = C invariant.

See [ARCHITECTURE.md](https://github.com/SuperInstance/SuperInstance/blob/main/ARCHITECTURE.md) for system-wide design.

## References

- Gamma, E., et al. (1994). *Design Patterns: Elements of Reusable Object-Oriented Software*. Addison-Wesley. Observer pattern (pp. 293–303).
- Fowler, M. (2002). *Patterns of Enterprise Application Architecture*. Addison-Wesley. Event Notification pattern.
- Rust async book. *Async and Await*. [rust-lang.org/async-book](https://rust-lang.org/async-book/)

## License

MIT
