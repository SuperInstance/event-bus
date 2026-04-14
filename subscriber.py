"""Subscriber Management — manages event subscriptions with filtering, throttle, and tracking."""

from __future__ import annotations

import fnmatch
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Callable

# Import FleetEvent from event_bus to avoid circular imports at module level.
# We resolve it lazily inside methods.


# ─── Delivery Status ─────────────────────────────────────────────────────────

class DeliveryStatus(Enum):
    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"
    FILTERED = "filtered"
    THROTTLED = "throttled"
    DEBOUNCED = "debounced"


@dataclass
class DeliveryRecord:
    """Record of a single event delivery attempt."""

    event_id: str
    status: DeliveryStatus
    timestamp: float = field(default_factory=time.time)
    error: str | None = None


# ─── Subscriber ──────────────────────────────────────────────────────────────

class EventSubscriber:
    """A single subscriber that listens for events on a topic pattern.

    Supports:
        - Wildcard topic patterns (fleet.agent.*)
        - Filtering by source, priority, data fields
        - Throttle (min ms between deliveries)
        - Debounce (wait ms after last event before delivering)
        - Delivery tracking (delivered, failed, pending counts)
    """

    def __init__(
        self,
        topic_pattern: str,
        callback: Callable,
        subscriber_id: str | None = None,
        filters: dict[str, Any] | None = None,
        throttle_ms: int = 0,
        debounce_ms: int = 0,
    ):
        self.subscriber_id = subscriber_id or f"sub-{uuid.uuid4().hex[:8]}"
        self.topic_pattern = topic_pattern
        self.callback = callback
        self.filters = filters or {}
        self.throttle_ms = throttle_ms
        self.debounce_ms = debounce_ms

        # Throttle state
        self._last_delivery_time: float = 0.0

        # Debounce state
        self._debounce_timer: threading.Timer | None = None
        self._debounce_queue: list[Any] = []  # pending events
        self._lock = threading.Lock()

        # Delivery tracking
        self._delivery_history: list[DeliveryRecord] = []
        self._max_history = 1000
        self._counts = {
            DeliveryStatus.DELIVERED: 0,
            DeliveryStatus.FAILED: 0,
            DeliveryStatus.FILTERED: 0,
            DeliveryStatus.THROTTLED: 0,
            DeliveryStatus.DEBOUNCED: 0,
        }

        self.created_at = time.time()

    def matches(self, event: Any) -> bool:
        """Check if this subscriber's pattern and filters match an event."""
        # Topic match
        if not fnmatch.fnmatch(event.topic, self.topic_pattern):
            return False

        # Filter: source
        if "source" in self.filters:
            if not fnmatch.fnmatch(event.source, self.filters["source"]):
                return False

        # Filter: priority_min
        if "priority_min" in self.filters:
            if event.priority < self.filters["priority_min"]:
                return False

        # Filter: priority_max
        if "priority_max" in self.filters:
            if event.priority > self.filters["priority_max"]:
                return False

        # Filter: data field matches (simple equality or fnmatch)
        if "data" in self.filters:
            for key, value in self.filters["data"].items():
                actual = event.data.get(key)
                if actual is None:
                    return False
                if isinstance(value, str) and isinstance(actual, str):
                    if not fnmatch.fnmatch(actual, value):
                        return False
                elif actual != value:
                    return False

        return True

    def deliver(self, event: Any) -> None:
        """Deliver an event to this subscriber.

        Applies throttle and debounce before calling the callback.
        """
        with self._lock:
            # Check throttle
            now = time.time()
            if self.throttle_ms > 0:
                elapsed_ms = (now - self._last_delivery_time) * 1000
                if elapsed_ms < self.throttle_ms:
                    self._record(event, DeliveryStatus.THROTTLED)
                    return

            # Check debounce
            if self.debounce_ms > 0:
                self._debounce_queue.append(event)
                self._record(event, DeliveryStatus.DEBOUNCED)
                # Reset debounce timer
                if self._debounce_timer is not None:
                    self._debounce_timer.cancel()
                self._debounce_timer = threading.Timer(
                    self.debounce_ms / 1000.0,
                    self._flush_debounce,
                )
                self._debounce_timer.daemon = True
                self._debounce_timer.start()
                return

            self._last_delivery_time = now

        # Deliver outside lock
        try:
            self.callback(event)
            self._record(event, DeliveryStatus.DELIVERED)
        except Exception as exc:
            self._record(event, DeliveryStatus.FAILED, error=str(exc))
            raise

    def _flush_debounce(self) -> None:
        """Flush all debounced events, delivering only the latest."""
        with self._lock:
            events = list(self._debounce_queue)
            self._debounce_queue.clear()
            self._debounce_timer = None
            self._last_delivery_time = time.time()

        if events:
            latest = events[-1]
            try:
                self.callback(latest)
                self._record(latest, DeliveryStatus.DELIVERED)
            except Exception as exc:
                self._record(latest, DeliveryStatus.FAILED, error=str(exc))
                raise

    def _record(
        self,
        event: Any,
        status: DeliveryStatus,
        error: str | None = None,
    ) -> None:
        """Record a delivery attempt."""
        record = DeliveryRecord(
            event_id=event.event_id,
            status=status,
            error=error,
        )
        self._delivery_history.append(record)
        if len(self._delivery_history) > self._max_history:
            self._delivery_history = self._delivery_history[-self._max_history:]
        self._counts[status] = self._counts.get(status, 0) + 1

    @property
    def delivery_counts(self) -> dict[str, int]:
        return {k.value: v for k, v in self._counts.items()}

    @property
    def pending_count(self) -> int:
        with self._lock:
            return len(self._debounce_queue)

    def get_history(self, limit: int = 50) -> list[dict]:
        records = self._delivery_history[-limit:]
        return [
            {
                "event_id": r.event_id,
                "status": r.status.value,
                "timestamp": r.timestamp,
                "error": r.error,
            }
            for r in records
        ]

    def to_dict(self) -> dict:
        return {
            "subscriber_id": self.subscriber_id,
            "topic_pattern": self.topic_pattern,
            "filters": self.filters,
            "throttle_ms": self.throttle_ms,
            "debounce_ms": self.debounce_ms,
            "delivery_counts": self.delivery_counts,
            "pending_count": self.pending_count,
            "created_at": self.created_at,
        }


# ─── Subscriber Manager ──────────────────────────────────────────────────────

class SubscriberManager:
    """Manages all event subscribers for the bus."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._subscribers: dict[str, EventSubscriber] = {}

    def add(
        self,
        topic_pattern: str,
        callback: Callable,
        subscriber_id: str | None = None,
        filters: dict[str, Any] | None = None,
        throttle_ms: int = 0,
        debounce_ms: int = 0,
    ) -> str:
        """Add a new subscriber.

        Returns the subscriber ID.
        """
        sub = EventSubscriber(
            topic_pattern=topic_pattern,
            callback=callback,
            subscriber_id=subscriber_id,
            filters=filters,
            throttle_ms=throttle_ms,
            debounce_ms=debounce_ms,
        )

        with self._lock:
            # If ID already exists, replace the subscriber
            self._subscribers[sub.subscriber_id] = sub

        return sub.subscriber_id

    def remove(self, subscriber_id: str) -> bool:
        """Remove a subscriber by ID. Returns True if found."""
        with self._lock:
            return self._subscribers.pop(subscriber_id, None) is not None

    def get(self, subscriber_id: str) -> EventSubscriber | None:
        """Get a subscriber by ID."""
        with self._lock:
            return self._subscribers.get(subscriber_id)

    def get_matching_subscribers(self, event: Any) -> list[EventSubscriber]:
        """Get all subscribers that match a given event."""
        with self._lock:
            subs = list(self._subscribers.values())
        return [sub for sub in subs if sub.matches(event)]

    def count(self) -> int:
        """Return the number of active subscribers."""
        with self._lock:
            return len(self._subscribers)

    def backlog_size(self) -> int:
        """Total pending (debounced) events across all subscribers."""
        with self._lock:
            return sum(s.pending_count for s in self._subscribers.values())

    def list_all(self) -> list[dict]:
        """Get info dicts for all subscribers."""
        with self._lock:
            return [sub.to_dict() for sub in self._subscribers.values()]

    def clear(self) -> int:
        """Remove all subscribers. Returns count removed."""
        with self._lock:
            count = len(self._subscribers)
            self._subscribers.clear()
            return count
