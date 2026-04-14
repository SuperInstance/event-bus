"""Core Event Bus — Publish/Subscribe event bus for the Pelagic fleet.

Agents publish events, other agents subscribe to topics they care about.
Events flow: agent → bus → subscribers → actions.
"""

from __future__ import annotations

import fnmatch
import json
import os
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable

from subscriber import EventSubscriber, SubscriberManager


# ─── Event Data Model ───────────────────────────────────────────────────────

@dataclass
class FleetEvent:
    """A single event flowing through the fleet event bus."""

    topic: str
    source: str  # agent name
    timestamp: float
    data: dict
    event_id: str = field(default_factory=lambda: uuid.uuid4().hex[:16])
    ttl: int = 300  # seconds before event is considered stale
    priority: int = 5  # 1=low, 5=normal, 10=critical

    @property
    def is_expired(self) -> bool:
        return (time.time() - self.timestamp) > self.ttl

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, d: dict) -> FleetEvent:
        return cls(**d)

    @classmethod
    def from_json(cls, s: str) -> FleetEvent:
        return cls.from_dict(json.loads(s))


# ─── Well-known Topics ──────────────────────────────────────────────────────

KNOWN_TOPICS = [
    "fleet.agent.boot",
    "fleet.agent.shutdown",
    "fleet.agent.health.ok",
    "fleet.agent.health.degraded",
    "fleet.agent.health.down",
    "fleet.agent.health.recovery",
    "fleet.agent.error",
    "fleet.git.push",
    "fleet.git.pr",
    "fleet.test.pass",
    "fleet.test.fail",
    "fleet.keeper.secret.*",
    "fleet.mud.*",
    "fleet.cicd.pipeline.*",
    "fleet.alert.*",
]

PRIORITY_LOW = 1
PRIORITY_NORMAL = 5
PRIORITY_HIGH = 8
PRIORITY_CRITICAL = 10


# ─── Dead Letter Queue ─────────────────────────────────────────────────────

@dataclass
class DeadLetterEntry:
    """An event that could not be delivered."""

    event: FleetEvent
    subscriber_id: str
    reason: str
    timestamp: float = field(default_factory=time.time)
    retry_count: int = 0

    def to_dict(self) -> dict:
        return {
            "event": self.event.to_dict(),
            "subscriber_id": self.subscriber_id,
            "reason": self.reason,
            "timestamp": self.timestamp,
            "retry_count": self.retry_count,
        }


# ─── Bus Statistics ─────────────────────────────────────────────────────────

@dataclass
class BusStats:
    """Running statistics about the event bus."""

    total_published: int = 0
    total_delivered: int = 0
    total_failed: int = 0
    total_filtered: int = 0
    dead_letter_size: int = 0
    subscriber_count: int = 0
    active_topics: int = 0
    events_per_second: float = 0.0
    uptime_seconds: float = 0.0
    backlog_size: int = 0

    def to_dict(self) -> dict:
        return asdict(self)


# ─── Core Event Bus ─────────────────────────────────────────────────────────

class FleetEventBus:
    """Publish/Subscribe event bus for the Pelagic fleet.

    Agents publish events, other agents subscribe to topics they care about.
    Events flow: agent → bus → subscribers → actions.

    Features:
        - Topic matching with wildcards (fleet.agent.* matches fleet.agent.boot)
        - Event persistence (JSONL event log)
        - Event replay (replay events from a specific time)
        - Dead letter queue (events that couldn't be delivered)
        - Event filtering (subscribers can filter by source, priority, data fields)
        - Bus statistics (events/sec, backlog, subscriber counts)
    """

    def __init__(
        self,
        log_path: str | Path | None = None,
        persist: bool = True,
        max_event_log: int = 10000,
        max_dlq_size: int = 1000,
    ):
        self._lock = threading.RLock()
        self._persist = persist
        self._max_event_log = max_event_log
        self._max_dlq_size = max_dlq_size

        # Event history (ring buffer of recent events)
        self._event_log: list[FleetEvent] = []

        # Dead letter queue
        self._dlq: list[DeadLetterEntry] = []

        # Subscriber manager
        self._subscribers = SubscriberManager()

        # Stream listeners (for long-polling / streaming)
        self._stream_listeners: list[dict] = []

        # Statistics tracking
        self._start_time = time.time()
        self._publish_timestamps: list[float] = []  # for events/sec calculation
        self._total_published = 0
        self._total_delivered = 0
        self._total_failed = 0
        self._total_filtered = 0

        # Event log file
        self._log_path = Path(log_path) if log_path else Path("events.jsonl")
        self._log_file = None
        if persist:
            self._open_log()

    # ── Log File Management ─────────────────────────────────────────────

    def _open_log(self) -> None:
        """Open the JSONL event log file for appending."""
        try:
            self._log_path.parent.mkdir(parents=True, exist_ok=True)
            self._log_file = open(self._log_path, "a", encoding="utf-8")
        except OSError as e:
            print(f"[event-bus] Warning: cannot open event log {self._log_path}: {e}")
            self._persist = False

    def _write_log(self, event: FleetEvent) -> None:
        """Append an event to the JSONL log file."""
        if self._persist and self._log_file:
            try:
                self._log_file.write(event.to_json() + "\n")
                self._log_file.flush()
            except OSError:
                pass

    def _close_log(self) -> None:
        """Close the JSONL event log file."""
        if self._log_file:
            try:
                self._log_file.close()
            except OSError:
                pass
            self._log_file = None

    # ── Topic Matching ──────────────────────────────────────────────────

    @staticmethod
    def topic_matches(pattern: str, topic: str) -> bool:
        """Check if a topic matches a pattern with wildcard support.

        Patterns support '*' as a multi-segment wildcard (like fnmatch).
        Examples:
            fleet.agent.* matches fleet.agent.boot
            fleet.* matches fleet.agent.boot, fleet.git.push
            * matches everything
        """
        return fnmatch.fnmatch(topic, pattern)

    # ── Publishing ──────────────────────────────────────────────────────

    def publish(
        self,
        topic: str,
        source: str,
        data: dict | None = None,
        priority: int = PRIORITY_NORMAL,
        ttl: int = 300,
    ) -> FleetEvent:
        """Publish an event to the bus.

        Args:
            topic: Event topic (e.g. 'fleet.git.push').
            source: Name of the publishing agent.
            data: Event payload dictionary.
            priority: Event priority (1-10).
            ttl: Time-to-live in seconds.

        Returns:
            The published FleetEvent.
        """
        event = FleetEvent(
            topic=topic,
            source=source,
            timestamp=time.time(),
            data=data or {},
            priority=max(1, min(10, priority)),
            ttl=ttl,
        )

        with self._lock:
            # Update statistics
            self._total_published += 1
            self._publish_timestamps.append(event.timestamp)

            # Trim old timestamps (keep last 60 seconds for rate calculation)
            cutoff = event.timestamp - 60.0
            self._publish_timestamps = [
                ts for ts in self._publish_timestamps if ts > cutoff
            ]

            # Add to event log (ring buffer)
            self._event_log.append(event)
            if len(self._event_log) > self._max_event_log:
                self._event_log = self._event_log[-self._max_event_log:]

            # Persist to disk
            self._write_log(event)

        # Deliver to subscribers (outside lock to avoid deadlocks)
        self._deliver_event(event)

        return event

    # ── Event Delivery ──────────────────────────────────────────────────

    def _deliver_event(self, event: FleetEvent) -> None:
        """Deliver an event to all matching subscribers."""
        matching = self._subscribers.get_matching_subscribers(event)

        for sub in matching:
            try:
                sub.deliver(event)
                with self._lock:
                    self._total_delivered += 1
            except Exception as exc:
                with self._lock:
                    self._total_failed += 1
                    self._add_to_dlq(event, sub.subscriber_id, str(exc))

        with self._lock:
            total_matched = len(matching)
            total_subs = self._subscribers.count()
            self._total_filtered += total_subs - total_matched

        # Notify stream listeners
        self._notify_stream_listeners(event)

    def _notify_stream_listeners(self, event: FleetEvent) -> None:
        """Push event to any active long-poll stream listeners."""
        stale = []
        with self._lock:
            for listener in self._stream_listeners:
                topics = listener.get("topics")
                if topics is None or any(
                    self.topic_matches(p, event.topic) for p in topics
                ):
                    listener["queue"].append(event)
                    if listener.get("event"):
                        listener["event"].set()

                # Remove listeners that have been waiting too long
                if listener.get("expires_at", 0) < time.time():
                    stale.append(listener)

            for s in stale:
                self._stream_listeners.remove(s)

    # ── Dead Letter Queue ───────────────────────────────────────────────

    def _add_to_dlq(
        self, event: FleetEvent, subscriber_id: str, reason: str
    ) -> None:
        """Add a failed event to the dead letter queue."""
        entry = DeadLetterEntry(
            event=event,
            subscriber_id=subscriber_id,
            reason=reason,
        )
        self._dlq.append(entry)
        # Trim DLQ to max size
        if len(self._dlq) > self._max_dlq_size:
            self._dlq = self._dlq[-self._max_dlq_size:]

    def get_dlq(self, limit: int = 100) -> list[DeadLetterEntry]:
        """Get entries from the dead letter queue."""
        with self._lock:
            return list(self._dlq[-limit:])

    def clear_dlq(self) -> int:
        """Clear the dead letter queue. Returns count of cleared entries."""
        with self._lock:
            count = len(self._dlq)
            self._dlq.clear()
            return count

    def retry_dlq(self) -> int:
        """Retry all events in the dead letter queue. Returns retry count."""
        with self._lock:
            entries = list(self._dlq)
            self._dlq.clear()

        retried = 0
        for entry in entries:
            entry.retry_count += 1
            if entry.retry_count > 3:
                # Give up after 3 retries, re-add to DLQ
                with self._lock:
                    self._dlq.append(entry)
                continue
            try:
                self._deliver_event(entry.event)
                retried += 1
            except Exception:
                with self._lock:
                    self._dlq.append(entry)
        return retried

    # ── Subscription ────────────────────────────────────────────────────

    def subscribe(
        self,
        topic_pattern: str,
        callback: Callable[[FleetEvent], None],
        subscriber_id: str | None = None,
        filters: dict[str, Any] | None = None,
        throttle_ms: int = 0,
        debounce_ms: int = 0,
    ) -> str:
        """Subscribe to events matching a topic pattern.

        Args:
            topic_pattern: Topic pattern with wildcard support.
            callback: Function called with each matching event.
            subscriber_id: Optional unique ID for the subscriber.
            filters: Optional dict of filters (source, priority_min, data fields).
            throttle_ms: Minimum milliseconds between deliveries.
            debounce_ms: Wait this many ms after last event before delivering.

        Returns:
            The subscriber ID.
        """
        return self._subscribers.add(
            topic_pattern=topic_pattern,
            callback=callback,
            subscriber_id=subscriber_id,
            filters=filters,
            throttle_ms=throttle_ms,
            debounce_ms=debounce_ms,
        )

    def unsubscribe(self, subscriber_id: str) -> bool:
        """Remove a subscriber by ID. Returns True if found and removed."""
        return self._subscribers.remove(subscriber_id)

    def get_subscribers(self) -> list[dict]:
        """Get info about all active subscribers."""
        return self._subscribers.list_all()

    # ── Event History ───────────────────────────────────────────────────

    def get_events(
        self,
        topic: str | None = None,
        source: str | None = None,
        limit: int = 100,
        since: float | None = None,
    ) -> list[FleetEvent]:
        """Get events from the in-memory log.

        Args:
            topic: Filter by topic (supports wildcards).
            source: Filter by source agent.
            limit: Maximum events to return.
            since: Only return events after this timestamp.

        Returns:
            List of matching events (newest first).
        """
        with self._lock:
            events = list(self._event_log)

        if since is not None:
            events = [e for e in events if e.timestamp >= since]
        if topic is not None:
            events = [e for e in events if self.topic_matches(topic, e.topic)]
        if source is not None:
            events = [e for e in events if e.source == source]

        events.sort(key=lambda e: e.timestamp, reverse=True)
        return events[:limit]

    # ── Event Replay ────────────────────────────────────────────────────

    def replay(
        self,
        since: float,
        topic: str | None = None,
        callback: Callable[[FleetEvent], None] | None = None,
    ) -> int:
        """Replay events from a specific time.

        Reads from the persistent JSONL log file if available.

        Args:
            since: Timestamp to start replaying from.
            topic: Optional topic filter.
            callback: If provided, deliver replayed events through this callback
                      instead of re-publishing.

        Returns:
            Number of events replayed.
        """
        events = []
        # Try reading from persistent log first
        if self._persist and self._log_path.exists():
            try:
                with open(self._log_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            event = FleetEvent.from_json(line)
                            if event.timestamp >= since:
                                if topic is None or self.topic_matches(topic, event.topic):
                                    events.append(event)
                        except (json.JSONDecodeError, TypeError):
                            continue
            except OSError:
                pass

        # Also check in-memory log (may have events not yet flushed)
        with self._lock:
            for event in self._event_log:
                if event.timestamp >= since:
                    if topic is None or self.topic_matches(topic, event.topic):
                        # Deduplicate by event_id
                        if not any(e.event_id == event.event_id for e in events):
                            events.append(event)

        events.sort(key=lambda e: e.timestamp)

        count = 0
        for event in events:
            if callback:
                try:
                    callback(event)
                except Exception:
                    pass
            else:
                self._deliver_event(event)
            count += 1

        return count

    # ── Topics ──────────────────────────────────────────────────────────

    def get_active_topics(self) -> list[dict]:
        """Get all topics that have been published to, with counts."""
        topic_counts: dict[str, int] = {}
        with self._lock:
            for event in self._event_log:
                topic_counts[event.topic] = topic_counts.get(event.topic, 0) + 1

        return [
            {"topic": topic, "count": count}
            for topic, count in sorted(topic_counts.items())
        ]

    # ── Streaming ───────────────────────────────────────────────────────

    def create_stream(
        self, topics: list[str] | None = None, timeout: float = 30.0
    ) -> list[FleetEvent]:
        """Create a long-poll stream that blocks until events arrive or timeout.

        Args:
            topics: Optional list of topic patterns to filter.
            timeout: Maximum seconds to wait for events.

        Returns:
            List of events received during the wait period.
        """
        event_obj = threading.Event()
        queue: list[FleetEvent] = []

        listener = {
            "topics": topics,
            "queue": queue,
            "event": event_obj,
            "expires_at": time.time() + timeout,
        }

        with self._lock:
            self._stream_listeners.append(listener)

        # Wait for events or timeout
        event_obj.wait(timeout=timeout)

        with self._lock:
            if listener in self._stream_listeners:
                self._stream_listeners.remove(listener)

        return list(queue)

    # ── Statistics ──────────────────────────────────────────────────────

    def get_stats(self) -> BusStats:
        """Get current bus statistics."""
        now = time.time()
        with self._lock:
            # Calculate events per second from recent publish timestamps
            recent = [ts for ts in self._publish_timestamps if now - ts < 60.0]
            eps = len(recent) / 60.0 if recent else 0.0

            return BusStats(
                total_published=self._total_published,
                total_delivered=self._total_delivered,
                total_failed=self._total_failed,
                total_filtered=self._total_filtered,
                dead_letter_size=len(self._dlq),
                subscriber_count=self._subscribers.count(),
                active_topics=len(set(e.topic for e in self._event_log)),
                events_per_second=round(eps, 2),
                uptime_seconds=round(now - self._start_time, 1),
                backlog_size=self._subscribers.backlog_size(),
            )

    # ── Lifecycle ───────────────────────────────────────────────────────

    def shutdown(self) -> None:
        """Gracefully shut down the event bus."""
        self._close_log()
        with self._lock:
            self._stream_listeners.clear()

    def __enter__(self) -> FleetEventBus:
        return self

    def __exit__(self, *args: Any) -> None:
        self.shutdown()

    def __repr__(self) -> str:
        stats = self.get_stats()
        return (
            f"FleetEventBus("
            f"published={stats.total_published}, "
            f"delivered={stats.total_delivered}, "
            f"subscribers={stats.subscriber_count}, "
            f"dlq={stats.dead_letter_size})"
        )
