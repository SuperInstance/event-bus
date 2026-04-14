"""Comprehensive tests for the Fleet Event Bus."""

import json
import os
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

# Add parent directory to path so we can import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from event_bus import (
    BusStats,
    DeadLetterEntry,
    FleetEvent,
    FleetEventBus,
    PRIORITY_CRITICAL,
    PRIORITY_HIGH,
    PRIORITY_LOW,
    PRIORITY_NORMAL,
)
from subscriber import DeliveryStatus, EventSubscriber, SubscriberManager
from bridge import LogBridge, MUDEventBridge, MUDCommand, WebhookBridge


# ─── Helper ──────────────────────────────────────────────────────────────────

def _make_event(
    topic: str = "fleet.test.pass",
    source: str = "test-agent",
    data: dict | None = None,
    priority: int = PRIORITY_NORMAL,
) -> FleetEvent:
    return FleetEvent(
        topic=topic,
        source=source,
        timestamp=time.time(),
        data=data or {},
        priority=priority,
    )


# ─── FleetEvent Tests ────────────────────────────────────────────────────────

class TestFleetEvent(unittest.TestCase):
    """Tests for the FleetEvent dataclass."""

    def test_create_event(self) -> None:
        event = _make_event("fleet.git.push", "git-agent", {"sha": "abc123"})
        self.assertEqual(event.topic, "fleet.git.push")
        self.assertEqual(event.source, "git-agent")
        self.assertEqual(event.data, {"sha": "abc123"})
        self.assertEqual(event.priority, 5)
        self.assertEqual(event.ttl, 300)
        self.assertIsNotNone(event.event_id)
        self.assertEqual(len(event.event_id), 16)

    def test_event_serialization(self) -> None:
        event = _make_event("fleet.test.fail", "ci-agent", {"test": "login"})
        d = event.to_dict()
        self.assertEqual(d["topic"], "fleet.test.fail")
        self.assertEqual(d["source"], "ci-agent")
        self.assertEqual(d["data"], {"test": "login"})

        # Round-trip through JSON
        json_str = event.to_json()
        restored = FleetEvent.from_json(json_str)
        self.assertEqual(restored.topic, event.topic)
        self.assertEqual(restored.source, event.source)
        self.assertEqual(restored.data, event.data)
        self.assertEqual(restored.event_id, event.event_id)

    def test_is_expired(self) -> None:
        event = FleetEvent(
            topic="test",
            source="test",
            timestamp=time.time() - 400,
            data={},
            ttl=300,
        )
        self.assertTrue(event.is_expired)

        fresh = _make_event()
        self.assertFalse(fresh.is_expired)

    def test_from_dict(self) -> None:
        d = {
            "topic": "fleet.agent.boot",
            "source": "agent-1",
            "timestamp": 1234567890.0,
            "data": {"version": "1.0"},
            "event_id": "abc123def456",
            "ttl": 600,
            "priority": 8,
        }
        event = FleetEvent.from_dict(d)
        self.assertEqual(event.topic, "fleet.agent.boot")
        self.assertEqual(event.event_id, "abc123def456")
        self.assertEqual(event.priority, 8)


# ─── Topic Matching Tests ────────────────────────────────────────────────────

class TestTopicMatching(unittest.TestCase):
    """Tests for topic pattern matching with wildcards."""

    def test_exact_match(self) -> None:
        self.assertTrue(FleetEventBus.topic_matches("fleet.agent.boot", "fleet.agent.boot"))
        self.assertFalse(FleetEventBus.topic_matches("fleet.agent.boot", "fleet.agent.shutdown"))

    def test_single_wildcard(self) -> None:
        self.assertTrue(FleetEventBus.topic_matches("fleet.agent.*", "fleet.agent.boot"))
        self.assertTrue(FleetEventBus.topic_matches("fleet.agent.*", "fleet.agent.shutdown"))
        self.assertFalse(FleetEventBus.topic_matches("fleet.agent.*", "fleet.git.push"))

    def test_multi_segment_wildcard(self) -> None:
        self.assertTrue(FleetEventBus.topic_matches("fleet.*", "fleet.agent.boot"))
        self.assertTrue(FleetEventBus.topic_matches("fleet.*", "fleet.git.push"))
        self.assertTrue(FleetEventBus.topic_matches("fleet.*", "fleet.agent.health.ok"))

    def test_catch_all(self) -> None:
        self.assertTrue(FleetEventBus.topic_matches("*", "fleet.agent.boot"))
        self.assertTrue(FleetEventBus.topic_matches("*", "anything"))

    def test_health_topics(self) -> None:
        self.assertTrue(FleetEventBus.topic_matches("fleet.agent.health.*", "fleet.agent.health.ok"))
        self.assertTrue(FleetEventBus.topic_matches("fleet.agent.health.*", "fleet.agent.health.degraded"))
        self.assertTrue(FleetEventBus.topic_matches("fleet.agent.health.*", "fleet.agent.health.down"))
        self.assertFalse(FleetEventBus.topic_matches("fleet.agent.health.*", "fleet.agent.boot"))

    def test_multiple_wildcards(self) -> None:
        self.assertTrue(FleetEventBus.topic_matches("fleet.*.fail", "fleet.test.fail"))
        self.assertFalse(FleetEventBus.topic_matches("fleet.*.fail", "fleet.test.pass"))


# ─── Core Event Bus Tests ────────────────────────────────────────────────────

class TestFleetEventBus(unittest.TestCase):
    """Tests for the core FleetEventBus."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.log_path = os.path.join(self.tmpdir, "events.jsonl")
        self.bus = FleetEventBus(log_path=self.log_path, persist=True)

    def tearDown(self) -> None:
        self.bus.shutdown()

    def test_publish_and_receive(self) -> None:
        """Test that a published event is delivered to subscribers."""
        received: list[FleetEvent] = []
        self.bus.subscribe("fleet.test.*", received.append, "test-sub")

        event = self.bus.publish("fleet.test.pass", "ci-agent", {"tests": 42})
        time.sleep(0.05)

        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].event_id, event.event_id)
        self.assertEqual(received[0].topic, "fleet.test.pass")

    def test_publish_multiple_subscribers(self) -> None:
        """Test that an event is delivered to all matching subscribers."""
        received_a: list[FleetEvent] = []
        received_b: list[FleetEvent] = []
        self.bus.subscribe("fleet.*", received_a.append, "sub-a")
        self.bus.subscribe("fleet.test.*", received_b.append, "sub-b")

        self.bus.publish("fleet.test.fail", "ci-agent", {"test": "broken"})
        time.sleep(0.05)

        self.assertEqual(len(received_a), 1)
        self.assertEqual(len(received_b), 1)

    def test_no_match_no_delivery(self) -> None:
        """Test that non-matching subscribers don't receive events."""
        received: list[FleetEvent] = []
        self.bus.subscribe("fleet.git.*", received.append, "git-sub")

        self.bus.publish("fleet.test.pass", "ci-agent", {})
        time.sleep(0.05)

        self.assertEqual(len(received), 0)

    def test_event_persistence(self) -> None:
        """Test that events are persisted to the JSONL log file."""
        self.bus.publish("fleet.git.push", "git-agent", {"sha": "abc123"})
        self.bus.publish("fleet.test.fail", "ci-agent", {"test": "login"})
        time.sleep(0.1)

        # Check the log file exists and has content
        self.assertTrue(os.path.exists(self.log_path))
        with open(self.log_path, "r") as f:
            lines = [l.strip() for l in f if l.strip()]
        self.assertEqual(len(lines), 2)

        # Verify we can parse the events back
        event = FleetEvent.from_json(lines[0])
        self.assertEqual(event.topic, "fleet.git.push")

    def test_get_events(self) -> None:
        """Test retrieving events from the bus."""
        self.bus.publish("fleet.test.pass", "ci", {"n": 1})
        self.bus.publish("fleet.test.fail", "ci", {"n": 2})
        self.bus.publish("fleet.git.push", "git", {"n": 3})

        # Get all events
        events = self.bus.get_events(limit=10)
        self.assertEqual(len(events), 3)

        # Filter by topic
        test_events = self.bus.get_events(topic="fleet.test.*")
        self.assertEqual(len(test_events), 2)

        # Filter by source
        ci_events = self.bus.get_events(source="ci")
        self.assertEqual(len(ci_events), 2)

        # Limit
        limited = self.bus.get_events(limit=1)
        self.assertEqual(len(limited), 1)

    def test_event_replay(self) -> None:
        """Test replaying events from a timestamp."""
        self.bus.publish("fleet.test.pass", "ci", {"n": 1})
        time.sleep(0.1)
        since = time.time()
        time.sleep(0.1)
        self.bus.publish("fleet.test.fail", "ci", {"n": 2})
        self.bus.publish("fleet.git.push", "git", {"n": 3})

        # Replay from 'since'
        received: list[FleetEvent] = []
        count = self.bus.replay(since, callback=received.append)
        self.assertGreaterEqual(count, 2)

    def test_active_topics(self) -> None:
        """Test listing active topics."""
        self.bus.publish("fleet.test.pass", "ci", {})
        self.bus.publish("fleet.test.fail", "ci", {})
        self.bus.publish("fleet.test.pass", "ci", {})

        topics = self.bus.get_active_topics()
        topic_names = [t["topic"] for t in topics]
        self.assertIn("fleet.test.pass", topic_names)
        self.assertIn("fleet.test.fail", topic_names)

        pass_topic = [t for t in topics if t["topic"] == "fleet.test.pass"][0]
        self.assertEqual(pass_topic["count"], 2)

    def test_dead_letter_queue(self) -> None:
        """Test that failed deliveries go to the DLQ."""

        def failing_callback(event: FleetEvent) -> None:
            raise RuntimeError("Subscriber exploded!")

        self.bus.subscribe("fleet.test.*", failing_callback, "fail-sub")
        self.bus.publish("fleet.test.pass", "ci", {})

        time.sleep(0.1)
        dlq = self.bus.get_dlq()
        self.assertGreater(len(dlq), 0)
        self.assertEqual(dlq[0].subscriber_id, "fail-sub")
        self.assertIn("Subscriber exploded", dlq[0].reason)

    def test_clear_dlq(self) -> None:
        """Test clearing the dead letter queue."""

        def failing_callback(event: FleetEvent) -> None:
            raise RuntimeError("fail")

        self.bus.subscribe("*", failing_callback, "fail-sub")
        self.bus.publish("fleet.test.pass", "ci", {})
        time.sleep(0.1)

        count = self.bus.clear_dlq()
        self.assertGreater(count, 0)
        self.assertEqual(len(self.bus.get_dlq()), 0)

    def test_stats(self) -> None:
        """Test bus statistics."""
        received: list[FleetEvent] = []
        self.bus.subscribe("*", received.append, "all-sub")

        self.bus.publish("fleet.test.pass", "ci", {})
        self.bus.publish("fleet.test.fail", "ci", {})
        time.sleep(0.1)

        stats = self.bus.get_stats()
        self.assertEqual(stats.total_published, 2)
        self.assertEqual(stats.total_delivered, 2)
        self.assertEqual(stats.subscriber_count, 1)
        self.assertGreater(stats.uptime_seconds, 0)

    def test_unsubscribe(self) -> None:
        """Test unsubscribing a subscriber."""
        received: list[FleetEvent] = []
        sub_id = self.bus.subscribe("fleet.test.*", received.append)

        self.bus.publish("fleet.test.pass", "ci", {})
        time.sleep(0.05)
        self.assertEqual(len(received), 1)

        self.bus.unsubscribe(sub_id)
        self.bus.publish("fleet.test.fail", "ci", {})
        time.sleep(0.05)
        self.assertEqual(len(received), 1)  # No new deliveries

    def test_stream_timeout(self) -> None:
        """Test that long-poll stream returns empty on timeout."""
        events = self.bus.create_stream(topics=["fleet.noop.*"], timeout=0.5)
        self.assertEqual(len(events), 0)

    def test_stream_receive(self) -> None:
        """Test that long-poll stream receives events."""
        # Start a thread that publishes after a short delay
        def publish_later() -> None:
            time.sleep(0.2)
            self.bus.publish("fleet.test.pass", "ci", {"delayed": True})

        t = threading.Thread(target=publish_later, daemon=True)
        t.start()

        events = self.bus.create_stream(topics=["fleet.test.*"], timeout=2.0)
        self.assertGreater(len(events), 0)
        self.assertEqual(events[0].topic, "fleet.test.pass")


# ─── Subscriber Tests ────────────────────────────────────────────────────────

class TestSubscriber(unittest.TestCase):
    """Tests for EventSubscriber and SubscriberManager."""

    def test_basic_matching(self) -> None:
        sub = EventSubscriber("fleet.test.*", lambda e: None, "test-1")
        event = _make_event("fleet.test.pass")
        self.assertTrue(sub.matches(event))

        wrong_event = _make_event("fleet.git.push")
        self.assertFalse(sub.matches(wrong_event))

    def test_filter_by_source(self) -> None:
        sub = EventSubscriber(
            "fleet.*",
            lambda e: None,
            "src-filter",
            filters={"source": "ci-agent"},
        )
        self.assertTrue(sub.matches(_make_event("fleet.test.pass", "ci-agent")))
        self.assertFalse(sub.matches(_make_event("fleet.test.pass", "git-agent")))

    def test_filter_by_source_wildcard(self) -> None:
        sub = EventSubscriber(
            "fleet.*",
            lambda e: None,
            "src-wildcard",
            filters={"source": "ci-*"},
        )
        self.assertTrue(sub.matches(_make_event("fleet.test.pass", "ci-agent")))
        self.assertTrue(sub.matches(_make_event("fleet.test.pass", "ci-runner")))
        self.assertFalse(sub.matches(_make_event("fleet.test.pass", "git-agent")))

    def test_filter_by_priority_min(self) -> None:
        sub = EventSubscriber(
            "fleet.*",
            lambda e: None,
            "prio-filter",
            filters={"priority_min": 8},
        )
        self.assertTrue(sub.matches(_make_event("fleet.test.pass", priority=10)))
        self.assertTrue(sub.matches(_make_event("fleet.test.pass", priority=8)))
        self.assertFalse(sub.matches(_make_event("fleet.test.pass", priority=5)))
        self.assertFalse(sub.matches(_make_event("fleet.test.pass", priority=1)))

    def test_filter_by_data_field(self) -> None:
        sub = EventSubscriber(
            "fleet.*",
            lambda e: None,
            "data-filter",
            filters={"data": {"branch": "main"}},
        )
        self.assertTrue(sub.matches(_make_event("fleet.git.push", data={"branch": "main"})))
        self.assertFalse(sub.matches(_make_event("fleet.git.push", data={"branch": "dev"})))
        self.assertFalse(sub.matches(_make_event("fleet.git.push", data={})))

    def test_delivery_tracking(self) -> None:
        received: list[FleetEvent] = []
        sub = EventSubscriber("fleet.test.*", received.append, "track-1")
        event = _make_event()
        sub.deliver(event)
        self.assertEqual(len(received), 1)
        counts = sub.delivery_counts
        self.assertEqual(counts.get("delivered"), 1)

    def test_delivery_failure_tracking(self) -> None:
        sub = EventSubscriber(
            "fleet.test.*",
            lambda e: (_ for _ in ()).throw(RuntimeError("boom")),
            "fail-1",
        )
        event = _make_event()
        with self.assertRaises(RuntimeError):
            sub.deliver(event)
        counts = sub.delivery_counts
        self.assertEqual(counts.get("failed"), 1)

    def test_throttle(self) -> None:
        received: list[FleetEvent] = []
        sub = EventSubscriber("fleet.test.*", received.append, "throttle-1", throttle_ms=500)

        sub.deliver(_make_event())
        sub.deliver(_make_event())  # Should be throttled
        time.sleep(0.6)
        sub.deliver(_make_event())  # Should pass now

        self.assertEqual(len(received), 2)
        self.assertEqual(sub.delivery_counts.get("throttled"), 1)

    def test_debounce(self) -> None:
        received: list[FleetEvent] = []
        sub = EventSubscriber("fleet.test.*", received.append, "debounce-1", debounce_ms=300)

        sub.deliver(_make_event())
        sub.deliver(_make_event())
        sub.deliver(_make_event())

        self.assertEqual(len(received), 0)
        self.assertEqual(sub.delivery_counts.get("debounced"), 3)

        time.sleep(0.5)
        self.assertEqual(len(received), 1)  # Only latest delivered

    def test_manager_add_remove(self) -> None:
        mgr = SubscriberManager()
        sid = mgr.add("fleet.*", lambda e: None, "test-1")
        self.assertEqual(mgr.count(), 1)

        self.assertTrue(mgr.remove(sid))
        self.assertEqual(mgr.count(), 0)
        self.assertFalse(mgr.remove("nonexistent"))

    def test_manager_get_matching(self) -> None:
        mgr = SubscriberManager()
        mgr.add("fleet.test.*", lambda e: None, "test-sub")
        mgr.add("fleet.git.*", lambda e: None, "git-sub")
        mgr.add("*", lambda e: None, "catch-all")

        event = _make_event("fleet.test.pass")
        matching = mgr.get_matching_subscribers(event)
        ids = {s.subscriber_id for s in matching}
        self.assertIn("test-sub", ids)
        self.assertIn("catch-all", ids)
        self.assertNotIn("git-sub", ids)

    def test_to_dict(self) -> None:
        sub = EventSubscriber("fleet.test.*", lambda e: None, "info-1", throttle_ms=100)
        d = sub.to_dict()
        self.assertEqual(d["subscriber_id"], "info-1")
        self.assertEqual(d["topic_pattern"], "fleet.test.*")
        self.assertEqual(d["throttle_ms"], 100)


# ─── Bridge Tests ────────────────────────────────────────────────────────────

class TestMUDBridge(unittest.TestCase):
    """Tests for the MUD event bridge."""

    def setUp(self) -> None:
        self.commands: list[MUDCommand] = []
        self.bridge = MUDEventBridge(command_callback=self.commands.append)

    def test_git_push_to_announce(self) -> None:
        event = _make_event("fleet.git.push", "git-agent", {"sha": "abc", "branch": "main"})
        self.bridge.handle(event)
        self.assertEqual(len(self.commands), 1)
        self.assertEqual(self.commands[0].command, "announce_commit")
        self.assertEqual(self.commands[0].source, "git-agent")
        self.assertEqual(self.commands[0].args["sha"], "abc")

    def test_test_fail_to_alert(self) -> None:
        event = _make_event("fleet.test.fail", "ci-agent", {"test": "login"})
        self.bridge.handle(event)
        self.assertEqual(len(self.commands), 1)
        self.assertEqual(self.commands[0].command, "alert_failure")

    def test_agent_down_emergency(self) -> None:
        event = _make_event("fleet.agent.health.down", "agent-1", {})
        self.bridge.handle(event)
        self.assertEqual(len(self.commands), 1)
        self.assertEqual(self.commands[0].command, "emergency_broadcast")
        self.assertEqual(self.commands[0].priority, 10)

    def test_unknown_topic_ignored(self) -> None:
        event = _make_event("unknown.topic", "agent", {})
        self.bridge.handle(event)
        self.assertEqual(len(self.commands), 0)

    def test_disabled_bridge(self) -> None:
        self.bridge.enabled = False
        event = _make_event("fleet.git.push", "git-agent", {})
        self.bridge.handle(event)
        self.assertEqual(len(self.commands), 0)

    def test_cicd_pipeline_wildcard(self) -> None:
        event = _make_event("fleet.cicd.pipeline.started", "cicd", {"pipeline": "deploy"})
        self.bridge.handle(event)
        self.assertEqual(len(self.commands), 1)
        self.assertEqual(self.commands[0].command, "pipeline_status")

    def test_buffer(self) -> None:
        self.bridge.handle(_make_event("fleet.git.push", "git", {"sha": "1"}))
        self.bridge.handle(_make_event("fleet.test.fail", "ci", {"test": "x"}))
        buf = self.bridge.get_buffer()
        self.assertEqual(len(buf), 2)

    def test_stats(self) -> None:
        self.bridge.handle(_make_event("fleet.git.push", "git", {}))
        stats = self.bridge.stats
        self.assertEqual(stats["events_processed"], 1)
        self.assertTrue(stats["enabled"])


class TestLogBridge(unittest.TestCase):
    """Tests for the log bridge."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.log_path = os.path.join(self.tmpdir, "bridge-test.jsonl")
        self.bridge = LogBridge(log_path=self.log_path)

    def tearDown(self) -> None:
        self.bridge.close()

    def test_log_event(self) -> None:
        event = _make_event("fleet.test.pass", "ci", {"tests": 42})
        self.bridge.handle(event)
        self.bridge.flush()

        lines = self.bridge.get_log_lines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(lines[0]["topic"], "fleet.test.pass")
        self.assertEqual(lines[0]["source"], "ci")

    def test_exclude_topics(self) -> None:
        bridge = LogBridge(
            log_path=self.log_path,
            exclude_topics=["fleet.agent.health.*"],
        )
        bridge.handle(_make_event("fleet.test.pass"))
        bridge.handle(_make_event("fleet.agent.health.ok"))
        bridge.flush()

        lines = bridge.get_log_lines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(lines[0]["topic"], "fleet.test.pass")
        bridge.close()

    def test_include_fields(self) -> None:
        bridge = LogBridge(
            log_path=self.log_path,
            include_fields=["branch", "sha"],
        )
        event = _make_event("fleet.git.push", "git", {"sha": "abc", "branch": "main", "extra": "removed"})
        bridge.handle(event)
        bridge.flush()

        lines = bridge.get_log_lines()
        self.assertEqual(len(lines[0]["data"]), 2)
        self.assertIn("sha", lines[0]["data"])
        self.assertIn("branch", lines[0]["data"])
        self.assertNotIn("extra", lines[0]["data"])
        bridge.close()

    def test_stats(self) -> None:
        self.bridge.handle(_make_event("fleet.test.pass"))
        self.bridge.handle(_make_event("fleet.test.fail"))
        stats = self.bridge.stats
        self.assertEqual(stats["events_processed"], 2)


class TestWebhookBridge(unittest.TestCase):
    """Tests for the webhook bridge."""

    def test_add_remove_endpoint(self) -> None:
        bridge = WebhookBridge()
        bridge.add_endpoint("fleet.test.fail", "https://example.com/hook")
        self.assertEqual(bridge.endpoint_count, 1)
        self.assertTrue(bridge.remove_endpoint("fleet.test.fail"))
        self.assertEqual(bridge.endpoint_count, 0)

    def test_handle_without_delivery(self) -> None:
        """Test that the bridge processes events without actually calling URLs."""
        bridge = WebhookBridge(endpoints={"fleet.test.*": "https://invalid.test/hook"})
        event = _make_event("fleet.test.fail", "ci", {})
        # Should not raise despite invalid URL (error is caught)
        bridge.handle(event)
        # The event was processed (attempted delivery)
        stats = bridge.stats
        # Either delivered or errored — the bridge handled it
        self.assertGreater(stats["events_processed"] + stats["errors"], 0)


# ─── HTTP Server Tests ──────────────────────────────────────────────────────

class TestHTTPServer(unittest.TestCase):
    """Tests for the HTTP server endpoints."""

    @classmethod
    def setUpClass(cls) -> None:
        from server import EventBusServer
        cls.tmpdir = tempfile.mkdtemp()
        log_path = os.path.join(cls.tmpdir, "server-events.jsonl")
        cls.server = EventBusServer(host="127.0.0.1", port=18900, log_path=log_path)
        cls.server.start(blocking=False)
        cls.base_url = "http://127.0.0.1:18900"
        time.sleep(0.3)  # Give server time to start

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.stop()

    def _get(self, path: str) -> dict:
        import urllib.request
        url = f"{self.base_url}{path}"
        with urllib.request.urlopen(url, timeout=5) as resp:
            return json.loads(resp.read().decode())

    def _post(self, path: str, data: dict) -> dict:
        import urllib.request
        url = f"{self.base_url}{path}"
        payload = json.dumps(data).encode("utf-8")
        req = urllib.request.Request(
            url, data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read().decode())

    def test_health_endpoint(self) -> None:
        result = self._get("/health")
        self.assertIn("status", result)
        self.assertEqual(result["status"], "healthy")

    def test_publish_endpoint(self) -> None:
        result = self._post("/publish", {
            "topic": "fleet.test.pass",
            "source": "http-test",
            "data": {"tests": 10},
            "priority": 5,
        })
        self.assertIn("event", result)
        self.assertEqual(result["event"]["topic"], "fleet.test.pass")
        self.assertEqual(result["event"]["source"], "http-test")

    def test_events_endpoint(self) -> None:
        # Ensure there are events
        self._post("/publish", {"topic": "fleet.git.push", "source": "events-test"})
        result = self._get("/events?limit=5")
        self.assertIn("events", result)
        self.assertGreater(result["count"], 0)

    def test_topics_endpoint(self) -> None:
        result = self._get("/topics")
        self.assertIn("topics", result)
        self.assertIsInstance(result["topics"], list)

    def test_stats_endpoint(self) -> None:
        result = self._get("/stats")
        self.assertIn("stats", result)
        self.assertIn("total_published", result["stats"])
        self.assertIn("events_per_second", result["stats"])

    def test_subscribe_endpoint(self) -> None:
        result = self._post("/subscribe", {
            "topic": "fleet.test.*",
            "subscriber_id": "http-test-sub",
        })
        self.assertIn("subscriber_id", result)
        self.assertEqual(result["subscriber_id"], "http-test-sub")

    def test_dlq_endpoint(self) -> None:
        result = self._get("/dlq")
        self.assertIn("dead_letters", result)
        self.assertIn("count", result)

    def test_subscribers_endpoint(self) -> None:
        result = self._get("/subscribers")
        self.assertIn("subscribers", result)
        self.assertIsInstance(result["subscribers"], list)

    def test_404(self) -> None:
        import urllib.request
        import urllib.error
        url = f"{self.base_url}/nonexistent"
        try:
            urllib.request.urlopen(url, timeout=5)
            self.fail("Should have returned 404")
        except urllib.error.HTTPError as e:
            self.assertEqual(e.code, 404)


# ─── CLI Argument Tests ─────────────────────────────────────────────────────

class TestCLIArgs(unittest.TestCase):
    """Test that CLI argument parsing works correctly."""

    def test_serve_args(self) -> None:
        from cli import build_parser
        parser = build_parser()
        args = parser.parse_args(["serve", "--host", "1.2.3.4", "--port", "9999"])
        self.assertEqual(args.command, "serve")
        self.assertEqual(args.host, "1.2.3.4")
        self.assertEqual(args.port, 9999)

    def test_publish_args(self) -> None:
        from cli import build_parser
        parser = build_parser()
        args = parser.parse_args([
            "publish", "--topic", "fleet.git.push",
            "--data", '{"sha": "abc"}',
            "--source", "cli-test",
            "--priority", "8",
        ])
        self.assertEqual(args.command, "publish")
        self.assertEqual(args.topic, "fleet.git.push")
        self.assertEqual(args.source, "cli-test")
        self.assertEqual(args.priority, 8)

    def test_subscribe_args(self) -> None:
        from cli import build_parser
        parser = build_parser()
        args = parser.parse_args([
            "subscribe", "--topic", "fleet.*",
            "--callback", "echo hello",
            "--throttle-ms", "500",
        ])
        self.assertEqual(args.command, "subscribe")
        self.assertEqual(args.topic, "fleet.*")
        self.assertEqual(args.callback, "echo hello")
        self.assertEqual(args.throttle_ms, 500)

    def test_events_args(self) -> None:
        from cli import build_parser
        parser = build_parser()
        args = parser.parse_args(["events", "--topic", "fleet.test.*", "--limit", "50"])
        self.assertEqual(args.command, "events")
        self.assertEqual(args.topic, "fleet.test.*")
        self.assertEqual(args.limit, 50)

    def test_replay_args(self) -> None:
        from cli import build_parser
        parser = build_parser()
        args = parser.parse_args(["replay", "--since", "3600"])
        self.assertEqual(args.command, "replay")
        self.assertEqual(args.since, "3600")

    def test_stats_args(self) -> None:
        from cli import build_parser
        parser = build_parser()
        args = parser.parse_args(["stats"])
        self.assertEqual(args.command, "stats")

    def test_onboard_args(self) -> None:
        from cli import build_parser
        parser = build_parser()
        args = parser.parse_args(["onboard", "--data-dir", "/tmp/test-bus"])
        self.assertEqual(args.command, "onboard")
        self.assertEqual(args.data_dir, "/tmp/test-bus")


# ─── BusStats Tests ──────────────────────────────────────────────────────────

class TestBusStats(unittest.TestCase):
    """Tests for BusStats dataclass."""

    def test_default_values(self) -> None:
        stats = BusStats()
        self.assertEqual(stats.total_published, 0)
        self.assertEqual(stats.events_per_second, 0.0)

    def test_to_dict(self) -> None:
        stats = BusStats(total_published=42, total_delivered=40)
        d = stats.to_dict()
        self.assertEqual(d["total_published"], 42)
        self.assertEqual(d["total_delivered"], 40)


if __name__ == "__main__":
    unittest.main(verbosity=2)
