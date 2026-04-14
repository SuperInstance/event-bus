"""Event Bus HTTP Server — REST API and event streaming.

Endpoints:
    POST /publish   — publish an event
    POST /subscribe — subscribe to topics (returns subscriber ID)
    GET  /events    — get recent events
    GET  /topics    — list active topics
    GET  /stats     — bus statistics
    GET  /health    — health check
    GET  /stream    — long-poll event streaming
    GET  /dlq       — dead letter queue
    POST /dlq/retry — retry dead letter queue
"""

from __future__ import annotations

import json
import signal
import sys
import threading
import time
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Any
from urllib.parse import urlparse, parse_qs

from event_bus import (
    FleetEvent,
    FleetEventBus,
    PRIORITY_NORMAL,
)
from bridge import MUDEventBridge, WebhookBridge, LogBridge


# ─── Global Bus Instance ─────────────────────────────────────────────────────

_bus: FleetEventBus | None = None
_server: HTTPServer | None = None


def get_bus() -> FleetEventBus:
    """Get or create the global event bus instance."""
    global _bus
    if _bus is None:
        _bus = FleetEventBus()
    return _bus


# ─── Request Handler ─────────────────────────────────────────────────────────

class EventBusHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the event bus API."""

    # Suppress default stderr logging
    def log_message(self, format: str, *args: Any) -> None:
        pass

    def _send_json(self, data: Any, status: int = 200) -> None:
        """Send a JSON response."""
        body = json.dumps(data, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self) -> bytes:
        """Read the request body."""
        content_length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(content_length)

    def _parse_json_body(self) -> dict:
        """Parse JSON request body."""
        body = self._read_body()
        if not body:
            return {}
        return json.loads(body)

    # ── GET Endpoints ────────────────────────────────────────────────

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        params = parse_qs(parsed.query)

        routes = {
            "/events": self._handle_get_events,
            "/topics": self._handle_get_topics,
            "/stats": self._handle_get_stats,
            "/health": self._handle_get_health,
            "/stream": self._handle_get_stream,
            "/dlq": self._handle_get_dlq,
            "/subscribers": self._handle_get_subscribers,
        }

        handler = routes.get(path)
        if handler:
            handler(params)
        else:
            self._send_json({"error": "Not found", "path": path}, 404)

    def _handle_get_events(self, params: dict) -> None:
        bus = get_bus()
        topic = params.get("topic", [None])[0]
        source = params.get("source", [None])[0]
        limit = int(params.get("limit", [100])[0])
        since = params.get("since", [None])[0]
        if since:
            since = float(since)

        events = bus.get_events(topic=topic, source=source, limit=limit, since=since)
        self._send_json({"events": [e.to_dict() for e in events], "count": len(events)})

    def _handle_get_topics(self, params: dict) -> None:
        bus = get_bus()
        topics = bus.get_active_topics()
        self._send_json({"topics": topics})

    def _handle_get_stats(self, params: dict) -> None:
        bus = get_bus()
        stats = bus.get_stats()
        self._send_json({"stats": stats.to_dict()})

    def _handle_get_health(self, params: dict) -> None:
        bus = get_bus()
        stats = bus.get_stats()
        healthy = (
            stats.dead_letter_size < 100
            and stats.total_failed < 1000
        )
        self._send_json({
            "status": "healthy" if healthy else "degraded",
            "uptime": stats.uptime_seconds,
            "published": stats.total_published,
            "delivered": stats.total_delivered,
            "failed": stats.total_failed,
            "dlq_size": stats.dead_letter_size,
            "subscribers": stats.subscriber_count,
        })

    def _handle_get_stream(self, params: dict) -> None:
        bus = get_bus()
        topics_param = params.get("topics", [None])[0]
        topics = topics_param.split(",") if topics_param else None
        timeout = float(params.get("timeout", [30])[0])

        # Long-poll: wait for events or timeout
        events = bus.create_stream(topics=topics, timeout=timeout)

        self._send_json({
            "events": [e.to_dict() for e in events],
            "count": len(events),
        })

    def _handle_get_dlq(self, params: dict) -> None:
        bus = get_bus()
        limit = int(params.get("limit", [100])[0])
        dlq = bus.get_dlq(limit=limit)
        self._send_json({
            "dead_letters": [e.to_dict() for e in dlq],
            "count": len(dlq),
        })

    def _handle_get_subscribers(self, params: dict) -> None:
        bus = get_bus()
        subs = bus.get_subscribers()
        self._send_json({"subscribers": subs})

    # ── POST Endpoints ───────────────────────────────────────────────

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")

        routes = {
            "/publish": self._handle_post_publish,
            "/subscribe": self._handle_post_subscribe,
            "/dlq/retry": self._handle_post_dlq_retry,
            "/dlq/clear": self._handle_post_dlq_clear,
        }

        handler = routes.get(path)
        if handler:
            handler()
        else:
            self._send_json({"error": "Not found", "path": path}, 404)

    def _handle_post_publish(self) -> None:
        bus = get_bus()
        try:
            body = self._parse_json_body()
        except (json.JSONDecodeError, ValueError) as e:
            self._send_json({"error": f"Invalid JSON: {e}"}, 400)
            return

        topic = body.get("topic")
        source = body.get("source", "unknown")
        data = body.get("data", {})
        priority = body.get("priority", PRIORITY_NORMAL)
        ttl = body.get("ttl", 300)

        if not topic:
            self._send_json({"error": "Missing required field: topic"}, 400)
            return

        event = bus.publish(
            topic=topic,
            source=source,
            data=data,
            priority=priority,
            ttl=ttl,
        )

        self._send_json({"event": event.to_dict(), "status": "published"}, 201)

    def _handle_post_subscribe(self) -> None:
        bus = get_bus()
        try:
            body = self._parse_json_body()
        except (json.JSONDecodeError, ValueError) as e:
            self._send_json({"error": f"Invalid JSON: {e}"}, 400)
            return

        topic_pattern = body.get("topic")
        webhook_url = body.get("webhook_url")
        subscriber_id = body.get("subscriber_id")
        filters = body.get("filters", {})
        throttle_ms = body.get("throttle_ms", 0)
        debounce_ms = body.get("debounce_ms", 0)

        if not topic_pattern:
            self._send_json({"error": "Missing required field: topic"}, 400)
            return

        if webhook_url:
            # Create a webhook subscriber
            callback = _make_webhook_callback(webhook_url)
        else:
            # Create an in-memory subscriber (events available via /events)
            callback = _make_store_callback()

        sub_id = bus.subscribe(
            topic_pattern=topic_pattern,
            callback=callback,
            subscriber_id=subscriber_id,
            filters=filters,
            throttle_ms=throttle_ms,
            debounce_ms=debounce_ms,
        )

        self._send_json({
            "subscriber_id": sub_id,
            "topic": topic_pattern,
            "status": "subscribed",
        }, 201)

    def _handle_post_dlq_retry(self) -> None:
        bus = get_bus()
        count = bus.retry_dlq()
        self._send_json({"retried": count})

    def _handle_post_dlq_clear(self) -> None:
        bus = get_bus()
        count = bus.clear_dlq()
        self._send_json({"cleared": count})


# ─── Callback Helpers ────────────────────────────────────────────────────────

# In-memory store for webhook-less subscribers
_subscriber_store: dict[str, list[dict]] = {}
_store_lock = threading.Lock()


def _make_store_callback() -> Any:
    """Create a callback that stores events in memory."""
    def callback(event: FleetEvent) -> None:
        with _store_lock:
            if "default" not in _subscriber_store:
                _subscriber_store["default"] = []
            _subscriber_store["default"].append(event.to_dict())
            # Keep last 1000
            if len(_subscriber_store["default"]) > 1000:
                _subscriber_store["default"] = _subscriber_store["default"][-1000:]
    return callback


def _make_webhook_callback(url: str) -> Any:
    """Create a callback that POSTs events to a webhook URL."""
    import urllib.request

    def callback(event: FleetEvent) -> None:
        try:
            payload = json.dumps(event.to_dict()).encode("utf-8")
            req = urllib.request.Request(
                url,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            urllib.request.urlopen(req, timeout=10)
        except Exception:
            pass  # Webhook failures are silent

    return callback


# ─── Server ──────────────────────────────────────────────────────────────────

class EventBusServer:
    """HTTP server for the event bus.

    Wraps the FleetEventBus in an HTTP interface with REST endpoints
    and long-poll event streaming.
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8900,
        bus: FleetEventBus | None = None,
        log_path: str | None = None,
    ):
        self.host = host
        self.port = port

        # Set up the global bus
        global _bus
        if bus:
            _bus = bus
        elif _bus is None:
            _bus = FleetEventBus(log_path=log_path)

        # Set up bridges
        self.mud_bridge = MUDEventBridge(name="mud-bridge")
        self.webhook_bridge = WebhookBridge(name="webhook-bridge")
        self.log_bridge = LogBridge(name="log-bridge")

        # Register bridges as subscribers
        self.mud_bridge_id = _bus.subscribe(
            topic_pattern="*",
            callback=self.mud_bridge.handle,
            subscriber_id="bridge:mud",
        )
        self.webhook_bridge_id = _bus.subscribe(
            topic_pattern="*",
            callback=self.webhook_bridge.handle,
            subscriber_id="bridge:webhook",
        )
        self.log_bridge_id = _bus.subscribe(
            topic_pattern="*",
            callback=self.log_bridge.handle,
            subscriber_id="bridge:log",
        )

        self._httpd: HTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self, blocking: bool = True) -> None:
        """Start the event bus server.

        Args:
            blocking: If True, block the calling thread. If False, run in background.
        """
        self._httpd = HTTPServer((self.host, self.port), EventBusHandler)
        print(f"[event-bus] Server started on {self.host}:{self.port}")

        if blocking:
            self._serve_forever()
        else:
            self._thread = threading.Thread(target=self._serve_forever, daemon=True)
            self._thread.start()

    def _serve_forever(self) -> None:
        """Run the server loop."""
        if self._httpd:
            self._httpd.serve_forever()

    def stop(self) -> None:
        """Stop the event bus server."""
        if self._httpd:
            self._httpd.shutdown()
            print("[event-bus] Server stopped")
        if _bus:
            _bus.shutdown()
        self.log_bridge.close()

    @property
    def bus(self) -> FleetEventBus:
        return get_bus()

    def setup_signal_handlers(self) -> None:
        """Register signal handlers for graceful shutdown."""
        def handler(signum: int, frame: Any) -> None:
            print(f"\n[event-bus] Received signal {signum}, shutting down...")
            self.stop()
            sys.exit(0)

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)
