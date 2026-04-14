"""External Bridges — connect the event bus to external systems.

Bridges:
    - MUDEventBridge: Convert fleet events → MUD commands
    - WebhookBridge: Send fleet events as outgoing webhooks
    - LogBridge: Log all events to file (JSONL audit trail)
"""

from __future__ import annotations

import json
import os
import threading
import time
import urllib.request
import urllib.error
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


# ─── Base Bridge ─────────────────────────────────────────────────────────────

class BaseBridge:
    """Base class for event bus bridges."""

    def __init__(self, name: str = "bridge"):
        self.name = name
        self.enabled = True
        self._event_count = 0
        self._error_count = 0
        self._lock = threading.Lock()

    def handle(self, event: Any) -> None:
        """Process an incoming event. Override in subclasses."""
        raise NotImplementedError

    def _record_event(self) -> None:
        with self._lock:
            self._event_count += 1

    def _record_error(self) -> None:
        with self._lock:
            self._error_count += 1

    @property
    def stats(self) -> dict[str, Any]:
        with self._lock:
            return {
                "name": self.name,
                "enabled": self.enabled,
                "events_processed": self._event_count,
                "errors": self._error_count,
            }


# ─── MUD Event Bridge ───────────────────────────────────────────────────────

@dataclass
class MUDCommand:
    """A command to be sent to the MUD server."""

    command: str
    args: dict[str, Any] = field(default_factory=dict)
    source: str = ""
    timestamp: float = field(default_factory=time.time)
    priority: int = 5  # 1=info, 5=normal, 10=emergency

    def to_dict(self) -> dict:
        return asdict(self)


class MUDEventBridge(BaseBridge):
    """Convert fleet events → MUD commands.

    Event mappings:
        - fleet.git.push  → announce commit in MUD
        - fleet.test.fail → alert in MUD
        - fleet.agent.health.down → emergency broadcast
        - fleet.agent.error → warning in MUD
        - fleet.cicd.pipeline.* → pipeline status updates
        - fleet.alert.* → forward alerts to MUD
        - fleet.agent.boot → announce new agent
        - fleet.agent.shutdown → announce agent leaving
    """

    # Mapping from event topics to MUD command names
    TOPIC_COMMAND_MAP: dict[str, str] = {
        "fleet.git.push": "announce_commit",
        "fleet.git.pr": "announce_pr",
        "fleet.test.pass": "test_result",
        "fleet.test.fail": "alert_failure",
        "fleet.agent.boot": "announce_agent",
        "fleet.agent.shutdown": "announce_departure",
        "fleet.agent.error": "warn_error",
        "fleet.agent.health.down": "emergency_broadcast",
        "fleet.agent.health.degraded": "warn_degraded",
        "fleet.agent.health.recovery": "announce_recovery",
        "fleet.cicd.pipeline.*": "pipeline_status",
        "fleet.alert.*": "alert",
        "fleet.keeper.secret.*": "secret_event",
        "fleet.mud.*": "mud_relay",
    }

    def __init__(
        self,
        command_callback: Callable[[MUDCommand], None] | None = None,
        name: str = "mud-bridge",
    ):
        super().__init__(name=name)
        self._callback = command_callback
        self._command_buffer: list[MUDCommand] = []
        self._buffer_max = 1000

    def handle(self, event: Any) -> None:
        """Convert a fleet event into a MUD command."""
        if not self.enabled:
            return

        command = self._event_to_command(event)
        if command is None:
            return

        # Buffer the command
        self._command_buffer.append(command)
        if len(self._command_buffer) > self._buffer_max:
            self._command_buffer = self._command_buffer[-self._buffer_max:]

        # Send via callback if registered
        if self._callback:
            try:
                self._callback(command)
                self._record_event()
            except Exception:
                self._record_error()
        else:
            self._record_event()

    def _event_to_command(self, event: Any) -> MUDCommand | None:
        """Convert a FleetEvent to a MUDCommand."""
        topic = event.topic
        command_name = None

        # Find matching command (support wildcards)
        for pattern, cmd in self.TOPIC_COMMAND_MAP.items():
            if self._match_topic(pattern, topic):
                command_name = cmd
                break

        if command_name is None:
            return None

        # Build command args from event data
        args = dict(event.data)
        args["_event_id"] = event.event_id
        args["_topic"] = event.topic

        # Determine MUD priority based on event priority
        mud_priority = 5
        if event.priority >= 10:
            mud_priority = 10
        elif event.priority >= 8:
            mud_priority = 7
        elif event.priority <= 2:
            mud_priority = 1

        # Override for critical topics
        if "fleet.agent.health.down" in topic:
            mud_priority = 10
        elif "fleet.test.fail" in topic:
            mud_priority = 8

        return MUDCommand(
            command=command_name,
            args=args,
            source=event.source,
            timestamp=event.timestamp,
            priority=mud_priority,
        )

    @staticmethod
    def _match_topic(pattern: str, topic: str) -> bool:
        """Simple wildcard topic matching."""
        import fnmatch
        return fnmatch.fnmatch(topic, pattern)

    def get_buffer(self, limit: int = 50) -> list[dict]:
        """Get buffered MUD commands."""
        with self._lock:
            cmds = self._command_buffer[-limit:]
        return [c.to_dict() for c in cmds]

    def set_callback(self, callback: Callable[[MUDCommand], None]) -> None:
        """Set or update the command callback."""
        self._callback = callback


# ─── Webhook Bridge ──────────────────────────────────────────────────────────

class WebhookBridge(BaseBridge):
    """Send fleet events as outgoing webhooks.

    For integrations with external systems (Slack, Discord, custom endpoints).
    """

    def __init__(
        self,
        endpoints: dict[str, str] | None = None,
        default_headers: dict[str, str] | None = None,
        timeout: float = 10.0,
        name: str = "webhook-bridge",
    ):
        """Initialize the webhook bridge.

        Args:
            endpoints: Map of topic patterns to webhook URLs.
                       Example: {"fleet.test.fail": "https://hooks.slack.com/..."}
            default_headers: Default HTTP headers for all webhooks.
            timeout: Request timeout in seconds.
        """
        super().__init__(name=name)
        self._endpoints = endpoints or {}
        self._default_headers = default_headers or {"Content-Type": "application/json"}
        self._timeout = timeout
        self._delivery_log: list[dict] = []
        self._log_max = 500

    def add_endpoint(self, topic_pattern: str, url: str) -> None:
        """Add a webhook endpoint for a topic pattern."""
        self._endpoints[topic_pattern] = url

    def remove_endpoint(self, topic_pattern: str) -> bool:
        """Remove a webhook endpoint. Returns True if found."""
        return self._endpoints.pop(topic_pattern, None) is not None

    def handle(self, event: Any) -> None:
        """Send an event to matching webhook endpoints."""
        if not self.enabled:
            return

        import fnmatch

        for pattern, url in self._endpoints.items():
            if fnmatch.fnmatch(event.topic, pattern):
                self._send_webhook(event, url)

    def _send_webhook(self, event: Any, url: str) -> None:
        """Send an event to a single webhook URL."""
        import fnmatch

        payload = json.dumps({
            "event": event.to_dict(),
            "delivered_at": datetime.now(timezone.utc).isoformat(),
        }).encode("utf-8")

        headers = dict(self._default_headers)

        log_entry = {
            "event_id": event.event_id,
            "url": url,
            "timestamp": time.time(),
        }

        try:
            req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                log_entry["status"] = resp.status
                log_entry["success"] = True
            self._record_event()
        except Exception as exc:
            log_entry["success"] = False
            log_entry["error"] = str(exc)
            self._record_error()

        self._delivery_log.append(log_entry)
        if len(self._delivery_log) > self._log_max:
            self._delivery_log = self._delivery_log[-self._log_max:]

    def get_delivery_log(self, limit: int = 50) -> list[dict]:
        """Get recent webhook delivery logs."""
        with self._lock:
            return list(self._delivery_log[-limit:])

    @property
    def endpoint_count(self) -> int:
        return len(self._endpoints)


# ─── Log Bridge ──────────────────────────────────────────────────────────────

class LogBridge(BaseBridge):
    """Log all events to file.

    Writes JSONL (JSON Lines) format for easy parsing and audit trail.
    Each line is a self-contained JSON object with event data and metadata.
    """

    def __init__(
        self,
        log_path: str | Path = "fleet-events.log",
        include_fields: list[str] | None = None,
        exclude_topics: list[str] | None = None,
        buffer_size: int = 100,
        name: str = "log-bridge",
    ):
        """Initialize the log bridge.

        Args:
            log_path: Path to the JSONL log file.
            include_fields: Only log these event data fields (None = all).
            exclude_topics: Topic patterns to exclude from logging.
            buffer_size: Number of events to buffer before flushing.
        """
        super().__init__(name=name)
        self._log_path = Path(log_path)
        self._include_fields = include_fields
        self._exclude_topics = exclude_topics or []
        self._buffer_size = buffer_size

        self._buffer: list[str] = []
        self._file = None
        self._file_lock = threading.Lock()

        self._open_log()

    def _open_log(self) -> None:
        """Open the log file for appending."""
        try:
            self._log_path.parent.mkdir(parents=True, exist_ok=True)
            self._file = open(self._log_path, "a", encoding="utf-8")
        except OSError as e:
            print(f"[log-bridge] Warning: cannot open log file {self._log_path}: {e}")
            self._file = None
            self.enabled = False

    def handle(self, event: Any) -> None:
        """Log an event to the JSONL file."""
        if not self.enabled:
            return

        import fnmatch

        # Check exclusion list
        for pattern in self._exclude_topics:
            if fnmatch.fnmatch(event.topic, pattern):
                return

        # Build log entry
        log_entry = {
            "timestamp": datetime.fromtimestamp(
                event.timestamp, tz=timezone.utc
            ).isoformat(),
            "event_id": event.event_id,
            "topic": event.topic,
            "source": event.source,
            "priority": event.priority,
            "data": event.data,
        }

        # Filter fields if specified
        if self._include_fields:
            filtered_data = {
                k: v for k, v in event.data.items() if k in self._include_fields
            }
            log_entry["data"] = filtered_data

        line = json.dumps(log_entry, default=str)

        with self._file_lock:
            self._buffer.append(line)

            if len(self._buffer) >= self._buffer_size:
                self._flush()

        self._record_event()

    def _flush(self) -> None:
        """Write buffered entries to file."""
        if not self._file or not self._buffer:
            return

        try:
            self._file.write("\n".join(self._buffer) + "\n")
            self._file.flush()
            self._buffer.clear()
        except OSError as exc:
            self._record_error()
            print(f"[log-bridge] Write error: {exc}")

    def flush(self) -> None:
        """Public flush method."""
        with self._file_lock:
            self._flush()

    @property
    def buffer_size(self) -> int:
        with self._file_lock:
            return len(self._buffer)

    def close(self) -> None:
        """Flush and close the log file."""
        with self._file_lock:
            self._flush()
            if self._file:
                try:
                    self._file.close()
                except OSError:
                    pass
                self._file = None

    def get_log_lines(self, limit: int = 100) -> list[dict]:
        """Read recent lines from the log file."""
        if not self._log_path.exists():
            return []

        lines = []
        try:
            with open(self._log_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            lines.append(json.loads(line))
                        except json.JSONDecodeError:
                            pass
        except OSError:
            return []

        return lines[-limit:]

    def __enter__(self) -> LogBridge:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
