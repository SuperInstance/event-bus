"""CLI — Command-line interface for the Fleet Event Bus.

Subcommands:
    serve       — start event bus server
    publish     — publish an event
    subscribe   — subscribe (runs command on event)
    topics      — list active topics
    events      — show recent events
    replay      — replay events from a timestamp
    stats       — bus statistics
    onboard     — initial setup
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8900
BASE_URL = os.environ.get("FLEET_BUS_URL", f"http://localhost:{DEFAULT_PORT}")


# ─── HTTP Helpers ────────────────────────────────────────────────────────────

def _api_get(path: str, params: dict | None = None) -> dict:
    """Make a GET request to the event bus API."""
    url = f"{BASE_URL}{path}"
    if params:
        qs = "&".join(f"{k}={v}" for k, v in params.items() if v is not None)
        if qs:
            url += f"?{qs}"
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.URLError as e:
        print(f"Error: Cannot connect to event bus at {BASE_URL}")
        print(f"  {e.reason}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


def _api_post(path: str, data: dict) -> dict:
    """Make a POST request to the event bus API."""
    url = f"{BASE_URL}{path}"
    payload = json.dumps(data).encode("utf-8")
    try:
        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.URLError as e:
        print(f"Error: Cannot connect to event bus at {BASE_URL}")
        print(f"  {e.reason}")
        sys.exit(1)
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        try:
            err = json.loads(body)
            print(f"Error ({e.code}): {err.get('error', body)}")
        except Exception:
            print(f"Error ({e.code}): {body}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


# ─── Subcommand Handlers ─────────────────────────────────────────────────────

def cmd_serve(args: argparse.Namespace) -> None:
    """Start the event bus HTTP server."""
    from server import EventBusServer

    host = args.host
    port = args.port
    log_path = args.log

    print(f"╔══════════════════════════════════════════╗")
    print(f"║       Fleet Event Bus v0.1.0             ║")
    print(f"║       Pub/Sub Event System                ║")
    print(f"╚══════════════════════════════════════════╝")
    print(f"")
    print(f"  Listening on: {host}:{port}")
    print(f"  API base:     http://{host}:{port}")
    if log_path:
        print(f"  Event log:    {log_path}")
    print(f"")
    print(f"  Endpoints:")
    print(f"    POST /publish    — publish an event")
    print(f"    POST /subscribe  — subscribe to topics")
    print(f"    GET  /events     — recent events")
    print(f"    GET  /topics     — active topics")
    print(f"    GET  /stats      — bus statistics")
    print(f"    GET  /health     — health check")
    print(f"    GET  /stream     — event stream")
    print(f"    GET  /dlq        — dead letter queue")
    print(f"")

    srv = EventBusServer(host=host, port=port, log_path=log_path)
    srv.setup_signal_handlers()
    srv.start(blocking=True)


def cmd_publish(args: argparse.Namespace) -> None:
    """Publish an event."""
    try:
        data = json.loads(args.data) if args.data else {}
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON data: {e}")
        sys.exit(1)

    payload = {
        "topic": args.topic,
        "source": args.source,
        "data": data,
        "priority": args.priority,
        "ttl": args.ttl,
    }

    result = _api_post("/publish", payload)
    event = result.get("event", {})
    print(f"✓ Event published:")
    print(f"  ID:       {event.get('event_id')}")
    print(f"  Topic:    {event.get('topic')}")
    print(f"  Source:   {event.get('source')}")
    print(f"  Priority: {event.get('priority')}")
    print(f"  Time:     {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(event.get('timestamp', 0)))}")


def cmd_subscribe(args: argparse.Namespace) -> None:
    """Subscribe to events and run a callback command."""
    payload = {
        "topic": args.topic,
        "subscriber_id": args.id,
        "throttle_ms": args.throttle_ms,
        "debounce_ms": args.debounce_ms,
    }

    result = _api_post("/subscribe", payload)
    sub_id = result.get("subscriber_id")
    print(f"✓ Subscribed:")
    print(f"  ID:     {sub_id}")
    print(f"  Topic:  {args.topic}")
    print(f"")

    if args.callback:
        print(f"  Polling for events... (Ctrl+C to stop)")
        print(f"  Callback: {args.callback}")
        print(f"")

        try:
            while True:
                result = _api_get("/stream", {"topics": args.topic, "timeout": "30"})
                events = result.get("events", [])
                for event in events:
                    print(f"  → [{event['topic']}] from {event['source']}: {json.dumps(event['data'])}")
                    # Run callback command
                    try:
                        env = {**os.environ, "FLEET_EVENT": json.dumps(event)}
                        subprocess.run(
                            args.callback,
                            shell=True,
                            env=env,
                            timeout=30,
                            capture_output=True,
                        )
                    except subprocess.TimeoutExpired:
                        print(f"  ! Callback timed out")
                    except Exception as e:
                        print(f"  ! Callback error: {e}")
        except KeyboardInterrupt:
            print(f"\n  Unsubscribed.")


def cmd_topics(args: argparse.Namespace) -> None:
    """List active topics."""
    result = _api_get("/topics")
    topics = result.get("topics", [])

    if not topics:
        print("No active topics.")
        return

    print(f"{'Topic':<40} {'Events':>8}")
    print(f"{'─' * 40} {'─' * 8}")
    for t in topics:
        print(f"{t['topic']:<40} {t['count']:>8}")
    print(f"")
    print(f"Total: {len(topics)} topics")


def cmd_events(args: argparse.Namespace) -> None:
    """Show recent events."""
    params = {"limit": str(args.limit)}
    if args.topic:
        params["topic"] = args.topic
    if args.source:
        params["source"] = args.source

    result = _api_get("/events", params)
    events = result.get("events", [])

    if not events:
        print("No events found.")
        return

    for event in events:
        ts = time.strftime("%H:%M:%S", time.localtime(event.get("timestamp", 0)))
        priority = event.get("priority", 5)
        priority_marker = "!" * min(priority, 5)
        print(f"[{ts}] {priority_marker} {event['topic']}")
        print(f"  Source: {event['source']}  |  ID: {event['event_id']}")
        if event.get("data"):
            print(f"  Data: {json.dumps(event['data'])}")
        print()

    print(f"Showing {len(events)} events (total: {result.get('count', 0)})")


def cmd_replay(args: argparse.Namespace) -> None:
    """Replay events from a timestamp."""
    try:
        since = float(args.since)
    except ValueError:
        # Try parsing as a relative time (e.g. "1h", "30m")
        s = args.since.strip()
        multiplier = 1
        if s.endswith("h"):
            multiplier = 3600
            s = s[:-1]
        elif s.endswith("m"):
            multiplier = 60
            s = s[:-1]
        elif s.endswith("s"):
            multiplier = 1
            s = s[:-1]
        try:
            seconds = float(s) * multiplier
        except ValueError:
            print(f"Error: Invalid timestamp format: {args.since}")
            print("  Use a Unix timestamp or relative time (e.g., '30m', '2h', '300s')")
            sys.exit(1)
        since = time.time() - seconds

    params: dict[str, str] = {"since": str(since)}
    if args.topic:
        params["topic"] = args.topic

    result = _api_get("/events", params)
    events = result.get("events", [])

    if not events:
        print("No events to replay.")
        return

    print(f"Replaying {len(events)} events from {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(since))}")
    for event in events:
        ts = time.strftime("%H:%M:%S", time.localtime(event.get("timestamp", 0)))
        print(f"  [{ts}] {event['topic']} from {event['source']}")

    print(f"Replayed {len(events)} events.")


def cmd_stats(args: argparse.Namespace) -> None:
    """Show bus statistics."""
    result = _api_get("/stats")
    stats = result.get("stats", {})

    print(f"╔══════════════════════════════════════════╗")
    print(f"║       Fleet Event Bus Statistics         ║")
    print(f"╚══════════════════════════════════════════╝")
    print(f"")
    print(f"  Uptime:            {stats.get('uptime_seconds', 0):.1f}s")
    print(f"  Events/sec:        {stats.get('events_per_second', 0):.2f}")
    print(f"")
    print(f"  Published:         {stats.get('total_published', 0)}")
    print(f"  Delivered:         {stats.get('total_delivered', 0)}")
    print(f"  Failed:            {stats.get('total_failed', 0)}")
    print(f"  Filtered:          {stats.get('total_filtered', 0)}")
    print(f"")
    print(f"  Subscribers:       {stats.get('subscriber_count', 0)}")
    print(f"  Active Topics:     {stats.get('active_topics', 0)}")
    print(f"  Dead Letters:      {stats.get('dead_letter_size', 0)}")
    print(f"  Backlog:           {stats.get('backlog_size', 0)}")


def cmd_onboard(args: argparse.Namespace) -> None:
    """Set up the event bus environment."""
    print(f"╔══════════════════════════════════════════╗")
    print(f"║       Fleet Event Bus Setup              ║")
    print(f"╚══════════════════════════════════════════╝")
    print()

    # Create data directory
    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    print(f"✓ Data directory: {data_dir}")

    # Create event log directory
    log_dir = data_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    print(f"✓ Log directory:  {log_dir}")

    # Write default config
    config = {
        "host": args.host,
        "port": args.port,
        "log_path": str(log_dir / "events.jsonl"),
        "max_event_log": 10000,
        "max_dlq_size": 1000,
    }
    config_path = data_dir / "config.json"
    config_path.write_text(json.dumps(config, indent=2) + "\n")
    print(f"✓ Config file:    {config_path}")

    # Test the bus
    print()
    print(f"Testing event bus...")
    from event_bus import FleetEventBus

    log_file = str(log_dir / "test-events.jsonl")
    with FleetEventBus(log_path=log_file, persist=True) as bus:
        received = []
        bus.subscribe("fleet.test.*", lambda e: received.append(e), "test-sub")

        bus.publish("fleet.test.pass", "onboard", {"test": "setup"})
        bus.publish("fleet.test.fail", "onboard", {"test": "fail-test"})

        assert len(received) == 2, f"Expected 2 events, got {len(received)}"
        stats = bus.get_stats()

    print(f"✓ Event bus test passed")
    print(f"  Published: {stats.total_published}")
    print(f"  Delivered: {stats.total_delivered}")

    print()
    print(f"Setup complete! Start the bus with:")
    print(f"  python cli.py serve --host {args.host} --port {args.port} --log {log_dir / 'events.jsonl'}")


# ─── Argument Parser ─────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="fleet-bus",
        description="Fleet Event Bus — Publish/Subscribe event system for the Pelagic fleet",
    )
    parser.add_argument(
        "--url",
        default=BASE_URL,
        help=f"Event bus API URL (default: {BASE_URL})",
    )
    sub = parser.add_subparsers(dest="command", help="Available commands")

    # serve
    p_serve = sub.add_parser("serve", help="Start event bus server")
    p_serve.add_argument("--host", default=DEFAULT_HOST, help=f"Bind host (default: {DEFAULT_HOST})")
    p_serve.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"Bind port (default: {DEFAULT_PORT})")
    p_serve.add_argument("--log", help="Event log file path")

    # publish
    p_pub = sub.add_parser("publish", help="Publish an event")
    p_pub.add_argument("--topic", "-t", required=True, help="Event topic")
    p_pub.add_argument("--data", "-d", default="{}", help="Event data as JSON")
    p_pub.add_argument("--source", "-s", default="cli", help="Source agent name")
    p_pub.add_argument("--priority", "-p", type=int, default=5, help="Priority 1-10 (default: 5)")
    p_pub.add_argument("--ttl", type=int, default=300, help="Time-to-live in seconds (default: 300)")

    # subscribe
    p_sub = sub.add_parser("subscribe", help="Subscribe to events")
    p_sub.add_argument("--topic", "-t", required=True, help="Topic pattern to subscribe to")
    p_sub.add_argument("--callback", "-c", help="Shell command to run on each event")
    p_sub.add_argument("--id", help="Custom subscriber ID")
    p_sub.add_argument("--throttle-ms", type=int, default=0, help="Throttle interval in ms")
    p_sub.add_argument("--debounce-ms", type=int, default=0, help="Debounce interval in ms")

    # topics
    sub.add_parser("topics", help="List active topics")

    # events
    p_events = sub.add_parser("events", help="Show recent events")
    p_events.add_argument("--topic", "-t", help="Filter by topic")
    p_events.add_argument("--source", "-s", help="Filter by source")
    p_events.add_argument("--limit", "-n", type=int, default=20, help="Max events to show (default: 20)")

    # replay
    p_replay = sub.add_parser("replay", help="Replay events from a timestamp")
    p_replay.add_argument("--since", required=True, help="Unix timestamp or relative time (e.g. '30m', '2h')")
    p_replay.add_argument("--topic", "-t", help="Filter by topic")

    # stats
    sub.add_parser("stats", help="Show bus statistics")

    # onboard
    p_onboard = sub.add_parser("onboard", help="Initial setup")
    p_onboard.add_argument("--host", default=DEFAULT_HOST)
    p_onboard.add_argument("--port", type=int, default=DEFAULT_PORT)
    p_onboard.add_argument("--data-dir", default=".fleet-bus", help="Data directory")

    return parser


# ─── Main ────────────────────────────────────────────────────────────────────

def main() -> None:
    """Main entry point."""
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    # Update BASE_URL if custom URL provided
    global BASE_URL
    if hasattr(args, "url"):
        BASE_URL = args.url

    commands = {
        "serve": cmd_serve,
        "publish": cmd_publish,
        "subscribe": cmd_subscribe,
        "topics": cmd_topics,
        "events": cmd_events,
        "replay": cmd_replay,
        "stats": cmd_stats,
        "onboard": cmd_onboard,
    }

    handler = commands.get(args.command)
    if handler:
        handler(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
