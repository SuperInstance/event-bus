# Fleet Event Bus

**Publish/Subscribe event system for the Pelagic fleet.**

When something happens in one agent (commit pushed, test failed, alert triggered), all interested agents get notified.

## Architecture

```
Agent A ──publish──→ ┌─────────────┐ ──deliver──→ Agent B (subscriber)
Agent C ──publish──→ │  Event Bus   │ ──deliver──→ Agent D (subscriber)
                     │             │ ──deliver──→ MUD Bridge
                     │  topic match │ ──deliver──→ Webhook Bridge
                     │  filtering   │ ──deliver──→ Log Bridge
                     └─────────────┘
```

## Event Flow

1. **Agent publishes** an event to a topic (e.g. `fleet.test.fail`)
2. **Bus matches** the topic against all subscriber patterns (with wildcard support)
3. **Filters** are applied (source, priority, data fields)
4. **Event is delivered** to matching subscribers via callbacks
5. **Undeliverable events** go to the dead letter queue
6. **Bridges** forward events to external systems (MUD, webhooks, log files)

## Topics

| Topic | Description |
|---|---|
| `fleet.agent.boot` | Agent started |
| `fleet.agent.shutdown` | Agent stopped |
| `fleet.agent.health.*` | Health status changes (ok/degraded/down/recovery) |
| `fleet.agent.error` | Agent encountered error |
| `fleet.git.push` | New commit pushed |
| `fleet.git.pr` | Pull request created/updated |
| `fleet.test.pass` | Tests passed |
| `fleet.test.fail` | Tests failed |
| `fleet.keeper.secret.*` | Secret operations |
| `fleet.mud.*` | MUD server events |
| `fleet.cicd.pipeline.*` | CI/CD pipeline events |
| `fleet.alert.*` | Alert events |

## Quick Start

```bash
# Start the event bus server
python cli.py serve --host 0.0.0.0 --port 8900

# Publish an event
python cli.py publish --topic fleet.git.push --data '{"sha":"abc123","branch":"main"}'

# Subscribe (runs a command on each event)
python cli.py subscribe --topic "fleet.git.*" --callback "echo received"

# List active topics
python cli.py topics

# Show recent events
python cli.py events --topic fleet.test.* --limit 10

# Show bus statistics
python cli.py stats
```

## HTTP API

| Method | Endpoint | Description |
|---|---|---|
| POST | `/publish` | Publish an event |
| POST | `/subscribe` | Subscribe to topics |
| GET | `/events` | Get recent events |
| GET | `/topics` | List active topics |
| GET | `/stats` | Bus statistics |
| GET | `/health` | Health check |
| GET | `/stream?topics=...` | Long-poll event stream |

## Features

- **Wildcard topic matching** — `fleet.agent.*` matches `fleet.agent.boot`
- **Event persistence** — JSONL event log for audit trail
- **Event replay** — Replay events from a specific timestamp
- **Dead letter queue** — Events that couldn't be delivered are retained
- **Event filtering** — Subscribers filter by source, priority, data fields
- **Throttle/debounce** — Prevent subscriber flooding
- **External bridges** — MUD, Webhook, and Log integrations
- **Bus statistics** — Events/sec, backlog, subscriber counts
