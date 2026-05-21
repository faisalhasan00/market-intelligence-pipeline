# Swarm Orchestrator (Control Plane)

Central hub for pipeline execution, event-driven hot paths, budget enforcement, and state commits.

## CLI

```bash
# One-shot full pipeline (crawler → analyst → strategist → alerter)
python -m orchestrator --once "Myntra"

# Adaptive watchlist loop (same behavior as main.py)
python -m orchestrator --watchlist

# HTTP webhook receiver — POST JSON events to trigger hot pipeline
python -m orchestrator --webhook-server
python -m orchestrator --webhook-server --port 8081
```

Alternate entry: `python orchestrator/run.py --once Myntra`

### Webhook API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Liveness check |
| `/webhook` | POST | Ingest event JSON |
| `/events` | POST | Alias of `/webhook` |

**Body (JSON):**

```json
{
  "merchant": "myntra",
  "type": "cashback_spike_detected",
  "severity": "critical"
}
```

`merchant`, `merchant_slug`, or `slug` is required. Events are enqueued for `run_hot_pipeline_for_merchant`.

**Auth (optional):** Set `ORCHESTRATOR_WEBHOOK_SECRET`. Send `X-Orchestrator-Secret: <secret>` or `Authorization: Bearer <secret>`.

## Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `MAX_BUDGET_USD` | `0.50` | Pipeline spend cap |
| `ORCHESTRATOR_MAX_RETRIES` | `2` | Agent retry count |
| `ORCHESTRATOR_RETRY_BACKOFF_SEC` | `2.0` | Exponential backoff base |
| `ORCHESTRATOR_AGENT_TIMEOUT_SEC` | `60` | Per-agent timeout |
| `ORCHESTRATOR_MAX_PARALLEL_MERCHANTS` | `1` | Parallel adaptive crawls |
| `ORCHESTRATOR_EVENT_POLL_SEC` | `5.0` | JSONL event log poll interval |
| `ORCHESTRATOR_POLL_EVENT_LOG` | `true` | Poll `CRAWLER_EVENT_LOG_PATH` |
| `ORCHESTRATOR_EVENT_INTEGRATION` | `true` | Stream subscribe in `main.py` |
| `ORCHESTRATOR_WEBHOOK_PORT` | `8081` | Webhook listen port |
| `ORCHESTRATOR_WEBHOOK_SECRET` | *(empty)* | Optional webhook auth |
| `ORCHESTRATOR_DEAD_LETTER_PATH` | `logs/dead_letter.jsonl` | Contract failure log |
| `ORCHESTRATOR_STATE_LOCK` | `false` | File lock before state commits |
| `ORCHESTRATOR_STATE_LOCK_PATH` | `logs/orchestrator.state.lock` | Lock file path |
| `ORCHESTRATOR_STATE_LOCK_TIMEOUT_SEC` | `30` | Lock acquire timeout |
| `CRAWLER_EVENT_LOG_PATH` | `data/intelligence_stream.jsonl` | Critical event JSONL |
| `SWARM_ALWAYS_PIPELINE` | `false` | Always run analyst+strategist |

## Multi-instance

`ORCHESTRATOR_STATE_LOCK=true` uses an exclusive file lock around each state version bump. **Recommended:** one orchestrator writer per deployment; multiple readers (webhook + watchlist) should share the same lock path if they write state.

## Logs

- Timeline: `logs/swarm_timeline.json` (loguru JSON lines)
- Dead letter: `logs/dead_letter.jsonl` (Pydantic / data contract failures)

## Tests

```bash
python -m unittest tests.test_orchestrator -v
```

## Pipeline feedback

After analyst + strategist, the orchestrator calls `CrawlerAgent.apply_pipeline_feedback()` to persist `analyst_risk` / `strategist_priority` on `merchant_schedule` and escalate crawl cadence (HIGH → critical, MEDIUM → hot).
