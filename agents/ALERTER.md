# Alerter Agent

Delivers strategist-aligned competitive intelligence alerts with severity routing, deduplication, and multi-channel delivery.

## Channels

| Channel | Env | Behavior |
| :--- | :--- | :--- |
| **File audit** | `ALERTER_AUDIT_PATH` | Always appends JSONL envelope to `data/alerts.jsonl` |
| **Slack** | `SLACK_WEBHOOK_URL` + `ALERTER_SLACK_ENABLED=true` | POST `{ "text": "..." }` to Incoming Webhook |
| **Generic webhook** | `ALERT_WEBHOOK_URL` + `ALERTER_WEBHOOK_ENABLED=true` | Structured envelope (same shape as crawler stream sinks) |

## Severity routing

Analyst `risk_level` + strategist `priority` map to:

- **critical** — HIGH risk or HIGH priority
- **warning** — MEDIUM tier
- **info** — LOW

Routing:

- `critical` → all enabled channels + file
- `warning` → enabled channels if `ALERTER_MIN_SEVERITY` allows (default `warning`)
- `info` → file audit only

## Deduplication & quiet hours

- `ALERTER_DEDUPE_TTL_SEC` (default 3600): skip duplicate `dedupe_key` in SQLite store (`ALERTER_DEDUPE_PATH`)
- `ALERTER_QUIET_HOURS=23-07`: skip non-critical sends during local hours 23:00–07:00

## Input

Prefer strategist `notification_preview` when present (built in `state.contracts.build_notification_preview`). Orchestrator passes full strategist dict plus `analyst_risk_level`, `merchant`, `merchant_slug`.

## Stream hook (optional)

```python
from agents.alerter import register_alerter_on_stream
from agents.crawler.intelligence.streaming.stream import get_intelligence_stream

register_alerter_on_stream(get_intelligence_stream())
```

Critical event types: `cashback_spike_detected`, `rate_anomaly_detected`, `market_sweep_initiated`, `evidence_mismatch`.

Primary path: orchestrator pipeline when analyst risk matches strategist priority.

## Configure Slack

1. Create an [Incoming Webhook](https://api.slack.com/messaging/webhooks) for your workspace channel.
2. Set in `.env`:
   ```env
   SLACK_WEBHOOK_URL=https://hooks.slack.com/services/T.../B.../xxx
   ALERTER_SLACK_ENABLED=true
   ALERTER_MIN_SEVERITY=warning
   ```
3. Run demo: `python -m agents.alerter`

## CLI

```bash
python -m agents.alerter
```

Uses fixture strategist payload; writes audit JSONL even when Slack/webhook disabled.

## Contract

Output validated as `AlerterOutputContract`: `alert_content`, `channel`, `severity`, `merchant_slug`, `dedupe_key`, `sent_at`, `delivery_status`, `channels_attempted`.
