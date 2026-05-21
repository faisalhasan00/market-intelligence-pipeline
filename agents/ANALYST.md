# Analyst Agent

Market-gap and defection-risk analysis from crawler intelligence. Rules enforce minimum risk floors; Groq LLM (optional) produces narrative summaries.

## Inputs

`AgentMessage.payload.data["input"]` — crawler `collect_intelligence()` payload:

| Field | Role |
|-------|------|
| `merchant`, `merchant_slug` | Display name and store key |
| `cashback_rate`, `client_rate` | Competitor vs client positioning |
| `events` | Market event list (`type`, `delta`, …) |
| `consensus`, `anomaly`, `evidence_validation` | Quality / contradiction signals |
| `aggregate_confidence` | Crawl confidence 0–1 |
| `crawl_timeline`, `recent_market_events`, `rate_samples` | Optional inline history |

When `ANALYST_USE_HISTORY=true`, the agent also loads from SQLite: rate history, last 3 analyst outputs, and includes them in the LLM prompt.

## Outputs

Validated `AnalystOutputContract` fields plus runtime metadata:

- **Core:** `risk_level`, `gap_summary`, `recommended_action`, `confidence`, `trend`, `gap_found`, `competitor_advantage_pct`
- **Predictive (rules):** `response_probability` (0–1), `predicted_competitor_move` (string)
- **Meta:** `event_risk_floor`, `shadow_test`, `analysis_mode`, `intelligence_events`, snapshots

Formulas for predictive fields are documented in `agents/analyst.py` module docstring.

## Environment

| Variable | Default | Purpose |
|----------|---------|---------|
| `GROQ_API_KEY` | — | Required for LLM path |
| `ANALYST_MODEL` | `llama-3.3-70b-versatile` | Primary model |
| `ANALYST_SHADOW_MODEL` | `llama-3.1-8b-instant` | Shadow comparison |
| `ANALYST_SHADOW_ENABLED` | `true` | Run shadow LLM |
| `ANALYST_USE_HISTORY` | `true` | Store + prompt history |
| `ANALYST_CACHE_ENABLED` | `true` | Semantic LRU cache |
| `ANALYST_CACHE_TTL_SEC` | `3600` | Cache TTL (seconds) |

## How to run

```bash
# Demo (sample Myntra payload)
python -m agents.analyst

# Unit tests (no live API)
python -m unittest tests.test_analyst -v
```

Shadow mismatches are persisted via `IntelligenceStore.record_analyst_shadow_delta()` (SQLite) with JSONL fallback at `logs/analyst_shadow.jsonl`.
