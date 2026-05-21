# Crawler completion checklist (~100%)

## Phase 1–4 (verified)
- [x] Surveillance engine (`surveillance.py`)
- [x] Policy `act()` closed-loop (`scheduling/policy.py`, `actions.py`)
- [x] Merchant registry + warmup (`platform/registry.py`)
- [x] Evidence validator (`extraction/evidence_validator.py`)
- [x] Crawl budget (`platform/budget.py`)
- [x] Event sinks (`intelligence/streaming/sinks.py`)
- [x] Campaign navigation (`crawl/navigation.py`)
- [x] Antibot recovery (`crawl/antibot.py`)
- [x] Worker CLI (`worker/`, `python -m agents.crawler.worker`)
- [x] Metrics CLI (`--metrics`, `--prometheus`)
- [x] Geo locale (`platform/geo.py`)
- [x] Browser context pool (`crawl/collector.py`)
- [x] Unit tests (`tests/test_crawler_intelligence.py`)

## Phase 2 (this sprint)
- [x] Login / session persistence (`platform/session.py`, wired in collector)
- [x] Redis queue optional backend (`platform/queue.py`, fallback SQLite)
- [x] HTTP metrics server (`--serve-metrics`, `platform/metrics_server.py`)
- [x] Captcha recovery enhancement (events, longer cooldown, double proxy)
- [x] `apply_pipeline_feedback` on `CrawlerAgent`
- [x] Extended tests (session, redis mock, metrics, pipeline feedback)

## Operator commands

```bash
# 24/7 surveillance
python -m agents.crawler --surveillance

# Distributed worker (SQLite or Redis queue)
python -m agents.crawler.worker

# Metrics snapshot / Prometheus text
python -m agents.crawler --metrics
python -m agents.crawler --metrics --prometheus

# Prometheus scrape endpoint
python -m agents.crawler --serve-metrics --port 9090
```

## True deferrals (optional / external)
- Paid captcha solver integration (stub only; set `CRAWLER_CAPTCHA_SOLVER_KEY` — no vendor wired)
- Full merchant-specific login flows beyond generic form fill
- Redis required only when `CRAWLER_QUEUE_BACKEND=redis` + `pip install redis`
