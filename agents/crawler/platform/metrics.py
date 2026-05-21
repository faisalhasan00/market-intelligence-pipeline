"""Crawler observability — JSON dict and Prometheus text formats."""
from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

from agents.crawler.platform.budget import CrawlBudget
from agents.crawler.platform.store import IntelligenceStore


def get_crawler_metrics(store: Optional[IntelligenceStore] = None) -> Dict[str, Any]:
    store = store or IntelligenceStore()
    queue = store.get_queue_stats()
    budget = CrawlBudget(store).status()
    runs = store.get_crawl_run_stats(hours=24.0)
    events = store.get_event_stats(hours=24.0)
    return {
        "queue": queue,
        "budget": budget,
        "crawl_runs_24h": runs,
        "events_24h": events,
        "merchants_enabled": len(store.list_merchants(enabled_only=True)),
        "geo": os.getenv("CRAWLER_GEO", "IN"),
        "queue_backend": type(store._queue).__name__.replace("QueueBackend", "").lower()
        if hasattr(store, "_queue")
        else os.getenv("CRAWLER_QUEUE_BACKEND", "sqlite"),
    }


def format_prometheus(metrics: Dict[str, Any]) -> str:
    lines: list[str] = []
    q = metrics.get("queue") or {}
    b = metrics.get("budget") or {}
    r = metrics.get("crawl_runs_24h") or {}
    e = metrics.get("events_24h") or {}

    def gauge(name: str, value: float | int, labels: str = "") -> None:
        lbl = f"{{{labels}}}" if labels else ""
        lines.append(f"crawler_{name}{lbl} {value}")

    gauge("queue_pending", q.get("pending", 0))
    gauge("queue_running", q.get("running", 0))
    gauge("queue_done", q.get("done", 0))
    gauge("queue_failed", q.get("failed", 0))
    gauge("budget_hourly_spend_usd", b.get("hourly_spend_usd", 0))
    gauge("budget_hourly_limit_usd", b.get("hourly_limit_usd", 0))
    gauge("budget_conserve", 1 if b.get("conserve") else 0)
    gauge("crawl_runs_total", r.get("count", 0))
    gauge("crawl_runs_cost_usd", r.get("cost_usd", 0))
    gauge("events_total", e.get("count", 0))
    gauge("merchants_enabled", metrics.get("merchants_enabled", 0))
    return "\n".join(lines) + "\n"


def format_json(metrics: Dict[str, Any], *, indent: int = 2) -> str:
    return json.dumps(metrics, indent=indent)


def print_metrics(*, prometheus: bool = False) -> None:
    m = get_crawler_metrics()
    if prometheus:
        print(format_prometheus(m), end="")
    else:
        print(format_json(m))
