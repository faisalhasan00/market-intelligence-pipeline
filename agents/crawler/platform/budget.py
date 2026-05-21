"""Hourly crawl cost and per-run vision/LLM caps."""
from __future__ import annotations

import os
from typing import Optional

from agents.crawler.crawl.profiles import competitor_count_for_run
from agents.crawler.platform.store import IntelligenceStore


class CrawlBudget:
    def __init__(self, store: IntelligenceStore):
        self.store = store
        self.hourly_limit = float(os.getenv("MAX_CRAWL_COST_USD_PER_HOUR", "2.0"))
        self.max_vision_per_merchant_day = int(
            os.getenv("MAX_VISION_CALLS_PER_MERCHANT_PER_DAY", "10")
        )
        self.max_llm_per_run = int(os.getenv("MAX_LLM_FALLBACK_PER_RUN", "3"))
        self._llm_this_run = 0

    def begin_run(self) -> None:
        self._llm_this_run = 0

    def hourly_spend(self) -> float:
        return self.store.get_hourly_crawl_cost()

    def is_over_hourly_limit(self) -> bool:
        return self.hourly_spend() >= self.hourly_limit

    def should_conserve(self) -> bool:
        return self.hourly_spend() >= self.hourly_limit * 0.75

    def can_use_vision(self, merchant_slug: str) -> bool:
        if self.is_over_hourly_limit():
            return False
        used = self.store.count_budget_events(
            "vision", merchant_slug=merchant_slug, hours=24
        )
        return used < self.max_vision_per_merchant_day

    def can_use_llm(self) -> bool:
        if self.is_over_hourly_limit():
            return False
        return self._llm_this_run < self.max_llm_per_run

    def record_vision(self, merchant_slug: str) -> None:
        self.store.record_budget_event("vision", merchant_slug=merchant_slug)

    def record_llm(self) -> None:
        self._llm_this_run += 1
        self.store.record_budget_event("llm")

    def max_competitors_for_run(self, *, full_scrape: bool) -> int:
        base = competitor_count_for_run(full_scrape=full_scrape)
        if self.is_over_hourly_limit():
            return min(3, base)
        if self.should_conserve():
            return min(5, base)
        return base

    def status(self) -> dict:
        return {
            "hourly_spend_usd": round(self.hourly_spend(), 4),
            "hourly_limit_usd": self.hourly_limit,
            "llm_used_this_run": self._llm_this_run,
            "llm_limit_per_run": self.max_llm_per_run,
            "conserve": self.should_conserve(),
            "over_limit": self.is_over_hourly_limit(),
        }
