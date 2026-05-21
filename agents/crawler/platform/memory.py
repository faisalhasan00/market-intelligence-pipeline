"""Persistent crawler memory — selectors, DOM history, merchant behavior."""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from agents.crawler.platform.categories import category_profile, merchant_category
from agents.crawler.platform.store import IntelligenceStore


class CrawlerMemory:
    def __init__(self, store: IntelligenceStore):
        self.store = store

    def get_selector_chain(self, merchant_slug: str, source_name: str) -> List[str]:
        row = self.store.get_parser_memory(merchant_slug, source_name)
        if row and row.get("selector_chain_json"):
            try:
                return json.loads(row["selector_chain_json"])
            except json.JSONDecodeError:
                pass
        return self.store.default_selector_chain(source_name)

    def record_parser_result(
        self,
        merchant_slug: str,
        source_name: str,
        *,
        selector_used: Optional[str],
        dom_fingerprint: str,
        success: bool,
        offer_count: int,
    ) -> None:
        self.store.upsert_parser_memory(
            merchant_slug,
            source_name,
            selector_used=selector_used,
            dom_fingerprint=dom_fingerprint,
            success=success,
            offer_count=offer_count,
        )
        self.store.record_dom_history(
            merchant_slug,
            source_name,
            dom_fingerprint=dom_fingerprint,
            offer_count=offer_count,
        )

    def last_dom_fingerprint(self, merchant_slug: str, source_name: str) -> Optional[str]:
        row = self.store.get_parser_memory(merchant_slug, source_name)
        return row.get("last_dom_fingerprint") if row else None

    def merchant_profile(self, merchant_slug: str) -> Dict[str, Any]:
        row = self.store.get_merchant_memory(merchant_slug) or {}
        cat = merchant_category(merchant_slug)
        prof = category_profile(merchant_slug)
        return {
            "category": row.get("category") or cat,
            "volatility_score": row.get("volatility_score") or prof.volatility,
            "anti_bot_patterns": json.loads(row.get("anti_bot_json") or "[]"),
            "behavior_notes": json.loads(row.get("behavior_json") or "{}"),
        }

    def plan_sources(self, merchant_slug: str, candidates: List[str]) -> List[str]:
        """Order/filter competitors using memory block list and reliability."""
        import os

        profile = self.merchant_profile(merchant_slug)
        notes = profile.get("behavior_notes") or {}
        memory_blocked = set(notes.get("blocked_sources") or [])
        block_counts = self.store.get_block_counts()
        threshold = int(os.getenv("CRAWLER_DEAD_BLOCK_THRESHOLD", "8"))

        planned: List[str] = []
        skipped: List[str] = []
        for name in candidates:
            if name in memory_blocked:
                skipped.append(name)
                continue
            if block_counts.get(name, 0) >= threshold:
                skipped.append(name)
                continue
            planned.append(name)

        if not planned and candidates:
            planned = candidates[: max(2, len(candidates) // 2)]
        return planned

    def update_merchant_behavior(
        self,
        merchant_slug: str,
        *,
        blocked_sources: Optional[List[str]] = None,
        spike_count: int = 0,
    ) -> None:
        notes: Dict[str, Any] = {}
        row = self.store.get_merchant_memory(merchant_slug)
        if row and row.get("behavior_json"):
            try:
                notes = json.loads(row["behavior_json"])
            except json.JSONDecodeError:
                pass
        if blocked_sources:
            prev = set(notes.get("blocked_sources", []))
            notes["blocked_sources"] = list(prev | set(blocked_sources))
        if spike_count:
            notes["recent_spikes"] = notes.get("recent_spikes", 0) + spike_count
        self.store.upsert_merchant_memory(
            merchant_slug,
            category=merchant_category(merchant_slug),
            volatility_score=category_profile(merchant_slug).volatility,
            behavior_json=json.dumps(notes),
        )
