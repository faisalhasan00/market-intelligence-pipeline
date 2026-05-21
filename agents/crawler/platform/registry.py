"""Dynamic merchant registry — DB-backed watchlist with hot promotions."""
from __future__ import annotations

import os
from typing import TYPE_CHECKING, List, Optional, Set

from agents.crawler.crawl.profiles import merchant_display, normalize_query
from agents.crawler.platform.categories import merchant_category
from agents.crawler.platform.store import IntelligenceStore
from agents.crawler.scheduling.policy import CRITICAL_EVENT_TYPES, TIER1_SLUGS

if TYPE_CHECKING:
    from agents.crawler.scheduling.policy import AdaptiveCrawlPolicy


class MerchantRegistry:
    """Seeds and resolves which merchants surveillance should monitor."""

    def __init__(
        self,
        store: IntelligenceStore,
        policy: Optional["AdaptiveCrawlPolicy"] = None,
    ):
        self.store = store
        self.policy = policy
        self._seeded = False

    def seed_from_env(self) -> None:
        if self._seeded:
            return
        names = (
            self.policy.default_watchlist()
            if self.policy
            else _env_watchlist()
        )
        for name in names:
            slug = normalize_query(name)
            tier = 1 if slug in TIER1_SLUGS else 2
            self.store.upsert_merchant(
                slug,
                display_name=name,
                category=merchant_category(slug),
                tier=tier,
                enabled=True,
                client_priority=1 if tier == 1 else 0,
            )
        self._seeded = True

    def get_active_merchants(self) -> List[str]:
        """Display names: enabled registry + hot-window / critical-event promotions."""
        self.seed_from_env()
        slugs: Set[str] = set()
        display: List[str] = []

        for row in self.store.list_merchants(enabled_only=True):
            slug = row["merchant_slug"]
            if self._auto_disabled(slug, row):
                continue
            slugs.add(slug)
            display.append(row["display_name"])

        for slug in self._promoted_slugs():
            if slug not in slugs:
                slugs.add(slug)
                display.append(merchant_display(slug))

        if not display:
            return _env_watchlist() or ["Myntra", "Flipkart"]
        return display

    def promote(self, merchant_slug: str, *, hours: int = 24) -> None:
        self.seed_from_env()
        slug = normalize_query(merchant_slug)
        self.store.promote_merchant(slug, hours=hours)
        self.store.upsert_merchant(
            slug,
            display_name=merchant_display(slug),
            enabled=True,
        )

    def _promoted_slugs(self) -> List[str]:
        extra: List[str] = []
        seen: Set[str] = set()
        for slug in self.store.list_schedules():
            if slug in seen:
                continue
            if self.store.is_in_hot_window(slug) or self.store.is_merchant_promoted(slug):
                extra.append(slug)
                seen.add(slug)
                continue
            recent = self.store.get_recent_events(slug, limit=5)
            if any(e.get("type") in CRITICAL_EVENT_TYPES for e in recent):
                extra.append(slug)
                seen.add(slug)
        return extra

    def _auto_disabled(self, slug: str, row: dict) -> bool:
        if os.getenv("CRAWLER_AUTO_DISABLE_DORMANT", "false").lower() != "true":
            return False
        dormant = int(row.get("dormant_count") or 0)
        return dormant >= int(os.getenv("CRAWLER_DORMANT_DISABLE_THRESHOLD", "10"))


def _env_watchlist() -> List[str]:
    raw = os.getenv("CRAWLER_WATCHLIST", "").strip()
    if raw:
        return [p.strip() for p in raw.split(",") if p.strip()]
    return ["Myntra", "Ajio", "Amazon", "Nykaa", "Flipkart"]
