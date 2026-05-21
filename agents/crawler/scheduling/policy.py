"""
Adaptive autonomous crawl policy (crawler-only).

Decides which merchants to watch, how often to crawl, and when to ramp
frequency up or down from market events and crawl history stored in SQLite.

Example: Myntra sale detected → crawl every 5 minutes (INTERVAL_CRITICAL).
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from agents.crawler.crawl.profiles import MERCHANT_DISPLAY, merchant_display, normalize_query
from agents.crawler.platform.store import IntelligenceStore
from agents.crawler.scheduling.actions import ActionType, CrawlAction, CrawlPlan

# Crawl cadence (seconds)
INTERVAL_CRITICAL = 300    # 5 min — active sale / spike / HIGH risk
INTERVAL_HOT = 600         # 10 min — campaigns, new offers
INTERVAL_PRIORITY = 1800   # 30 min — tier-1 baseline
INTERVAL_NORMAL = 3600     # 1 hour
INTERVAL_QUIET = 7200      # 2 hours — stable, no events
INTERVAL_DORMANT = 14400   # 4 hours — long quiet + LOW risk

TIER1_SLUGS = {"myntra", "flipkart", "amazon"}

CRITICAL_EVENT_TYPES = {
    "cashback_spike_detected",
    "high_cashback_observed",
}

HOT_EVENT_TYPES = {
    "cashback_spike_detected",
    "campaign_started",
    "new_offers_detected",
    "high_cashback_observed",
    "exclusive_offer_detected",
    "dom_structure_changed",
    "visual_campaign_changed",
    "visual_sale_detected",
    "visual_campaign_detected",
    "hero_banner_changed",
    "image_only_offer_detected",
    "offer_added",
    "rate_anomaly_detected",
    "market_sweep_initiated",
    "competitive_intent_detected",
    "offer_started",
    "parser_drift_detected",
}

@dataclass(frozen=True)
class CrawlDecision:
    merchant_slug: str
    interval_sec: int
    priority: int  # 1 = highest (claim queue / loop order)
    mode: str      # critical | hot | priority | normal | quiet | dormant
    reason: str


class AdaptiveCrawlPolicy:
    """Self-deciding crawl behavior backed by SQLite intelligence history."""

    def __init__(self, store: Optional[IntelligenceStore] = None):
        self.store = store or IntelligenceStore()

    # --- Merchant universe ---

    def default_watchlist(self) -> List[str]:
        """Display names the radar should consider (env override)."""
        raw = os.getenv("CRAWLER_WATCHLIST", "").strip()
        if raw:
            return [p.strip() for p in raw.split(",") if p.strip()]
        return ["Myntra", "Ajio", "Amazon", "Nykaa", "Flipkart"]

    def full_registry(self) -> List[str]:
        return [merchant_display(slug) for slug in MERCHANT_DISPLAY]

    def active_watchlist(self) -> List[str]:
        """Merchants to monitor via registry (env seed + hot promotions)."""
        from agents.crawler.platform.registry import MerchantRegistry

        return MerchantRegistry(self.store, self).get_active_merchants()

    def plan_crawl(
        self,
        merchant_slug: str,
        *,
        force_full: bool = False,
        budget=None,
    ) -> CrawlPlan:
        """Crawl depth and priority for the next run on this merchant."""
        slug = merchant_slug.lower().strip()
        decision = self.decide_interval(slug)
        full = force_full or decision.mode in ("critical", "hot")
        if os.getenv("CRAWLER_ALWAYS_FULL_SCRAPE", "false").lower() == "true":
            full = True
        reason = decision.reason
        if budget is not None:
            if budget.is_over_hourly_limit():
                full = False
                reason = f"{reason} (budget: hourly cap)"
            elif budget.should_conserve() and not force_full:
                full = False
                reason = f"{reason} (budget: conserve)"
        follow = (
            os.getenv("CRAWLER_FOLLOW_LINKS_ON_CRITICAL", "true").lower() == "true"
            and decision.mode == "critical"
            and (budget is None or not budget.is_over_hourly_limit())
        )
        return CrawlPlan(
            merchant_slug=slug,
            full_scrape=full,
            priority=decision.priority,
            mode=decision.mode,
            reason=reason,
            follow_links=follow,
        )

    def act(self, merchant_slug: str, payload: Dict) -> List[CrawlAction]:
        """Closed-loop actions after a crawl based on intelligence output."""
        slug = merchant_slug.lower().strip()
        events: List[Dict] = payload.get("events") or []
        types = {e.get("type") for e in events}
        actions: List[CrawlAction] = []

        if types & CRITICAL_EVENT_TYPES:
            actions.append(
                CrawlAction(
                    ActionType.FULL_SCRAPE,
                    slug,
                    priority=0,
                    reason="critical market signal",
                )
            )
            actions.append(
                CrawlAction(
                    ActionType.SET_CRITICAL,
                    slug,
                    reason="escalate monitoring cadence",
                )
            )
            self._promote_registry(slug)

        sweep = payload.get("sweep") or {}
        if types & CRITICAL_EVENT_TYPES and not sweep.get("sweep"):
            actions.append(
                CrawlAction(
                    ActionType.MARKET_SWEEP,
                    slug,
                    reason="spike without sweep yet",
                    payload={"events": events},
                )
            )

        anomaly = payload.get("anomaly") or {}
        if anomaly.get("requires_revalidation") or "rate_anomaly_detected" in types:
            delay = int(os.getenv("CRAWLER_RECRAWL_DELAY_SEC", "120"))
            actions.append(
                CrawlAction(
                    ActionType.RECRAWL_IN,
                    slug,
                    delay_sec=delay,
                    priority=0,
                    reason="anomaly revalidation",
                )
            )

        if "consensus_contradiction" in types:
            actions.append(
                CrawlAction(
                    ActionType.RECRAWL_TRUSTED,
                    slug,
                    priority=1,
                    reason="consensus mismatch",
                )
            )

        parser_drifts = [e for e in events if e.get("type") == "parser_drift_detected"]
        if parser_drifts and not any(e.get("recovered") for e in parser_drifts):
            actions.append(
                CrawlAction(
                    ActionType.ROTATE_PROXY,
                    slug,
                    reason="parser drift without recovery",
                )
            )

        decision = self.decide_interval(slug, fresh_events=events)
        if decision.mode == "dormant":
            actions.append(
                CrawlAction(
                    ActionType.SET_DORMANT,
                    slug,
                    reason="sustained quiet period",
                )
            )

        if not actions:
            actions.append(CrawlAction(ActionType.CRAWL_ONLY, slug))
        return actions

    def _promote_registry(self, slug: str) -> None:
        from agents.crawler.platform.registry import MerchantRegistry

        MerchantRegistry(self.store, self).promote(slug, hours=24)

    # --- Interval & escalation ---

    def decide_interval(self, merchant_slug: str, *, fresh_events: Optional[List[Dict]] = None) -> CrawlDecision:
        slug = merchant_slug.lower().strip()
        schedule = self.store.get_merchant_schedule(slug) or {}
        analyst_risk = (schedule.get("analyst_risk") or "LOW").upper()
        strategist_pri = (schedule.get("strategist_priority") or "LOW").upper()
        if analyst_risk in ("HIGH", "CRITICAL") or strategist_pri in ("HIGH", "CRITICAL"):
            return CrawlDecision(
                slug,
                INTERVAL_CRITICAL,
                1,
                "critical",
                "analyst/strategist priority from pipeline",
            )
        if analyst_risk == "MEDIUM" or strategist_pri == "MEDIUM":
            return CrawlDecision(
                slug,
                INTERVAL_HOT,
                2,
                "hot",
                "medium pipeline priority",
            )

        events = fresh_events if fresh_events is not None else self.store.get_recent_events(slug, limit=20)
        event_types = {e.get("type") for e in events}
        if self.store.is_in_hot_window(slug):
            return CrawlDecision(
                slug, INTERVAL_CRITICAL, 1, "critical",
                "hot_until window (sale/spike monitoring)",
            )

        if event_types & CRITICAL_EVENT_TYPES:
            return CrawlDecision(
                slug, INTERVAL_CRITICAL, 1, "critical",
                "cashback spike / high cashback observed",
            )

        if event_types & HOT_EVENT_TYPES:
            return CrawlDecision(
                slug, INTERVAL_HOT, 2, "hot",
                "campaign or elevated market activity",
            )

        if slug in TIER1_SLUGS:
            base_interval, mode = INTERVAL_PRIORITY, "priority"
            reason = "tier-1 merchant"
        else:
            base_interval, mode = INTERVAL_NORMAL, "normal"
            reason = "standard monitoring"

        timeline = self.store.get_merchant_timeline(slug, limit=4)
        if len(timeline) >= 3 and all(t.get("event_count", 0) == 0 for t in timeline):
            return CrawlDecision(slug, INTERVAL_DORMANT, 8, "dormant", "3+ quiet crawls, no events")

        return CrawlDecision(slug, base_interval, 4 if slug in TIER1_SLUGS else 5, mode, reason)

    def after_crawl(
        self,
        merchant_slug: str,
        events: List[Dict],
    ) -> Dict[str, object]:
        """Persist next schedule from crawl outcome (may escalate into critical)."""
        decision = self.decide_interval(merchant_slug, fresh_events=events)
        hot_until = None
        if decision.mode in ("critical", "hot"):
            # Hold elevated cadence: 6× interval before relaxing
            hold_sec = decision.interval_sec * 6
            hot_until = (datetime.now(timezone.utc) + timedelta(seconds=hold_sec)).isoformat()
        self.store.set_schedule(
            merchant_slug,
            interval_sec=decision.interval_sec,
            hot_until=hot_until,
            priority_tier=1 if merchant_slug in TIER1_SLUGS else 2,
            crawl_mode=decision.mode,
            monitor_reason=decision.reason,
        )
        return {
            "crawl_interval_sec": decision.interval_sec,
            "priority_tier": 1 if merchant_slug in TIER1_SLUGS else 2,
            "crawl_mode": decision.mode,
            "monitor_reason": decision.reason,
            "hot_until": hot_until,
        }

    # --- Due selection ---

    def due_merchants(
        self,
        display_names: Optional[List[str]] = None,
        *,
        limit: Optional[int] = None,
    ) -> List[str]:
        """Due merchants sorted by autonomous priority (most urgent first)."""
        names = display_names or self.active_watchlist()
        scored: List[Tuple[int, str]] = []
        any_schedule = bool(self.store.list_schedules())

        for name in names:
            slug = normalize_query(name)
            if not self.store.is_merchant_due(slug):
                if any_schedule:
                    continue
            decision = self.decide_interval(slug)
            scored.append((decision.priority, name))

        if not scored and not any_schedule:
            for name in names:
                slug = normalize_query(name)
                decision = self.decide_interval(slug)
                scored.append((decision.priority, name))

        scored.sort(key=lambda x: x[0])
        ordered = [name for _, name in scored]
        if limit:
            return ordered[:limit]
        return ordered

    def should_run_full_pipeline(
        self,
        *,
        events: List[Dict],
        crawl_mode: Optional[str] = None,
    ) -> bool:
        """Run analyst+strategist when market is hot (used by main.py swarm loop)."""
        if os.getenv("SWARM_ALWAYS_PIPELINE", "false").lower() == "true":
            return True
        if crawl_mode in ("critical", "hot"):
            return True
        types = {e.get("type") for e in events}
        return bool(types & HOT_EVENT_TYPES)

    def sleep_between_iterations(self, merchants_crawled: int) -> int:
        if merchants_crawled >= 4:
            return 60
        if merchants_crawled >= 2:
            return 120
        return 180
