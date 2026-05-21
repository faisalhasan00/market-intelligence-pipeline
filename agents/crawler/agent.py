"""
Autonomous Market Intelligence Collection Agent.

Continuously collects evidence-backed competitive intelligence: crawl, extract,
validate, detect changes, emit market events, and self-schedule monitoring
(see agents/crawler/SPEC.md for full responsibility spec).
"""
from __future__ import annotations

import asyncio
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

from agents.base_agent import BaseAgent
from agents.crawler.crawl.antibot import AntibotRecovery
from agents.crawler.crawl.collector import PlaywrightCollector
from agents.crawler.extraction.confidence import aggregate_confidence
from agents.crawler.intelligence import IntelligenceHub, get_intelligence_stream
from agents.crawler.extraction.validation import validate_intelligence_payload
from agents.crawler.crawl.profiles import (
    competitor_count_for_run,
    merchant_display,
    normalize_query,
)
from agents.crawler.crawl.proxy import ProxyPool
from agents.crawler.crawl.reliability import build_targets_ranked
from agents.crawler.scheduling.policy import (
    INTERVAL_CRITICAL,
    INTERVAL_HOT,
    CrawlDecision,
)
from agents.crawler.scheduling.scheduler import MarketScheduler
from agents.crawler.surveillance import SurveillanceEngine
from agents.crawler.platform.budget import CrawlBudget
from agents.crawler.platform.store import IntelligenceStore
from agents.crawler.crawl.workers import CrawlWorkerPool
from messaging.schemas import AgentMessage, AgentRole, MessageType

if sys.platform.startswith("win"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

load_dotenv()

CYAN = "\033[96m"
RESET = "\033[0m"


class CrawlerAgent(BaseAgent):
    """Autonomous market intelligence collector and surveillance engine."""

    def __init__(self, store: IntelligenceStore | None = None):
        super().__init__(role=AgentRole.CRAWLER, model="gemini-flash-latest", provider="google")
        self.store = store or IntelligenceStore()
        self.scheduler = MarketScheduler(self.store)
        self.policy = self.scheduler.policy
        self.proxy_pool = ProxyPool()
        self.worker_pool = CrawlWorkerPool()
        self.collector_proxy_enabled = self.proxy_pool.enabled
        self._radar_worker_id = f"crawler-{uuid.uuid4().hex[:8]}"
        self.stream = get_intelligence_stream()
        self.intelligence = IntelligenceHub(self.store, self.stream)
        self.budget = CrawlBudget(self.store)
        self.antibot = AntibotRecovery(self.store, self.proxy_pool)
        self._follow_links = False

    async def handle_request(self, message: AgentMessage) -> AgentMessage:
        payload = message.payload.data
        raw_input = payload.get("input", "") if isinstance(payload, dict) else payload
        if isinstance(raw_input, dict):
            raw_query = raw_input.get("query", raw_input.get("merchant", "myntra"))
        else:
            raw_query = str(raw_input)

        full_scrape = bool(payload.get("full_scrape", False)) if isinstance(payload, dict) else False
        data = await self.collect_intelligence(str(raw_query), full_scrape=full_scrape)
        cost = float(data.pop("_cost_usd", 0.0))
        data.pop("phase", None)
        return self.create_response(message, data, cost=cost)

    async def collect_intelligence(
        self,
        raw_query: str,
        *,
        full_scrape: bool = False,
    ) -> Dict[str, Any]:
        """Core collection API — used by orchestrator and autonomous radar."""
        merchant_slug = normalize_query(raw_query)
        merchant_name = merchant_display(merchant_slug)
        self.budget.begin_run()
        plan = self.policy.plan_crawl(
            merchant_slug,
            force_full=full_scrape,
            budget=self.budget,
        )
        self._follow_links = plan.follow_links
        full_scrape = plan.full_scrape
        max_competitors = self.budget.max_competitors_for_run(full_scrape=full_scrape)
        schedule_row = self.store.get_merchant_schedule(merchant_slug) or {}
        crawl_mode = schedule_row.get("crawl_mode") or plan.mode
        started_at = datetime.now(timezone.utc).isoformat()

        if self.budget.should_conserve():
            print(f"   [Budget] Conserving spend (${self.budget.hourly_spend():.2f}/{self.budget.hourly_limit})")

        print(f"\n   [Crawler] Intelligence run: {merchant_name} (slug={merchant_slug})")
        targets = build_targets_ranked(
            merchant_slug, self.store, full_scrape=full_scrape, max_competitors=max_competitors
        )
        print(f"   [Crawler] {len(targets)} competitors | workers={self.worker_pool.concurrency}")
        for t in targets:
            print(f"      → {t.source_name}: {t.url}")

        sources: List[Dict[str, Any]] = []
        total_cost = 0.0

        try:
            async with PlaywrightCollector(self.proxy_pool) as collector:
                sources, total_cost = await self.worker_pool.crawl_all(
                    self,
                    self.store,
                    collector,
                    targets,
                    merchant_name,
                    merchant_slug,
                )
        except ImportError as e:
            print(f"   [Crawler] Playwright unavailable: {e}")
            return self._empty_payload(merchant_name, merchant_slug, error=str(e))

        raw_texts = {s.get("source_name", ""): s.pop("_raw_text", "") for s in sources}
        for s in sources:
            s.pop("_html", None)
        parser_events: List[Dict[str, Any]] = []
        for s in sources:
            parser_events.extend(s.pop("_parser_events", []) or [])
        sources, best_cashback, rate_meta = validate_intelligence_payload(sources, raw_texts)

        all_offers: List[str] = []
        for s in sources:
            all_offers.extend(s.get("offers") or [])

        if rate_meta.get("method") == "no_valid_rate":
            print(f"   [Crawler] No validated cashback rate (noise filtered from page chrome)")

        intel_bundle = await self.intelligence.process(
            merchant_slug,
            sources,
            raw_texts=raw_texts,
            parser_events=parser_events,
            crawl_mode=crawl_mode,
        )
        events = intel_bundle["events"]
        schedule_meta = self.scheduler.mark_crawled(merchant_slug, events)
        timeline = self.store.get_merchant_timeline(merchant_slug, limit=5)
        recent_events = self.store.get_recent_events(merchant_slug, limit=10)
        agg_conf = aggregate_confidence(sources)
        consensus = intel_bundle.get("consensus") or {}
        if consensus.get("confidence"):
            agg_conf = min(0.98, (agg_conf + consensus["confidence"]) / 2)
        best_cashback = consensus.get("consensus_rate_label") or best_cashback

        self.store.record_crawl_run(
            merchant_slug,
            offer_count=len(all_offers),
            event_count=len(events),
            confidence=agg_conf,
            cost_usd=total_cost,
            started_at=started_at,
        )

        if events:
            print(f"   [Crawler] {CYAN}Events:{RESET} {[e['type'] for e in events]}")

        payload = {
            "merchant": merchant_name,
            "merchant_slug": merchant_slug,
            "cashback_rate": best_cashback,
            "offers": list(dict.fromkeys(all_offers))[:30],
            "client_rate": os.getenv("CLIENT_BASE_RATE", "5%"),
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "aggregate_confidence": agg_conf,
            "sources": sources,
            "events": events,
            "recent_market_events": recent_events,
            "offer_lifecycle": self.store.get_offer_lifecycle(merchant_slug, limit=20),
            "crawl_timeline": timeline,
            "monitoring": schedule_meta,
            "source_reliability": dict(self.store.get_source_rankings()),
            "rate_validation": rate_meta,
            "consensus": intel_bundle.get("consensus"),
            "anomaly": intel_bundle.get("anomaly"),
            "category": intel_bundle.get("category"),
            "merchant_memory": intel_bundle.get("merchant_memory"),
            "competitive_intents": [
                e for e in events if e.get("type") == "competitive_intent_detected"
            ],
            "sweep": intel_bundle.get("sweep"),
            "evidence_validation": intel_bundle.get("evidence_validation"),
            "budget": self.budget.status(),
            "intelligence_stream": intel_bundle.get("stream_recent"),
            "data_source": "live_web",
            "summary": (
                f"Intelligence from {len(sources)} sources; "
                f"{len(events)} events; workers={self.worker_pool.concurrency}."
            ),
            "query": merchant_slug,
            "results": sources,
            "_cost_usd": total_cost,
            "phase": "C",
            "crawl_plan": {
                "mode": plan.mode,
                "full_scrape": plan.full_scrape,
                "follow_links": plan.follow_links,
                "reason": plan.reason,
            },
        }
        if os.getenv("CRAWLER_VALIDATE_CONTRACT", "true").lower() == "true":
            from state.contracts import validate_intelligence_output

            validate_intelligence_output(payload)
        return payload

    def get_due_merchants(self, display_names: List[str] | None = None) -> List[str]:
        return self.policy.due_merchants(display_names)

    def apply_pipeline_feedback(
        self,
        merchant_slug: str,
        *,
        analyst_risk: Optional[str] = None,
        strategist_priority: Optional[str] = None,
    ):
        """
        Closed-loop schedule boost from analyst/strategist outputs.
        Persists risk/priority on merchant_schedule and escalates crawl cadence.
        """
        from datetime import timedelta
        from agents.crawler.scheduling.policy import (
            CrawlDecision,
            INTERVAL_CRITICAL,
            INTERVAL_HOT,
        )

        slug = normalize_query(merchant_slug)
        self.store.set_intelligence_feedback(
            slug,
            analyst_risk=analyst_risk,
            strategist_priority=strategist_priority,
        )

        risk = (analyst_risk or "").upper()
        priority = (strategist_priority or "").upper()

        if risk == "HIGH" or priority == "HIGH":
            decision = CrawlDecision(
                slug,
                INTERVAL_CRITICAL,
                1,
                "critical",
                "pipeline feedback: HIGH analyst risk or strategist priority",
            )
        elif risk == "MEDIUM" or priority == "MEDIUM":
            decision = CrawlDecision(
                slug,
                INTERVAL_HOT,
                2,
                "hot",
                "pipeline feedback: elevated risk or priority",
            )
        else:
            decision = self.policy.decide_interval(slug)

        hot_until = None
        if decision.mode in ("critical", "hot"):
            hold_sec = decision.interval_sec * 6
            hot_until = (datetime.now(timezone.utc) + timedelta(seconds=hold_sec)).isoformat()

        self.store.set_schedule(
            slug,
            interval_sec=decision.interval_sec,
            hot_until=hot_until,
            priority_tier=1 if slug in {"myntra", "flipkart", "amazon"} else 2,
            crawl_mode=decision.mode,
            monitor_reason=decision.reason,
        )
        return decision

    def apply_pipeline_feedback(
        self,
        merchant_slug: str,
        *,
        analyst_risk: Optional[str] = None,
        strategist_priority: Optional[str] = None,
        crawler_data: Optional[Dict[str, Any]] = None,
        analysis: Optional[Dict[str, Any]] = None,
        strategy: Optional[Dict[str, Any]] = None,
    ) -> Optional[CrawlDecision]:
        """Update schedule/risk from analyst+strategist pipeline output."""
        if analysis and analyst_risk is None:
            analyst_risk = analysis.get("risk_level")
        if strategy and strategist_priority is None:
            strategist_priority = strategy.get("priority")
        if crawler_data:
            if analyst_risk is None:
                analyst_risk = crawler_data.get("analyst_risk")
            if strategist_priority is None:
                strategist_priority = crawler_data.get("strategist_priority")

        slug = normalize_query(merchant_slug)
        self.store.set_intelligence_feedback(
            slug,
            analyst_risk=analyst_risk,
            strategist_priority=strategist_priority,
        )

        risk = (analyst_risk or "LOW").upper()
        priority = (strategist_priority or "LOW").upper()
        from datetime import timedelta

        if risk in ("HIGH", "CRITICAL") or priority in ("HIGH", "CRITICAL"):
            hot_until = (
                datetime.now(timezone.utc) + timedelta(seconds=INTERVAL_CRITICAL * 6)
            ).isoformat()
            self.store.set_schedule(
                slug,
                interval_sec=INTERVAL_CRITICAL,
                hot_until=hot_until,
                priority_tier=1,
                crawl_mode="critical",
                monitor_reason="pipeline feedback escalation",
            )
            return CrawlDecision(
                slug,
                INTERVAL_CRITICAL,
                1,
                "critical",
                "analyst/strategist escalation",
            )

        if risk == "MEDIUM" or priority == "MEDIUM":
            self.store.set_schedule(
                slug,
                interval_sec=INTERVAL_HOT,
                hot_until=None,
                priority_tier=2,
                crawl_mode="hot",
                monitor_reason="pipeline feedback medium priority",
            )
            return CrawlDecision(
                slug,
                INTERVAL_HOT,
                2,
                "hot",
                "medium pipeline priority",
            )

        return self.policy.decide_interval(slug)

    def monitoring_summary(self) -> List[Dict[str, Any]]:
        """Per-merchant schedule snapshot for operators."""
        rows = []
        for slug, row in self.store.list_schedules().items():
            decision = self.policy.decide_interval(slug)
            rows.append(
                {
                    "merchant": merchant_display(slug),
                    "slug": slug,
                    "due": self.store.is_merchant_due(slug),
                    "mode": row.get("crawl_mode") or decision.mode,
                    "interval_sec": row.get("crawl_interval_sec") or decision.interval_sec,
                    "next_reason": row.get("monitor_reason") or decision.reason,
                    "hot_until": row.get("hot_until"),
                }
            )
        return sorted(rows, key=lambda r: (r["due"] is False, r.get("interval_sec", 9999)))

    async def run_autonomous_forever(
        self,
        merchants: Optional[List[str]] = None,
    ) -> None:
        """Autonomous surveillance — delegates to SurveillanceEngine."""
        await SurveillanceEngine(self).run(merchants)

    def _empty_payload(self, merchant_name: str, slug: str, error: str) -> Dict[str, Any]:
        return {
            "merchant": merchant_name,
            "merchant_slug": slug,
            "cashback_rate": None,
            "offers": [],
            "sources": [],
            "events": [{"type": "collection_failed", "error": error}],
            "aggregate_confidence": 0.0,
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "summary": error,
            "_cost_usd": 0.0,
        }


