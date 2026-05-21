"""
IntelligenceHub — orchestrates post-crawl intelligence modules.

    IntelligenceHub
         ├── consensus/          cross-source validation
         ├── anomaly/            historical baseline anomalies
         ├── change_detection/   DOM, cashback, visual diffs
         ├── lifecycle/          offer_started → removed
         ├── intent/             competitive strategy signals
         ├── sweep/              market-wide escalation
         └── streaming/          realtime event stream
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from agents.crawler.platform.categories import category_profile
from agents.crawler.intelligence.anomaly import AnomalyEngine
from agents.crawler.intelligence.change_detection import detect_events
from agents.crawler.intelligence.consensus import ConsensusValidator
from agents.crawler.intelligence.intent import IntentDetector
from agents.crawler.intelligence.lifecycle import OfferLifecycleTracker
from agents.crawler.intelligence.streaming import IntelligenceStream, get_intelligence_stream
from agents.crawler.intelligence.sweep import MarketSweep
from agents.crawler.extraction.evidence_validator import EvidenceValidator
from agents.crawler.platform.memory import CrawlerMemory
from agents.crawler.platform.store import IntelligenceStore


class IntelligenceHub:
    def __init__(
        self,
        store: IntelligenceStore,
        stream: Optional[IntelligenceStream] = None,
    ):
        self.store = store
        self.stream = stream or get_intelligence_stream()
        self.memory = CrawlerMemory(store)
        self.consensus = ConsensusValidator()
        self.anomaly = AnomalyEngine(store)
        self.lifecycle = OfferLifecycleTracker(store)
        self.intent = IntentDetector()
        self.sweep = MarketSweep(store)
        self.evidence = EvidenceValidator()

    async def process(
        self,
        merchant_slug: str,
        sources: List[Dict[str, Any]],
        *,
        raw_texts: Optional[Dict[str, str]] = None,
        parser_events: Optional[List[Dict[str, Any]]] = None,
        crawl_mode: Optional[str] = None,
    ) -> Dict[str, Any]:
        parser_events = list(parser_events or [])
        all_events: List[Dict[str, Any]] = list(parser_events)

        consensus = self.consensus.validate(sources)
        for c in consensus.get("contradictions") or []:
            all_events.append({
                "type": "consensus_contradiction",
                "merchant": merchant_slug,
                **c,
            })

        evidence_events, evidence_summary = self.evidence.validate(
            merchant_slug,
            sources,
            consensus,
            crawl_mode=crawl_mode,
        )
        all_events.extend(evidence_events)
        if evidence_summary.get("downgraded_sources"):
            for s in sources:
                if s.get("requires_revalidation"):
                    consensus = self.consensus.validate(sources)

        anomaly_report = self.anomaly.analyze(
            merchant_slug,
            sources,
            consensus_rate=consensus.get("consensus_rate"),
        )
        all_events.extend(anomaly_report.get("anomalies") or [])

        all_events.extend(
            detect_events(merchant_slug, sources, self.store, raw_texts=raw_texts)
        )
        warmup = not self.store.has_baseline(merchant_slug)
        all_events.extend(
            self.lifecycle.update(merchant_slug, sources, warmup=warmup)
        )

        intent_signals = self.intent.detect(merchant_slug, sources, all_events)
        all_events.extend(intent_signals)

        sweep_plan: Dict[str, Any] = {"sweep": False}
        if self.sweep.should_sweep(all_events):
            sweep_plan = self.sweep.plan_sweep(merchant_slug, all_events)
            all_events.append(sweep_plan.get("event", {}))

        for ev in all_events:
            await self.stream.emit(ev)

        cat = category_profile(merchant_slug)
        self.memory.update_merchant_behavior(
            merchant_slug,
            blocked_sources=[s["source_name"] for s in sources if s.get("blocked")],
            spike_count=sum(1 for e in all_events if e.get("type") == "cashback_spike_detected"),
        )

        return {
            "events": all_events,
            "consensus": consensus,
            "evidence_validation": evidence_summary,
            "anomaly": anomaly_report,
            "category": cat.name,
            "category_profile": {
                "volatility": cat.volatility,
                "typical_max_cashback": cat.max_cashback_typical,
            },
            "merchant_memory": self.memory.merchant_profile(merchant_slug),
            "sweep": sweep_plan,
            "stream_recent": self.stream.recent(10),
        }
