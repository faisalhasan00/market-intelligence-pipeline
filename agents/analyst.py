"""
Analyst Agent — market gap and defection risk from crawler intelligence.

Event → risk floor matrix (highest matching floor wins):
  HIGH: cashback_spike_detected, high_cashback_observed, rate_anomaly_detected,
        competitive_intent_detected, market_sweep_initiated, collection_failed
  MEDIUM: campaign_started, new_offers_detected, exclusive_offer_detected,
          cashback_drop_detected, dom_structure_changed, visual_campaign_changed,
          hero_banner_changed, offer_added, offer_set_changed, parser_drift_detected,
          consensus_contradiction, evidence_mismatch, evidence_gap
  LOW (informational, no floor): baseline_collected, parser_recovered,
          parser_drift_adapted, offer_removed, source_cooldown, stage_* lifecycle

Predictive signals (rules, not ML):
  rate_slope = (last_rate - first_rate) / max(n_samples - 1, 1) over last ≤8 rate_samples
  event_density = min(1.0, (len(current_events) + len(recent_market_events)) / 10)
  response_probability = clamp(
      0.25 + min(0.4, max(0, rate_slope / 5))
           + trend_bonus(escalating=0.25, stable=0.10, declining=0.05)
           + floor_bonus(HIGH=0.20, MEDIUM=0.10, LOW=0)
           + event_density * 0.20,
      0, 1)
  predicted_competitor_move: template from trend + HIGH/MEDIUM event types
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import time
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Set

from dotenv import load_dotenv
from groq import Groq
from pydantic import ValidationError

from agents.base_agent import BaseAgent
from messaging.schemas import AgentMessage, AgentRole, MessageType
from state.contracts import AnalystOutputContract, coerce_analyst_payload

load_dotenv(override=True)

# Documented event → minimum risk floor (see module docstring).
EVENT_RISK_MATRIX: Dict[str, str] = {
    "cashback_spike_detected": "HIGH",
    "high_cashback_observed": "HIGH",
    "rate_anomaly_detected": "HIGH",
    "competitive_intent_detected": "HIGH",
    "market_sweep_initiated": "HIGH",
    "collection_failed": "HIGH",
    "campaign_started": "MEDIUM",
    "new_offers_detected": "MEDIUM",
    "exclusive_offer_detected": "MEDIUM",
    "cashback_drop_detected": "MEDIUM",
    "dom_structure_changed": "MEDIUM",
    "visual_campaign_changed": "MEDIUM",
    "hero_banner_changed": "MEDIUM",
    "offer_added": "MEDIUM",
    "offer_set_changed": "MEDIUM",
    "parser_drift_detected": "MEDIUM",
    "consensus_contradiction": "MEDIUM",
    "evidence_mismatch": "MEDIUM",
    "evidence_gap": "MEDIUM",
    "visual_sale_detected": "MEDIUM",
    "image_only_offer_detected": "MEDIUM",
}

_RISK_ORDER = {"UNKNOWN": -1, "LOW": 0, "MEDIUM": 1, "HIGH": 2}
_TREND_BONUS = {"escalating": 0.25, "stable": 0.10, "declining": 0.05}
_FLOOR_BONUS = {"HIGH": 0.20, "MEDIUM": 0.10, "LOW": 0.0}


class _AnalystSemanticCache:
    """In-memory LRU keyed by merchant + consensus_rate + event_types; TTL-bound."""

    def __init__(self, maxsize: int = 64, ttl_sec: int = 3600) -> None:
        self._data: OrderedDict[str, Dict[str, Any]] = OrderedDict()
        self.maxsize = maxsize
        self.ttl_sec = ttl_sec

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        entry = self._data.get(key)
        if not entry:
            return None
        if time.time() - entry["ts"] > self.ttl_sec:
            del self._data[key]
            return None
        self._data.move_to_end(key)
        return dict(entry["value"])

    def set(self, key: str, value: Dict[str, Any]) -> None:
        self._data[key] = {"ts": time.time(), "value": dict(value)}
        self._data.move_to_end(key)
        while len(self._data) > self.maxsize:
            self._data.popitem(last=False)


class AnalystAgent(BaseAgent):
    """
    Analyst Agent: Identifies market gaps and computes defection risk.
    Consumes CrawlerAgent.collect_intelligence() output.
    """

    def __init__(self, model: Optional[str] = None):
        model_name = model or os.getenv("ANALYST_MODEL", "llama-3.3-70b-versatile")
        super().__init__(role=AgentRole.ANALYST, model=model_name, provider="groq")
        api_key = os.getenv("GROQ_API_KEY")
        self.client = Groq(api_key=api_key) if api_key else None
        self.shadow_model = (
            os.getenv("ANALYST_SHADOW_MODEL") or "llama-3.1-8b-instant"
        ).strip()
        self.use_history = os.getenv("ANALYST_USE_HISTORY", "true").lower() in ("1", "true", "yes")
        self.shadow_enabled = os.getenv("ANALYST_SHADOW_ENABLED", "true").lower() in ("1", "true", "yes")
        self.cache_enabled = os.getenv("ANALYST_CACHE_ENABLED", "true").lower() in ("1", "true", "yes")
        ttl = int(os.getenv("ANALYST_CACHE_TTL_SEC", "3600"))
        self._semantic_cache = _AnalystSemanticCache(ttl_sec=ttl)

    async def handle_request(self, message: AgentMessage) -> AgentMessage:
        crawler_data = message.payload.data.get("input", {}) or {}
        events = crawler_data.get("events") or []
        merchant = crawler_data.get("merchant", "unknown")
        slug = crawler_data.get("merchant_slug") or crawler_data.get("query") or merchant
        print(f"\n   [Analyst] Analyzing {merchant} | {len(events)} market events")

        history = self._historical_context(crawler_data)
        trend = self._detect_trend(history)
        rule_floor = self._compute_rule_floor(crawler_data, events)
        predictive = self._compute_predictive_signals(history, events, trend, rule_floor)
        if rule_floor:
            print(f"   [Analyst] Rule-driven risk floor: {rule_floor}")
        if trend == "escalating":
            print(f"   [Analyst] Predictive trend: {trend} (rates rising in history)")
        if predictive.get("response_probability", 0) >= 0.6:
            print(
                f"   [Analyst] Response probability: {predictive['response_probability']:.2f}"
            )

        cache_key = self._cache_key(crawler_data, events)
        cached = self._cache_get(cache_key) if self.cache_enabled else None
        prompt = self._build_prompt(crawler_data, events, history, rule_floor, trend)
        cost = 0.0
        raw_llm: Dict[str, Any] = {}
        analysis_mode: Optional[str] = None

        if cached:
            print("   [Analyst] Semantic cache hit — skipping primary LLM")
            analysis = dict(cached)
            analysis.setdefault("response_probability", predictive.get("response_probability"))
            analysis.setdefault(
                "predicted_competitor_move", predictive.get("predicted_competitor_move")
            )
            analysis["intelligence_events"] = events
            analysis["historical_context_used"] = bool(history)
            analysis_mode = "cache_hit"
        else:
            if self.client or os.getenv("GROQ_API_KEY"):
                content, cost = await self._call_llm(prompt, self.model_name)
                raw_llm = self._clean_json_response(content)
            else:
                print("   [Analyst] No GROQ_API_KEY — rules-only analysis")

            analysis = self._finalize_output(
                raw_llm,
                crawler_data,
                events,
                rule_floor=rule_floor,
                trend=trend,
                history=history,
                predictive=predictive,
            )
            if self.cache_enabled:
                self._cache_set(cache_key, analysis)

        if analysis_mode:
            analysis["analysis_mode"] = analysis_mode

        if (
            self.shadow_enabled
            and not cached
            and (self.client or os.getenv("GROQ_API_KEY"))
        ):
            shadow_content, shadow_cost = await self._call_llm(prompt, self.shadow_model)
            cost += shadow_cost
            shadow_data = self._clean_json_response(shadow_content)
            primary_risk = analysis.get("risk_level")
            shadow_risk = str(shadow_data.get("risk_level", "LOW")).upper()
            match = primary_risk == shadow_risk
            delta = None if match else {"primary": primary_risk, "shadow": shadow_risk}
            analysis["shadow_test"] = {
                "shadow_model": self.shadow_model,
                "shadow_risk": shadow_risk,
                "primary_risk": primary_risk,
                "match": match,
                "delta": delta,
            }
            if not match:
                print(
                    f"[Analyst] SHADOW_DELTA: Primary ({primary_risk}) vs Shadow ({shadow_risk})"
                )
                self._persist_shadow_delta(str(slug), primary_risk, shadow_risk, delta)

        self._persist_analyst_output(str(slug), analysis)
        return self.create_response(message, analysis, cost=cost)

    def _historical_context(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if not self.use_history:
            return {}
        
        timeline = []
        for t in (data.get("crawl_timeline") or [])[:4]:
            if isinstance(t, dict):
                timeline.append({
                    "recorded_at": t.get("recorded_at"),
                    "aggregate_confidence": t.get("aggregate_confidence")
                })

        recent = []
        for e in (data.get("recent_market_events") or [])[:5]:
            if isinstance(e, dict) and e.get("type"):
                recent.append({
                    "type": e.get("type"),
                    "delta": e.get("delta")
                })

        ctx: Dict[str, Any] = {
            "crawl_timeline": timeline,
            "recent_market_events": recent,
        }
        rate_samples = data.get("rate_samples")
        slug = data.get("merchant_slug") or data.get("query")
        if slug:
            try:
                from agents.crawler.platform.store import IntelligenceStore

                store = IntelligenceStore()
                if rate_samples is None:
                    rate_samples = store.get_rate_history(str(slug), limit=10)
                
                prior = []
                for row in store.get_analyst_history(str(slug), limit=2):
                    if isinstance(row, dict):
                        prior.append({
                            "risk_level": row.get("risk_level"),
                            "competitor_rate": row.get("competitor_rate"),
                            "client_rate": row.get("client_rate")
                        })
                ctx["prior_analyst_outputs"] = prior
            except Exception:
                ctx.setdefault("prior_analyst_outputs", [])
        if rate_samples is None:
            rate_samples = []
        ctx["rate_samples"] = rate_samples[:8]
        return ctx

    def _compute_predictive_signals(
        self,
        history: Dict[str, Any],
        events: List[Dict[str, Any]],
        trend: str,
        rule_floor: Optional[str],
    ) -> Dict[str, Any]:
        samples = list(history.get("rate_samples") or [])
        if samples and samples[0].get("recorded_at"):
            samples = sorted(samples, key=lambda r: r.get("recorded_at") or "")
        rates: List[float] = []
        for row in samples[-8:]:
            pct = row.get("rate_pct")
            if pct is not None:
                try:
                    rates.append(float(pct))
                except (TypeError, ValueError):
                    continue
        rate_slope = 0.0
        if len(rates) >= 2:
            rate_slope = (rates[-1] - rates[0]) / max(len(rates) - 1, 1)

        recent = history.get("recent_market_events") or []
        event_density = min(1.0, (len(events) + len(recent)) / 10.0)
        slope_factor = min(0.4, max(0.0, rate_slope / 5.0))
        trend_bonus = _TREND_BONUS.get(trend, 0.10)
        floor_bonus = _FLOOR_BONUS.get((rule_floor or "LOW").upper(), 0.0)
        response_probability = round(
            min(
                1.0,
                max(
                    0.0,
                    0.25 + slope_factor + trend_bonus + floor_bonus + event_density * 0.20,
                ),
            ),
            3,
        )

        event_types = {e.get("type") for e in events if e.get("type")}
        high_types = {t for t in event_types if EVENT_RISK_MATRIX.get(t) == "HIGH"}
        if "cashback_spike_detected" in high_types or trend == "escalating":
            move = "Likely cashback rate increase or promotional boost within 48-72h"
        elif "competitive_intent_detected" in high_types or "market_sweep_initiated" in high_types:
            move = "Aggressive competitive sweep; expect multi-source rate pushes"
        elif "campaign_started" in event_types or "new_offers_detected" in event_types:
            move = "Campaign or offer refresh expected; monitor banners and exclusives"
        elif trend == "declining":
            move = "Competitor may reduce cashback or pivot to coupon-led offers"
        elif response_probability >= 0.55:
            move = "Elevated competitive pressure; prepare counter-offer options"
        else:
            move = "Status quo; maintain routine monitoring cadence"

        return {
            "response_probability": response_probability,
            "predicted_competitor_move": move,
        }

    @staticmethod
    def _cache_key(data: Dict[str, Any], events: List[Dict[str, Any]]) -> str:
        merchant = str(data.get("merchant_slug") or data.get("merchant") or "unknown")
        consensus = data.get("consensus") or {}
        consensus_rate = consensus.get("consensus_rate") or data.get("cashback_rate") or ""
        event_types = sorted({str(e.get("type")) for e in events if e.get("type")})
        raw = f"{merchant}|{consensus_rate}|{','.join(event_types)}"
        return hashlib.sha256(raw.encode()).hexdigest()[:32]

    def _cache_get(self, key: str) -> Optional[Dict[str, Any]]:
        return self._semantic_cache.get(key)

    def _cache_set(self, key: str, analysis: Dict[str, Any]) -> None:
        slim = {
            k: analysis[k]
            for k in (
                "risk_level",
                "gap_summary",
                "recommended_action",
                "confidence",
                "trend",
                "response_probability",
                "predicted_competitor_move",
                "competitor_advantage_pct",
                "gap_found",
                "competitor_rate",
                "client_rate",
                "reasoning",
                "evidence_refs",
                "event_risk_floor",
            )
            if k in analysis
        }
        self._semantic_cache.set(key, slim)

    @staticmethod
    def _persist_analyst_output(merchant_slug: str, analysis: Dict[str, Any]) -> None:
        if not merchant_slug or merchant_slug == "unknown":
            return
        try:
            from agents.crawler.platform.store import IntelligenceStore

            IntelligenceStore().record_analyst_output(merchant_slug, analysis)
        except Exception:
            pass

    @staticmethod
    def _persist_shadow_delta(
        merchant_slug: str,
        primary_risk: str,
        shadow_risk: str,
        delta: Optional[Dict[str, Any]],
    ) -> None:
        try:
            from agents.crawler.platform.store import IntelligenceStore

            IntelligenceStore().record_analyst_shadow_delta(
                merchant_slug,
                primary_risk=primary_risk or "LOW",
                shadow_risk=shadow_risk or "LOW",
                shadow_model=os.getenv("ANALYST_SHADOW_MODEL", "llama-3.1-8b-instant"),
                delta=delta,
            )
        except Exception:
            pass

    def _detect_trend(self, history: Dict[str, Any]) -> str:
        """MVP predictive hint: rising rate_samples → escalating."""
        samples = list(history.get("rate_samples") or [])
        if samples and samples[0].get("recorded_at"):
            samples = sorted(samples, key=lambda r: r.get("recorded_at") or "")
        rates: List[float] = []
        for row in samples[-8:]:
            pct = row.get("rate_pct")
            if pct is not None:
                try:
                    rates.append(float(pct))
                except (TypeError, ValueError):
                    continue
        if len(rates) >= 3:
            delta = rates[-1] - rates[0]
            if delta >= 2.0 and rates[-1] > rates[0]:
                return "escalating"
            if delta <= -2.0 and rates[-1] < rates[0]:
                return "declining"
        timeline = history.get("crawl_timeline") or []
        if len(timeline) >= 3:
            confs = [
                float(r.get("aggregate_confidence") or 0)
                for r in reversed(timeline[:5])
                if r.get("aggregate_confidence") is not None
            ]
            if len(confs) >= 3 and confs[-1] - confs[0] >= 0.15:
                return "escalating"
        return "stable"

    def _event_risk_floor(self, events: List[Dict[str, Any]]) -> Optional[str]:
        floors: List[str] = []
        for ev in events:
            etype = ev.get("type")
            if etype and etype in EVENT_RISK_MATRIX:
                floors.append(EVENT_RISK_MATRIX[etype])
        return self._max_risk(floors) if floors else None

    def _consensus_anomaly_floor(self, data: Dict[str, Any]) -> Optional[str]:
        floors: List[str] = []
        anomaly = data.get("anomaly") or {}
        if anomaly.get("escalated"):
            floors.append("HIGH")
        elif anomaly.get("anomalies"):
            floors.append("MEDIUM")
        if anomaly.get("requires_revalidation"):
            floors.append("MEDIUM")

        consensus = data.get("consensus") or {}
        contradictions = consensus.get("contradictions") or []
        if contradictions:
            floors.append("MEDIUM")
        if consensus.get("low_confidence"):
            floors.append("MEDIUM")

        ev_val = data.get("evidence_validation") or {}
        mismatches = ev_val.get("mismatches") or ev_val.get("issues") or []
        gaps = ev_val.get("gaps") or []
        if mismatches:
            floors.append("MEDIUM")
        if gaps and len(gaps) >= 2:
            floors.append("MEDIUM")
        if ev_val.get("blocked_sources"):
            floors.append("LOW")

        return self._max_risk(floors) if floors else None

    def _rate_heuristic_floor(self, data: Dict[str, Any]) -> Optional[str]:
        conf = data.get("aggregate_confidence") or 0
        if conf < 0.4:
            return "LOW"
        rate = data.get("cashback_rate") or ""
        try:
            m = re.search(r"(\d+(?:\.\d+)?)", str(rate))
            if m:
                pct = float(m.group(1))
                if pct > 30:
                    return None
                if pct >= 10:
                    return "HIGH"
                if pct >= 5:
                    return "MEDIUM"
        except ValueError:
            pass
        return None

    def _compute_rule_floor(
        self,
        data: Dict[str, Any],
        events: List[Dict[str, Any]],
    ) -> Optional[str]:
        parts = [
            self._event_risk_floor(events),
            self._consensus_anomaly_floor(data),
            self._rate_heuristic_floor(data),
        ]
        if not any(events):
            parts.append("LOW")
        return self._max_risk([p for p in parts if p])

    @staticmethod
    def _max_risk(levels: List[str]) -> Optional[str]:
        if not levels:
            return None
        return max(levels, key=lambda x: _RISK_ORDER.get(x.upper(), 0))

    def _apply_risk_floor(self, analysis: Dict[str, Any], floor: Optional[str]) -> Dict[str, Any]:
        if not floor:
            return analysis
        current = str(analysis.get("risk_level", "LOW")).upper()
        if current != "UNKNOWN" and _RISK_ORDER.get(floor, 0) > _RISK_ORDER.get(current, 0):
            analysis["risk_level"] = floor
            note = f" [Elevated to {floor} by rule floor.]"
            analysis["gap_summary"] = (analysis.get("gap_summary") or "") + note
            analysis["reasoning"] = (analysis.get("reasoning") or analysis.get("gap_summary", "")) + note
        analysis["event_risk_floor"] = floor
        return analysis

    def _advantage_pct(self, crawler_data: Dict[str, Any]) -> Optional[float]:
        client = self._parse_pct(crawler_data.get("client_rate"))
        competitor = self._parse_pct(crawler_data.get("cashback_rate"))
        if client is None or competitor is None:
            return None
        return round(max(0.0, competitor - client), 2)

    @staticmethod
    def _parse_pct(value: Any) -> Optional[float]:
        if value is None:
            return None
        m = re.search(r"(\d+(?:\.\d+)?)", str(value))
        return float(m.group(1)) if m else None

    def _rules_fallback(
        self,
        crawler_data: Dict[str, Any],
        events: List[Dict[str, Any]],
        *,
        rule_floor: Optional[str],
        trend: str,
        predictive: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        slug = crawler_data.get("merchant_slug")
        from agents.crawler.crawl.profiles import MERCHANT_ALIASES
        if slug not in MERCHANT_ALIASES and not crawler_data.get("cashback_rate"):
            risk = "UNKNOWN"
        elif slug == "myntra" and not crawler_data.get("cashback_rate"):
            risk = "HIGH"
        else:
            risk = rule_floor or "LOW"
        adv = self._advantage_pct(crawler_data)
        gap_found = bool(adv and adv > 0.5)
        event_types: Set[str] = {e.get("type") for e in events if e.get("type")}
        summary_parts = [
            f"Rules-only analysis for {crawler_data.get('merchant', 'merchant')}.",
            f"Validated competitor rate: {crawler_data.get('cashback_rate') or 'unknown'}.",
        ]
        if event_types:
            summary_parts.append(f"Events: {', '.join(sorted(event_types)[:8])}.")
        if crawler_data.get("consensus"):
            summary_parts.append("Consensus signals included.")
        if crawler_data.get("anomaly", {}).get("escalated"):
            summary_parts.append("Anomaly engine escalated.")
        payload: Dict[str, Any] = {
            "risk_level": risk,
            "gap_summary": " ".join(summary_parts),
            "competitor_advantage_pct": adv,
            "recommended_action": {
                "HIGH": "Immediate competitive response required.",
                "MEDIUM": "Prepare counter-offer and increase crawl frequency.",
                "LOW": "No urgent action; maintain monitoring.",
                "UNKNOWN": "Maintain current positioning; routine monitoring only.",
            }.get(risk, "Maintain current positioning; routine monitoring only."),
            "confidence": float(crawler_data.get("aggregate_confidence") or 0.4),
            "evidence_refs": [
                s.get("source_name", "")
                for s in (crawler_data.get("sources") or [])[:5]
                if s.get("evidence")
            ],
            "trend": trend,
            "gap_found": gap_found,
            "competitor_rate": crawler_data.get("cashback_rate"),
            "client_rate": crawler_data.get("client_rate"),
            "reasoning": " ".join(summary_parts),
            "analysis_mode": "rules_fallback",
        }
        if predictive:
            payload["response_probability"] = predictive.get("response_probability")
            payload["predicted_competitor_move"] = predictive.get("predicted_competitor_move")
        return coerce_analyst_payload(payload, crawler_data)

    def _finalize_output(
        self,
        raw_llm: Dict[str, Any],
        crawler_data: Dict[str, Any],
        events: List[Dict[str, Any]],
        *,
        rule_floor: Optional[str],
        trend: str,
        history: Dict[str, Any],
        predictive: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if not raw_llm:
            merged = self._rules_fallback(
                crawler_data,
                events,
                rule_floor=rule_floor,
                trend=trend,
                predictive=predictive,
            )
        else:
            merged = coerce_analyst_payload(raw_llm, crawler_data)
            merged.setdefault("trend", trend)
            if merged.get("competitor_advantage_pct") is None:
                merged["competitor_advantage_pct"] = self._advantage_pct(crawler_data)
            if predictive:
                merged.setdefault("response_probability", predictive.get("response_probability"))
                merged.setdefault(
                    "predicted_competitor_move", predictive.get("predicted_competitor_move")
                )

        analysis_mode = merged.pop("analysis_mode", None)
        try:
            contract = AnalystOutputContract.model_validate(merged)
            analysis = contract.model_dump()
        except ValidationError:
            print("   [Analyst] Contract validation failed — using rules fallback")
            analysis = self._rules_fallback(
                crawler_data,
                events,
                rule_floor=rule_floor,
                trend=trend,
                predictive=predictive,
            )
            analysis_mode = analysis.get("analysis_mode")

        if analysis_mode:
            analysis["analysis_mode"] = analysis_mode

        analysis = self._apply_risk_floor(analysis, rule_floor)
        analysis.setdefault("trend", trend)
        if predictive:
            analysis.setdefault("response_probability", predictive.get("response_probability"))
            analysis.setdefault(
                "predicted_competitor_move", predictive.get("predicted_competitor_move")
            )
        analysis["intelligence_events"] = events
        analysis["aggregate_confidence"] = crawler_data.get("aggregate_confidence")
        analysis["evidence_summary"] = self._evidence_summary(crawler_data)
        analysis["historical_context_used"] = bool(history)
        analysis["consensus_snapshot"] = crawler_data.get("consensus")
        analysis["anomaly_snapshot"] = crawler_data.get("anomaly")
        return analysis

    def _evidence_summary(self, data: Dict[str, Any]) -> Dict[str, Any]:
        sources = data.get("sources") or []
        with_evidence = sum(1 for s in sources if s.get("evidence"))
        return {
            "sources_count": len(sources),
            "sources_with_screenshot": with_evidence,
            "best_cashback": data.get("cashback_rate"),
            "monitoring": data.get("monitoring"),
            "evidence_validation": data.get("evidence_validation"),
        }

    def _build_prompt(
        self,
        data: Dict[str, Any],
        events: List[Dict[str, Any]],
        history: Dict[str, Any],
        rule_floor: Optional[str],
        trend: str,
    ) -> str:
        slim_events = [
            {"type": e.get("type"), "delta": e.get("delta")}
            for e in events[:6]
            if isinstance(e, dict) and e.get("type")
        ]
        events_blob = json.dumps(slim_events)
        hint_line = f"Minimum risk from rules: {rule_floor}" if rule_floor else ""
        trend_line = f"Historical trend hint: {trend}" if trend != "stable" else ""
        live_keys = (
            "merchant",
            "cashback_rate",
            "client_rate",
            "aggregate_confidence",
            "summary",
            "rate_validation",
            "data_source",
            "consensus",
            "anomaly",
            "evidence_validation",
            "monitoring",
        )
        live_blob = json.dumps(
            {k: data.get(k) for k in live_keys if data.get(k) is not None}
        )
        history_blob = json.dumps(history)
        prior = history.get("prior_analyst_outputs") or []
        prior_line = (
            f"Prior analyst runs (last {len(prior)}): "
            + json.dumps(prior)
            if prior
            else ""
        )
        return f"""
Analyze competitor intelligence for a coupon/cashback platform.

Live payload: {live_blob}

Market events (ground truth signals): {events_blob}

Historical context (prior crawls / rates): {history_blob}

{prior_line}

{trend_line}
{hint_line}

Rules:
- High-risk events (e.g. cashback_spike_detected, rate_anomaly_detected, competitive_intent_detected) require HIGH risk.
- Factor consensus contradictions and anomaly.escalated into risk.
- evidence_validation mismatches → at least MEDIUM.
- competitor_rate from validated cashback only (ignore >30%).
- If cashback_rate is null, state insufficient data — do not invent rates.
- trend should be escalating|stable|declining aligned with history when provided.

Return ONLY valid JSON:
{{
    "risk_level": "HIGH|MEDIUM|LOW",
    "gap_summary": "2-4 sentences referencing events and rates",
    "competitor_advantage_pct": 0.0,
    "recommended_action": "concrete next step for partnerships team",
    "confidence": 0.0,
    "evidence_refs": ["source names or event types"],
    "trend": "escalating|stable|declining",
    "gap_found": true,
    "competitor_rate": "string",
    "client_rate": "string",
    "reasoning": "same as gap_summary or shorter"
}}
"""


if __name__ == "__main__":
    import asyncio
    import uuid

    from messaging.schemas import Payload

    async def _demo() -> None:
        print("\n--- [ANALYST DEMO] ---")
        agent = AnalystAgent()
        sample = {
            "merchant": "Myntra",
            "merchant_slug": "myntra",
            "cashback_rate": "12%",
            "client_rate": "5%",
            "aggregate_confidence": 0.82,
            "events": [{"type": "cashback_spike_detected", "delta": 7}],
            "crawl_timeline": [
                {"aggregate_confidence": 0.6, "event_count": 1},
                {"aggregate_confidence": 0.75, "event_count": 2},
            ],
            "rate_samples": [
                {"rate_pct": 5.0},
                {"rate_pct": 8.0},
                {"rate_pct": 12.0},
            ],
            "consensus": {"consensus_rate": 12.0, "source_count": 3},
            "sources": [{"source_name": "competitor_a", "evidence": {"screenshot": "x"}}],
        }
        msg = AgentMessage(
            message_id=str(uuid.uuid4()),
            sender=AgentRole.ORCHESTRATOR,
            receiver=AgentRole.ANALYST,
            message_type=MessageType.REQUEST,
            payload=Payload(data={"input": sample}),
        )
        resp = await agent.handle_request(msg)
        print(json.dumps(resp.payload.data, indent=2))

    asyncio.run(_demo())
