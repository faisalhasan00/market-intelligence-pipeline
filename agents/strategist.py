"""
Strategist Agent — negotiation briefs and merchant action plans from analyst intelligence.

Consumes AnalystAgent output (includes crawler snapshots: consensus, anomaly, events).
"""
from __future__ import annotations

import argparse
import json
import os
import uuid
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from pydantic import ValidationError

from agents.base_agent import BaseAgent
from agents.strategist_approval import (
    approval_queue_enabled,
    list_pending,
    resolve_approval,
    submit_for_approval,
)
from agents.strategist_playbooks import get_playbook, playbooks_enabled
from messaging.schemas import AgentMessage, AgentRole, MessageType
from state.contracts import (
    StrategistOutputContract,
    _RISK_TO_PRIORITY,
    _RISK_TO_URGENCY_HOURS,
    build_notification_preview,
    coerce_strategist_payload,
    compute_counter_offer_suggestion,
    compute_negotiation_leverage,
    validate_strategist_output,
)

load_dotenv()

_PRIORITY_ORDER = {"UNKNOWN": -1, "LOW": 0, "MEDIUM": 1, "HIGH": 2}


class StrategistAgent(BaseAgent):
    """
    Strategist Agent: Generates re-negotiation briefs and threat reports.
    Uses Groq Llama 3 for synthesis with rules fallback when LLM fails.
    """

    def __init__(self, model: Optional[str] = None):
        model_name = model or os.getenv("STRATEGIST_MODEL", "llama-3.3-70b-versatile")
        super().__init__(role=AgentRole.STRATEGIST, model=model_name, provider="groq")
        self.use_playbooks = playbooks_enabled()

    async def handle_request(self, message: AgentMessage) -> AgentMessage:
        analysis = message.payload.data.get("input", {}) or {}
        risk = str(analysis.get("risk_level", "LOW")).upper()
        merchant = analysis.get("merchant") or analysis.get("merchant_slug") or "merchant"
        print(f"\n   [Strategist] Generating strategy for {merchant} | analyst risk: {risk}")

        playbook = (
            get_playbook(analysis.get("merchant_slug"))
            if self.use_playbooks
            else None
        )
        prompt = self._build_prompt(analysis, playbook)
        cost = 0.0
        raw_llm: Dict[str, Any] = {}

        if os.getenv("GROQ_API_KEY"):
            content, cost = await self._call_llm(prompt, self.model_name)
            raw_llm = self._clean_json_response(content)
        else:
            print("   [Strategist] No GROQ_API_KEY — rules-only strategy")

        strategy = self._finalize_output(raw_llm, analysis, playbook=playbook)
        if approval_queue_enabled():
            merchant = analysis.get("merchant") or analysis.get("merchant_slug") or "merchant"
            strategy = submit_for_approval(strategy, merchant=str(merchant))
        return self.create_response(message, strategy, cost=cost)

    def _crawler_summary(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Build crawler context from analyst-embedded snapshots."""
        consensus = analysis.get("consensus_snapshot") or {}
        anomaly = analysis.get("anomaly_snapshot") or {}
        evidence = (analysis.get("evidence_summary") or {}).get("evidence_validation") or {}
        monitoring = (analysis.get("evidence_summary") or {}).get("monitoring") or {}
        events = analysis.get("intelligence_events") or []
        top_events = [
            {"type": e.get("type"), "delta": e.get("delta")}
            for e in events[:8]
            if isinstance(e, dict) and e.get("type")
        ]
        sweep = monitoring.get("sweep") or monitoring.get("market_sweep")
        return {
            "consensus_rate": consensus.get("consensus_rate"),
            "source_count": consensus.get("source_count"),
            "contradictions": len(consensus.get("contradictions") or []),
            "anomaly_escalated": bool(anomaly.get("escalated")),
            "anomaly_count": len(anomaly.get("anomalies") or []),
            "evidence_mismatches": len(
                evidence.get("mismatches") or evidence.get("issues") or []
            ),
            "top_events": top_events,
            "sweep_status": sweep or monitoring.get("sweep_status", "unknown"),
            "aggregate_confidence": analysis.get("aggregate_confidence"),
        }

    def _rules_fallback(
        self,
        analysis: Dict[str, Any],
        *,
        playbook: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        risk = str(analysis.get("risk_level", "LOW")).upper()
        priority = _RISK_TO_PRIORITY.get(risk, "LOW")
        adv = analysis.get("competitor_advantage_pct")
        merchant = analysis.get("merchant") or "merchant"
        competitor = analysis.get("competitor_rate") or "unknown"
        client = analysis.get("client_rate") or "unknown"
        trend = analysis.get("trend") or "stable"

        angle = ""
        if playbook:
            angle = playbook.get("negotiation_angle", "")

        brief_parts = [
            f"Rules-based brief for {merchant}.",
            f"Analyst risk {risk}; competitor {competitor} vs client {client}.",
        ]
        if adv is not None:
            brief_parts.append(f"Competitive advantage ~{adv} pts.")
        if trend == "escalating":
            brief_parts.append("Trend is escalating — accelerate response.")
        if angle:
            brief_parts.append(angle)

        actions = list((playbook or {}).get("sample_actions") or [])[:4]
        if not actions:
            actions = {
                "HIGH": [
                    "Escalate to merchant VP within 4 hours",
                    "Authorize temporary cashback match",
                    "Enable daily competitive crawl",
                ],
                "MEDIUM": [
                    "Schedule partnership call within 48 hours",
                    "Prepare counter-offer deck",
                ],
                "LOW": ["Add to weekly monitoring report"],
                "UNKNOWN": ["Maintain monitoring"],
            }.get(risk, ["Maintain monitoring"])

        events = analysis.get("intelligence_events") or []
        based_on = [
            str(e.get("type")) for e in events if isinstance(e, dict) and e.get("type")
        ][:12]

        return coerce_strategist_payload(
            {
                "recommendation": analysis.get("recommended_action")
                or f"Execute {priority} priority response for {merchant}.",
                "priority": priority,
                "negotiation_brief": " ".join(brief_parts),
                "threat_level": risk,
                "merchant_actions": actions,
                "confidence": float(analysis.get("confidence") or 0.5),
                "based_on_events": based_on,
                "executive_summary": [
                    f"{merchant}: {risk} competitive threat.",
                    f"Gap: competitor {competitor} vs GrabOn client {client}.",
                    f"Trend {trend}; act within {_RISK_TO_URGENCY_HOURS.get(risk, 72)}h.",
                ],
                "talking_points": [
                    f"We have validated intelligence at {analysis.get('confidence', 0):.0%} confidence.",
                    f"Competitor advantage: {adv} pts" if adv is not None else "Rate gap under review.",
                    analysis.get("gap_summary", "")[:180],
                ],
                "urgency_hours": _RISK_TO_URGENCY_HOURS.get(risk, 72),
                "negotiation_leverage": compute_negotiation_leverage(analysis),
                "counter_offer_suggestion": compute_counter_offer_suggestion(analysis),
                "strategy_mode": "rules_fallback",
            },
            analysis,
        )

    def _apply_priority_floor(self, strategy: Dict[str, Any], analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Never under-prioritize vs analyst risk_level."""
        risk = str(analysis.get("risk_level", "LOW")).upper()
        floor = _RISK_TO_PRIORITY.get(risk, "LOW")
        current = str(strategy.get("priority", "LOW")).upper()
        if current != "UNKNOWN" and _PRIORITY_ORDER.get(floor, 0) > _PRIORITY_ORDER.get(current, 0):
            strategy["priority"] = floor
            note = f" [Priority elevated to {floor} to match analyst risk.]"
            strategy["recommendation"] = (strategy.get("recommendation") or "") + note
        strategy["aligned_with_analyst"] = str(strategy.get("priority", "")).upper() == risk
        return strategy

    def _finalize_output(
        self,
        raw_llm: Dict[str, Any],
        analysis: Dict[str, Any],
        *,
        playbook: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if not raw_llm or raw_llm.get("status") == "MOCKED":
            merged = self._rules_fallback(analysis, playbook=playbook)
            mode = "rules_fallback"
        else:
            merged = coerce_strategist_payload(raw_llm, analysis)
            mode = None

        try:
            contract = StrategistOutputContract.model_validate(merged)
            strategy = contract.model_dump()
        except ValidationError:
            print("   [Strategist] Contract validation failed — using rules fallback")
            strategy = self._rules_fallback(analysis, playbook=playbook)
            mode = "rules_fallback"

        strategy = self._apply_priority_floor(strategy, analysis)
        strategy.setdefault(
            "negotiation_leverage", compute_negotiation_leverage(analysis)
        )
        strategy.setdefault(
            "counter_offer_suggestion", compute_counter_offer_suggestion(analysis)
        )
        strategy["notification_preview"] = build_notification_preview(strategy, analysis)
        if mode:
            strategy["strategy_mode"] = mode
        return strategy

    def _build_prompt(
        self,
        analysis: Dict[str, Any],
        playbook: Optional[Dict[str, Any]],
    ) -> str:
        crawler_summary = self._crawler_summary(analysis)
        analyst_keys = (
            "risk_level",
            "gap_summary",
            "recommended_action",
            "competitor_advantage_pct",
            "confidence",
            "evidence_refs",
            "trend",
            "gap_found",
            "competitor_rate",
            "client_rate",
            "reasoning",
            "event_risk_floor",
            "analysis_mode",
        )
        analyst_blob = json.dumps(
            {k: analysis.get(k) for k in analyst_keys if analysis.get(k) is not None},
            indent=2,
        )
        crawler_blob = json.dumps(crawler_summary, indent=2)
        playbook_blob = json.dumps(playbook, indent=2) if playbook else "{}"

        return f"""
You are a GrabOn partnerships strategist. Produce an approval-ready merchant negotiation package.

Analyst assessment (authoritative risk): {analyst_blob}

Crawler intelligence summary: {crawler_blob}

Merchant playbook (tone and angles): {playbook_blob}

Rules:
- priority MUST align with risk_level unless you explicitly justify downgrade in recommendation.
- threat_level should mirror competitive severity (typically same tier as risk_level).
- merchant_actions: 2-5 concrete steps for the partnerships team.
- based_on_events: list event type strings from intelligence (not invented).
- executive_summary: exactly 3 short bullets for VP approval.
- talking_points: 3-5 bullets for merchant call.
- urgency_hours: 4 for HIGH, 24 for MEDIUM, 72 for LOW risk.
- negotiation_leverage: low|medium|high from gap and risk.
- counter_offer_suggestion: one sentence counter-offer tied to competitor_advantage_pct.

Return ONLY valid JSON:
{{
    "recommendation": "string",
    "priority": "HIGH|MEDIUM|LOW",
    "negotiation_brief": "2-4 sentences, playbook tone",
    "threat_level": "HIGH|MEDIUM|LOW",
    "merchant_actions": ["action1", "action2"],
    "confidence": 0.0,
    "based_on_events": ["event_type"],
    "executive_summary": ["bullet1", "bullet2", "bullet3"],
    "talking_points": ["point1", "point2"],
    "urgency_hours": 24,
    "negotiation_leverage": "medium",
    "counter_offer_suggestion": "string"
}}
"""


def _realistic_analyst_fixture() -> Dict[str, Any]:
    """Rich analyst payload for CLI demo (mirrors production shape)."""
    return {
        "merchant": "Myntra",
        "merchant_slug": "myntra",
        "risk_level": "HIGH",
        "gap_summary": (
            "Competitor cashback at 12% vs client 5% on fashion coupons; "
            "cashback_spike_detected with escalating rate trend."
        ),
        "recommended_action": "Immediate competitive match and merchant escalation.",
        "competitor_advantage_pct": 7.0,
        "confidence": 0.86,
        "evidence_refs": ["competitor_a", "cashback_spike_detected"],
        "trend": "escalating",
        "gap_found": True,
        "competitor_rate": "12%",
        "client_rate": "5%",
        "reasoning": "Validated spike; consensus across 3 sources.",
        "event_risk_floor": "HIGH",
        "intelligence_events": [
            {"type": "cashback_spike_detected", "delta": 7},
            {"type": "new_offers_detected"},
        ],
        "aggregate_confidence": 0.86,
        "consensus_snapshot": {
            "consensus_rate": 12.0,
            "source_count": 3,
            "contradictions": [],
        },
        "anomaly_snapshot": {"escalated": False, "anomalies": []},
        "evidence_summary": {
            "sources_count": 3,
            "sources_with_screenshot": 2,
            "best_cashback": "12%",
            "monitoring": {"crawl_mode": "hot", "sweep_status": "idle"},
            "evidence_validation": {"mismatches": []},
        },
    }


def _cli_list_pending() -> int:
    pending = list_pending()
    if not pending:
        print("No pending approvals.")
        return 0
    for row in pending:
        print(
            f"{row.get('approval_id')} | {row.get('merchant')} | "
            f"{row.get('priority')} | {row.get('submitted_at')}"
        )
    return 0


def _cli_resolve(approval_id: str, *, approved: bool) -> int:
    try:
        record = resolve_approval(approval_id, approved=approved)
    except FileNotFoundError as exc:
        print(exc)
        return 1
    status = record.get("approval_status")
    print(f"{approval_id} → {status} ({record.get('_approval_path')})")
    return 0


if __name__ == "__main__":
    import asyncio

    from messaging.schemas import Payload

    parser = argparse.ArgumentParser(description="Strategist agent CLI")
    parser.add_argument("--list-pending", action="store_true", help="List pending approvals")
    parser.add_argument("--approve", metavar="ID", help="Approve brief by approval_id")
    parser.add_argument("--veto", metavar="ID", help="Veto brief by approval_id")
    args = parser.parse_args()

    if args.list_pending:
        raise SystemExit(_cli_list_pending())
    if args.approve:
        raise SystemExit(_cli_resolve(args.approve, approved=True))
    if args.veto:
        raise SystemExit(_cli_resolve(args.veto, approved=False))

    async def _demo() -> None:
        print("\n--- [STRATEGIST DEMO] ---")
        agent = StrategistAgent()
        msg = AgentMessage(
            message_id=str(uuid.uuid4()),
            sender=AgentRole.ORCHESTRATOR,
            receiver=AgentRole.STRATEGIST,
            message_type=MessageType.REQUEST,
            payload=Payload(data={"input": _realistic_analyst_fixture()}),
        )
        resp = await agent.handle_request(msg)
        print(json.dumps(resp.payload.data, indent=2))

    asyncio.run(_demo())
