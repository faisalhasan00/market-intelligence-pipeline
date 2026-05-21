from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

# --- ONTOLOGY ENUMS ---
class OfferType(str, Enum):
    CASHBACK = "cashback"
    COUPON = "coupon"
    BANK_OFFER = "bank_offer"
    SALE = "sale"
    UNKNOWN = "unknown"

class PlacementType(str, Enum):
    BANNER = "banner"
    CONTENT = "content"
    POPUP = "popup"
    UNKNOWN = "unknown"

# --- DATA GOVERNANCE CONTRACTS ---
class OfferContract(BaseModel):
    """Ontological definition of a validated offer."""
    offer_type: OfferType = OfferType.UNKNOWN
    description: str = Field(..., min_length=5)
    code: Optional[str] = None
    expiry: Optional[str] = None
    placement: PlacementType = PlacementType.UNKNOWN
    
    @field_validator("description")
    def validate_description(cls, v: str) -> str:
        if len(v) > 500:
            raise ValueError("Data Contract Violation: Description exceeds 500 chars (possible hallucination or raw HTML leakage).")
        return v

class CrawlerPayloadContract(BaseModel):
    """Strict data contract for incoming Crawler JSON payloads."""
    target: str
    url_visited: str
    status_code: int
    timestamp: Optional[int] = None
    screenshot: Optional[str] = None
    raw_text: str = Field(..., description="Raw extracted text")
    
    @field_validator("status_code")
    def validate_status(cls, v: int) -> int:
        if v not in [200, 201, 301, 302]:
            raise ValueError(f"Data Contract Violation: Invalid or blocked Status Code {v}.")
        return v


class SourceIntelligenceContract(BaseModel):
    """Per-competitor source record from live crawler."""
    model_config = ConfigDict(extra="ignore")

    source_name: str
    target_type: str = "competitor"
    url: Optional[str] = None
    merchant: Optional[str] = None
    cashback_rate: Optional[str] = None
    offers: List[str] = Field(default_factory=list)
    blocked: bool = False
    status_code: int = 200
    reliability_score: float = Field(default=0.5, ge=0.0, le=1.0)
    extraction: Optional[Dict[str, Any]] = None
    evidence: Optional[Dict[str, Any]] = None
    visual_intelligence: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class IntelligencePayloadContract(BaseModel):
    """Stable contract for CrawlerAgent.collect_intelligence() output."""
    model_config = ConfigDict(extra="ignore")

    merchant: str
    merchant_slug: str
    collected_at: str
    aggregate_confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    offers: List[str] = Field(default_factory=list)
    sources: List[SourceIntelligenceContract] = Field(default_factory=list)
    events: List[Dict[str, Any]] = Field(default_factory=list)
    cashback_rate: Optional[str] = None
    client_rate: Optional[str] = None
    data_source: str = "live_web"
    summary: str = ""
    query: Optional[str] = None
    consensus: Optional[Dict[str, Any]] = None
    anomaly: Optional[Dict[str, Any]] = None
    monitoring: Optional[Dict[str, Any]] = None
    budget: Optional[Dict[str, Any]] = None
    evidence_validation: Optional[Dict[str, Any]] = None


def sanitize_crawler_payload(data: Dict[str, Any]) -> Dict[str, Any]:
    """Strip internal/runtime fields before contract validation."""
    skip = {"_cost_usd", "phase", "results", "intelligence_stream"}
    return {k: v for k, v in data.items() if k not in skip}


def validate_intelligence_output(data: Dict[str, Any]) -> IntelligencePayloadContract:
    return IntelligencePayloadContract.model_validate(sanitize_crawler_payload(data))


class AnalystOutputContract(BaseModel):
    """Structured analyst intelligence for strategist / orchestrator."""
    model_config = ConfigDict(extra="ignore")

    risk_level: str
    gap_summary: str = Field(..., min_length=1)
    competitor_advantage_pct: Optional[float] = Field(default=None, ge=0.0, le=100.0)
    recommended_action: str = Field(..., min_length=1)
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    evidence_refs: List[str] = Field(default_factory=list)
    trend: Optional[str] = None
    gap_found: Optional[bool] = None
    competitor_rate: Optional[str] = None
    client_rate: Optional[str] = None
    reasoning: Optional[str] = None
    response_probability: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    predicted_competitor_move: Optional[str] = None

    @field_validator("risk_level")
    @classmethod
    def normalize_risk(cls, v: str) -> str:
        level = str(v).upper().strip()
        if level not in {"HIGH", "MEDIUM", "LOW", "UNKNOWN"}:
            raise ValueError(f"Invalid risk_level: {v}")
        return level

    @field_validator("trend")
    @classmethod
    def normalize_trend(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        t = str(v).lower().strip()
        if t not in {"escalating", "stable", "declining"}:
            raise ValueError(f"Invalid trend: {v}")
        return t


def coerce_analyst_payload(raw: Dict[str, Any], crawler_data: Dict[str, Any]) -> Dict[str, Any]:
    """Map legacy LLM keys into AnalystOutputContract shape."""
    data = dict(raw)
    if not data.get("gap_summary"):
        data["gap_summary"] = (
            data.get("reasoning")
            or data.get("gap_summary")
            or "No gap summary from model."
        )
    if not data.get("recommended_action"):
        risk = str(data.get("risk_level", "LOW")).upper()
        data["recommended_action"] = {
            "HIGH": "Escalate merchant negotiation and match competitive cashback immediately.",
            "MEDIUM": "Monitor daily and prepare a counter-offer within 48 hours.",
            "LOW": "Maintain current positioning; routine monitoring only.",
        }.get(risk, "Continue monitoring.")
    if data.get("confidence") is None:
        data["confidence"] = crawler_data.get("aggregate_confidence", 0.5)
    if not data.get("client_rate"):
        data["client_rate"] = crawler_data.get("client_rate")
    if not data.get("competitor_rate"):
        data["competitor_rate"] = crawler_data.get("cashback_rate")
    if data.get("gap_found") is None:
        adv = data.get("competitor_advantage_pct")
        data["gap_found"] = bool(adv and adv > 0)
    if not data.get("reasoning"):
        data["reasoning"] = data["gap_summary"]
    return data


def validate_analyst_output(
    raw: Dict[str, Any],
    crawler_data: Optional[Dict[str, Any]] = None,
) -> AnalystOutputContract:
    merged = coerce_analyst_payload(raw, crawler_data or {})
    return AnalystOutputContract.model_validate(merged)


_PRIORITY_LEVELS = frozenset({"HIGH", "MEDIUM", "LOW", "UNKNOWN"})
_RISK_TO_PRIORITY = {"HIGH": "HIGH", "MEDIUM": "MEDIUM", "LOW": "LOW", "UNKNOWN": "UNKNOWN"}
_RISK_TO_URGENCY_HOURS = {"HIGH": 4, "MEDIUM": 24, "LOW": 72, "UNKNOWN": 72}


LeverageLevel = Literal["low", "medium", "high"]


def compute_negotiation_leverage(analysis: Dict[str, Any]) -> LeverageLevel:
    """Rules-based leverage from analyst risk and competitor advantage."""
    risk = str(analysis.get("risk_level", "LOW")).upper()
    adv = analysis.get("competitor_advantage_pct")
    trend = str(analysis.get("trend") or "stable").lower()
    try:
        gap = float(adv) if adv is not None else 0.0
    except (TypeError, ValueError):
        gap = 0.0

    if risk == "HIGH" and gap >= 5:
        return "high"
    if risk == "LOW" and gap < 3 and trend != "escalating":
        return "low"
    if risk in ("HIGH", "MEDIUM") or gap >= 3 or trend == "escalating":
        return "medium"
    return "low"


def compute_counter_offer_suggestion(analysis: Dict[str, Any]) -> str:
    """Suggest counter-offer from competitive gap percentage."""
    adv = analysis.get("competitor_advantage_pct")
    client = analysis.get("client_rate") or "current rate"
    try:
        gap = float(adv) if adv is not None else 0.0
    except (TypeError, ValueError):
        gap = 0.0

    if gap <= 0:
        return f"Hold {client}; no validated gap — request 30-day performance review."
    if gap < 3:
        return (
            f"Propose +{gap:.1f} pt parity bump on top SKUs only; cap burn at 2 weeks."
        )
    if gap < 7:
        return (
            f"Counter with +{gap:.1f} pt match on hero categories; "
            f"bundle bank-offer messaging vs full-platform match."
        )
    return (
        f"Authorize up to +{min(gap, 10):.1f} pt emergency match on flagged SKUs; "
        f"tie to 14-day volume commit from merchant."
    )


def build_notification_preview(strategy: Dict[str, Any], analysis: Dict[str, Any]) -> str:
    """Short Slack-style text — no LLM required for alerter."""
    merchant = analysis.get("merchant") or analysis.get("merchant_slug") or "Merchant"
    risk = str(analysis.get("risk_level", "LOW")).upper()
    priority = str(strategy.get("priority", risk)).upper()
    urgency = strategy.get("urgency_hours", 72)
    rec = (strategy.get("recommendation") or "")[:120]
    leverage = strategy.get("negotiation_leverage", "medium")
    emoji = {"HIGH": ":red_circle:", "MEDIUM": ":large_orange_circle:", "LOW": ":large_green_circle:"}.get(
        priority, ":white_circle:"
    )
    return (
        f"{emoji} *{merchant}* | Risk {risk} → Priority {priority} | "
        f"Act within {urgency}h | Leverage: {leverage}\n"
        f"> {rec}"
    )


class StrategistOutputContract(BaseModel):
    """Structured strategist output for orchestrator / alerter."""
    model_config = ConfigDict(extra="ignore")

    recommendation: str = Field(..., min_length=1)
    priority: str
    negotiation_brief: str = Field(..., min_length=1)
    threat_level: str = Field(..., min_length=1)
    merchant_actions: List[str] = Field(default_factory=list)
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    based_on_events: List[str] = Field(default_factory=list)
    aligned_with_analyst: bool = False
    executive_summary: List[str] = Field(default_factory=list)
    talking_points: List[str] = Field(default_factory=list)
    urgency_hours: int = Field(default=72, ge=1, le=168)
    notification_preview: str = Field(default="", min_length=0)
    negotiation_leverage: LeverageLevel = "medium"
    counter_offer_suggestion: str = Field(default="", min_length=0)
    approval_status: Optional[str] = None
    approval_id: Optional[str] = None
    submitted_at: Optional[str] = None

    @field_validator("priority")
    @classmethod
    def normalize_priority(cls, v: str) -> str:
        level = str(v).upper().strip()
        if level not in _PRIORITY_LEVELS:
            raise ValueError(f"Invalid priority: {v}")
        return level

    @field_validator("negotiation_leverage")
    @classmethod
    def normalize_leverage(cls, v: str) -> str:
        level = str(v).lower().strip()
        if level not in {"low", "medium", "high"}:
            raise ValueError(f"Invalid negotiation_leverage: {v}")
        return level

    @field_validator("executive_summary")
    @classmethod
    def normalize_executive_summary(cls, v: List[str]) -> List[str]:
        bullets = [str(b).strip() for b in (v or []) if str(b).strip()]
        return bullets[:5]


def coerce_strategist_payload(
    raw: Dict[str, Any],
    analysis: Dict[str, Any],
) -> Dict[str, Any]:
    """Map legacy LLM keys into StrategistOutputContract shape."""
    data = dict(raw)
    risk = str(analysis.get("risk_level", "LOW")).upper()
    default_priority = _RISK_TO_PRIORITY.get(risk, "LOW")

    if not data.get("recommendation"):
        data["recommendation"] = analysis.get("recommended_action") or (
            f"Respond to {risk} competitive risk for merchant partnership."
        )
    if not data.get("priority"):
        data["priority"] = default_priority
    if not data.get("negotiation_brief"):
        data["negotiation_brief"] = (
            data.get("recommendation")
            or analysis.get("gap_summary")
            or "No negotiation brief generated."
        )
    if not data.get("threat_level"):
        data["threat_level"] = risk
    if data.get("confidence") is None:
        data["confidence"] = float(analysis.get("confidence") or 0.5)
    if not data.get("merchant_actions"):
        data["merchant_actions"] = []
    if not data.get("based_on_events"):
        events = analysis.get("intelligence_events") or []
        data["based_on_events"] = [
            str(e.get("type"))
            for e in events
            if isinstance(e, dict) and e.get("type")
        ][:12]
    if not data.get("executive_summary"):
        gap = analysis.get("gap_summary") or analysis.get("reasoning") or ""
        data["executive_summary"] = [
            f"Risk tier: {risk}.",
            gap[:200] if gap else "No gap summary from analyst.",
            f"Recommended analyst action: {analysis.get('recommended_action', 'Monitor.')[:120]}",
        ]
    if not data.get("talking_points"):
        data["talking_points"] = [
            f"Competitor rate {analysis.get('competitor_rate') or 'unknown'} vs "
            f"client {analysis.get('client_rate') or 'unknown'}.",
        ]
    if data.get("urgency_hours") is None:
        data["urgency_hours"] = _RISK_TO_URGENCY_HOURS.get(risk, 72)
    if not data.get("merchant_slug"):
        data["merchant_slug"] = analysis.get("merchant_slug") or analysis.get("merchant")
    if not data.get("merchant"):
        data["merchant"] = analysis.get("merchant") or data.get("merchant_slug")
    if not data.get("negotiation_leverage"):
        data["negotiation_leverage"] = compute_negotiation_leverage(analysis)
    if not data.get("counter_offer_suggestion"):
        data["counter_offer_suggestion"] = compute_counter_offer_suggestion(analysis)
    if not data.get("notification_preview"):
        data["notification_preview"] = build_notification_preview(data, analysis)
    priority = str(data.get("priority", default_priority)).upper()
    data["aligned_with_analyst"] = priority == risk
    return data


def validate_strategist_output(
    raw: Dict[str, Any],
    analysis: Dict[str, Any],
) -> StrategistOutputContract:
    merged = coerce_strategist_payload(raw, analysis)
    return StrategistOutputContract.model_validate(merged)


_SEVERITY_LEVELS = frozenset({"critical", "warning", "info"})
_SEVERITY_ORDER = {"info": 0, "warning": 1, "critical": 2}


class AlerterOutputContract(BaseModel):
    """Structured alert delivery record for orchestrator / audit."""
    model_config = ConfigDict(extra="ignore")

    alert_content: str = Field(..., min_length=1)
    channel: str = Field(..., min_length=1)
    severity: str
    merchant_slug: str = Field(..., min_length=1)
    dedupe_key: str = Field(..., min_length=1)
    sent_at: str
    delivery_status: str = Field(..., min_length=1)
    channels_attempted: List[str] = Field(default_factory=list)

    @field_validator("severity")
    @classmethod
    def normalize_severity(cls, v: str) -> str:
        level = str(v).lower().strip()
        if level not in _SEVERITY_LEVELS:
            raise ValueError(f"Invalid severity: {v}")
        return level


def map_risk_priority_to_severity(risk_level: str, priority: str) -> str:
    """Map analyst risk + strategist priority to alert severity."""
    risk = str(risk_level or "LOW").upper()
    pri = str(priority or "LOW").upper()
    if risk == "HIGH" and pri == "HIGH":
        return "critical"
    if risk == "HIGH" or pri == "HIGH":
        return "critical"
    if risk == "MEDIUM" or pri == "MEDIUM":
        return "warning"
    return "info"


def severity_meets_minimum(severity: str, minimum: str) -> bool:
    return _SEVERITY_ORDER.get(severity, 0) >= _SEVERITY_ORDER.get(minimum, 0)


def validate_alerter_output(data: Dict[str, Any]) -> AlerterOutputContract:
    return AlerterOutputContract.model_validate(data)


def validate_competitor_entry(target: str, payload: Any) -> None:
    """Validate one competitor_data entry (legacy or intelligence shape)."""
    if not isinstance(payload, dict):
        return
    if "merchant_slug" in payload:
        validate_intelligence_output(payload)
        return
    if "url_visited" in payload:
        CrawlerPayloadContract(**payload)
        return
