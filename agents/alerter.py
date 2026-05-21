"""
Alert Agent — severity routing, deduplication, and multi-channel delivery.

Channels: Slack incoming webhook, generic ALERT_WEBHOOK_URL, JSONL audit file.
Optional Ollama mistral formatting when ALERTER_USE_OLLAMA=true.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib import error, request

import aiohttp
from dotenv import load_dotenv

from agents.base_agent import BaseAgent
from messaging.schemas import AgentMessage, AgentRole, MessageType
from state.contracts import (
    AlerterOutputContract,
    map_risk_priority_to_severity,
    severity_meets_minimum,
    validate_alerter_output,
)

load_dotenv(override=True)

_SEVERITY_RANK = {"info": 0, "warning": 1, "critical": 2}
_CRITICAL_EVENT_TYPES = frozenset(
    {
        "cashback_spike_detected",
        "rate_anomaly_detected",
        "market_sweep_initiated",
        "evidence_mismatch",
    }
)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name, str(default)).strip().lower()
    return raw in ("1", "true", "yes", "on")


def _normalize_slug(value: Optional[str]) -> str:
    if not value:
        return "unknown"
    slug = re.sub(r"[^a-z0-9]+", "-", str(value).lower()).strip("-")
    return slug or "unknown"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_quiet_hours(spec: str) -> Optional[Tuple[int, int]]:
    """Parse '23-07' as start_hour, end_hour (local time)."""
    spec = (spec or "").strip()
    if not spec:
        return None
    m = re.match(r"^(\d{1,2})\s*-\s*(\d{1,2})$", spec)
    if not m:
        return None
    start, end = int(m.group(1)), int(m.group(2))
    if not (0 <= start <= 23 and 0 <= end <= 23):
        return None
    return start, end


def in_quiet_hours(now: Optional[datetime] = None) -> bool:
    spec = parse_quiet_hours(os.getenv("ALERTER_QUIET_HOURS", ""))
    if not spec:
        return False
    start, end = spec
    now = now or datetime.now()
    hour = now.hour
    if start <= end:
        return start <= hour < end
    return hour >= start or hour < end


class DedupeStore:
    """SQLite-backed dedupe keys with TTL."""

    def __init__(self, path: str, ttl_sec: int):
        self.path = path
        self.ttl_sec = max(1, ttl_sec)
        self._mem_conn: Optional[sqlite3.Connection] = None
        if path == ":memory:":
            self._mem_conn = sqlite3.connect(":memory:", timeout=5.0)
            self._ensure_table(self._mem_conn)
        else:
            parent = os.path.dirname(path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            with self._connect() as conn:
                self._ensure_table(conn)
                conn.commit()

    def _connect(self) -> sqlite3.Connection:
        if self._mem_conn is not None:
            return self._mem_conn
        return sqlite3.connect(self.path, timeout=5.0)

    def _ensure_table(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alert_dedupe (
                dedupe_key TEXT PRIMARY KEY,
                sent_at REAL NOT NULL
            )
            """
        )

    def _prune(self, conn: sqlite3.Connection) -> None:
        cutoff = datetime.now(timezone.utc).timestamp() - self.ttl_sec
        conn.execute("DELETE FROM alert_dedupe WHERE sent_at < ?", (cutoff,))

    def seen_recently(self, dedupe_key: str) -> bool:
        now = datetime.now(timezone.utc).timestamp()
        conn = self._connect()
        close_after = self._mem_conn is None
        try:
            self._ensure_table(conn)
            self._prune(conn)
            row = conn.execute(
                "SELECT sent_at FROM alert_dedupe WHERE dedupe_key = ?",
                (dedupe_key,),
            ).fetchone()
            if not row:
                return False
            return (now - float(row[0])) < self.ttl_sec
        finally:
            if close_after:
                conn.close()

    def record(self, dedupe_key: str) -> None:
        now = datetime.now(timezone.utc).timestamp()
        conn = self._connect()
        close_after = self._mem_conn is None
        try:
            self._ensure_table(conn)
            conn.execute(
                """
                INSERT INTO alert_dedupe (dedupe_key, sent_at) VALUES (?, ?)
                ON CONFLICT(dedupe_key) DO UPDATE SET sent_at = excluded.sent_at
                """,
                (dedupe_key, now),
            )
            conn.commit()
        finally:
            if close_after:
                conn.close()


def build_dedupe_key(strategy: Dict[str, Any], severity: str) -> str:
    slug = _normalize_slug(
        strategy.get("merchant_slug")
        or strategy.get("merchant")
        or "unknown"
    )
    rec = (strategy.get("recommendation") or "")[:80]
    digest = hashlib.sha256(f"{rec}|{severity}".encode()).hexdigest()[:12]
    return f"{slug}:{severity}:{digest}"


def build_slack_message(strategy: Dict[str, Any]) -> str:
    """Prefer strategist notification_preview; enrich for Slack with a comparison table."""
    merchant = strategy.get("merchant") or strategy.get("merchant_slug") or "Merchant"
    priority = str(strategy.get("priority", "LOW")).upper()
    urgency = strategy.get("urgency_hours", 72)
    leverage = strategy.get("negotiation_leverage") or "medium"
    
    client_rate = strategy.get("client_rate") or "5%"
    competitor_rate = strategy.get("competitor_rate") or "Unknown"
    comp_adv = strategy.get("competitor_advantage_pct")
    gap = f"+{comp_adv}%" if comp_adv is not None else "Unknown"

    emoji = {"HIGH": ":red_circle:", "MEDIUM": ":large_orange_circle:", "LOW": ":large_green_circle:"}.get(
        priority, ":white_circle:"
    )

    preview = (strategy.get("notification_preview") or "").strip()
    lines: List[str] = []
    if preview:
        lines.append(preview)
    else:
        lines.append(f"{emoji} *{merchant}* | Risk {priority} → Priority {priority} | Act within {urgency}h | Leverage: {leverage}")
    
    # 1. Rate Comparison Table
    lines.append("\n*Rate Comparison Table*")
    lines.append("```")
    lines.append(f"| Merchant   | GrabOn Offer | Competitor Offer | Advantage |")
    lines.append(f"|------------|--------------|------------------|-----------|")
    lines.append(f"| {merchant:<10} | {client_rate:<12} | {competitor_rate:<16} | {gap:<9} |")
    lines.append("```")

    # 2. Evidence / References
    evidence_list = strategy.get("evidence_refs") or []
    if evidence_list:
        lines.append("\n*Evidence / Sources*")
        for item in evidence_list[:3]:
            lines.append(f"• {item}")
    else:
        events = strategy.get("based_on_events") or []
        if events:
            lines.append("\n*Evidence / Events*")
            for item in events[:3]:
                lines.append(f"• Event: {item}")

    # 3. Why / Reasoning / Executive Summary
    bullets = strategy.get("executive_summary") or []
    lines.append("\n*Executive summary*")
    if bullets:
        for b in bullets[:5]:
            lines.append(f"• {b}")
    else:
        reason = strategy.get("reasoning") or strategy.get("negotiation_brief") or "No detailed explanation provided."
        lines.append(f"• {reason}")

    # 4. Action details
    counter = strategy.get("counter_offer_suggestion")
    if counter:
        lines.append(f"\n*Counter-Offer Suggestion*")
        lines.append(f"_Suggestion:_ {counter}")

    return "\n".join(lines).strip()


def format_alert_envelope(
    strategy: Dict[str, Any],
    *,
    severity: str,
    alert_content: str,
    dedupe_key: str,
    analyst_risk: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "type": "swarm_alert",
        "severity": severity,
        "merchant": strategy.get("merchant") or strategy.get("merchant_slug") or "",
        "merchant_slug": _normalize_slug(
            strategy.get("merchant_slug") or strategy.get("merchant")
        ),
        "priority": strategy.get("priority"),
        "analyst_risk_level": analyst_risk or strategy.get("analyst_risk_level"),
        "urgency_hours": strategy.get("urgency_hours"),
        "recommendation": strategy.get("recommendation"),
        "executive_summary": strategy.get("executive_summary") or [],
        "alert_content": alert_content,
        "dedupe_key": dedupe_key,
        "ts": _utc_now_iso(),
        "payload": strategy,
    }


def channels_for_severity(
    severity: str,
    *,
    slack_enabled: bool,
    webhook_enabled: bool,
) -> List[str]:
    """critical → all enabled; warning → enabled if min met; info → file only."""
    channels: List[str] = ["file"]
    if severity in ("critical", "warning"):
        if slack_enabled:
            channels.append("slack")
        if webhook_enabled:
            channels.append("webhook")
    return channels


def _post_json(url: str, body: Dict[str, Any], *, timeout_sec: float = 8.0) -> bool:
    data = json.dumps(body, default=str).encode("utf-8")
    req = request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=timeout_sec) as resp:
            return 200 <= resp.status < 300
    except error.URLError:
        return False


def deliver_slack(text: str, webhook_url: str) -> bool:
    return _post_json(webhook_url.strip(), {"text": text})


def deliver_webhook(envelope: Dict[str, Any], webhook_url: str) -> bool:
    return _post_json(webhook_url.strip(), envelope)


def append_file_audit(envelope: Dict[str, Any], path: str) -> bool:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    line = json.dumps(envelope, default=str) + "\n"
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)
        return True
    except OSError:
        return False


class AlertDeliveryService:
    """Channel routing, dedupe, quiet hours — testable without AlertAgent."""

    def __init__(
        self,
        *,
        dedupe_store: Optional[DedupeStore] = None,
        slack_url: Optional[str] = None,
        webhook_url: Optional[str] = None,
        audit_path: Optional[str] = None,
    ):
        self.slack_enabled = _env_bool("ALERTER_SLACK_ENABLED")
        self.webhook_enabled = _env_bool("ALERTER_WEBHOOK_ENABLED")
        self.slack_url = (slack_url or os.getenv("SLACK_WEBHOOK_URL", "")).strip()
        self.webhook_url = (webhook_url or os.getenv("ALERT_WEBHOOK_URL", "")).strip()
        self.audit_path = audit_path or os.getenv(
            "ALERTER_AUDIT_PATH", "data/alerts.jsonl"
        )
        self.min_severity = os.getenv("ALERTER_MIN_SEVERITY", "warning").lower().strip()
        ttl = int(os.getenv("ALERTER_DEDUPE_TTL_SEC", "3600"))
        dedupe_path = os.getenv("ALERTER_DEDUPE_PATH", "data/alerter_dedupe.db")
        self.dedupe = dedupe_store or DedupeStore(dedupe_path, ttl)

        if self.slack_enabled and not self.slack_url:
            self.slack_enabled = False
        if self.webhook_enabled and not self.webhook_url:
            self.webhook_enabled = False

    def resolve_severity(self, strategy: Dict[str, Any]) -> str:
        risk = strategy.get("analyst_risk_level") or strategy.get("threat_level") or "LOW"
        priority = strategy.get("priority") or "LOW"
        return map_risk_priority_to_severity(risk, priority)

    async def send_alert(
        self,
        strategy: Dict[str, Any],
        *,
        alert_content: Optional[str] = None,
        force: bool = False,
    ) -> Dict[str, Any]:
        severity = self.resolve_severity(strategy)
        if not severity_meets_minimum(severity, self.min_severity):
            return self._skipped_output(
                strategy,
                severity,
                reason="below_min_severity",
                alert_content=alert_content or build_slack_message(strategy),
            )

        if in_quiet_hours() and severity != "critical" and not force:
            return self._skipped_output(
                strategy,
                severity,
                reason="quiet_hours",
                alert_content=alert_content or build_slack_message(strategy),
            )

        content = alert_content or build_slack_message(strategy)
        dedupe_key = build_dedupe_key(strategy, severity)
        if self.dedupe.seen_recently(dedupe_key) and not force:
            return self._skipped_output(
                strategy,
                severity,
                reason="dedupe",
                dedupe_key=dedupe_key,
                alert_content=content,
            )

        channels = channels_for_severity(
            severity,
            slack_enabled=self.slack_enabled,
            webhook_enabled=self.webhook_enabled,
        )
        envelope = format_alert_envelope(
            strategy,
            severity=severity,
            alert_content=content,
            dedupe_key=dedupe_key,
            analyst_risk=strategy.get("analyst_risk_level"),
        )

        attempted: List[str] = []
        ok_channels: List[str] = []
        for ch in channels:
            attempted.append(ch)
            if ch == "file":
                if append_file_audit(envelope, self.audit_path):
                    ok_channels.append("file")
            elif ch == "slack":
                if deliver_slack(content, self.slack_url):
                    ok_channels.append("slack")
                    print("   [Alerter] Alert sent on Slack")
            elif ch == "webhook":
                if deliver_webhook(envelope, self.webhook_url):
                    ok_channels.append("webhook")

        self.dedupe.record(dedupe_key)

        primary = ok_channels[0] if ok_channels else (attempted[0] if attempted else "none")
        if ok_channels:
            status = "sent" if len(ok_channels) == len(attempted) else "partial"
        else:
            status = "failed"

        out = {
            "alert_content": content,
            "channel": primary,
            "severity": severity,
            "merchant_slug": _normalize_slug(
                strategy.get("merchant_slug") or strategy.get("merchant")
            ),
            "dedupe_key": dedupe_key,
            "sent_at": _utc_now_iso(),
            "delivery_status": status,
            "channels_attempted": attempted,
            "channels_ok": ok_channels,
        }
        validate_alerter_output(out)
        return out

    def _skipped_output(
        self,
        strategy: Dict[str, Any],
        severity: str,
        *,
        reason: str,
        alert_content: str,
        dedupe_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        key = dedupe_key or build_dedupe_key(strategy, severity)
        out = {
            "alert_content": alert_content,
            "channel": "skipped",
            "severity": severity,
            "merchant_slug": _normalize_slug(
                strategy.get("merchant_slug") or strategy.get("merchant")
            ),
            "dedupe_key": key,
            "sent_at": _utc_now_iso(),
            "delivery_status": f"skipped:{reason}",
            "channels_attempted": [],
        }
        validate_alerter_output(out)
        return out


class AlertAgent(BaseAgent):
    """
    Formats strategist notifications and delivers via Slack / webhook / JSONL audit.
  Optional local Ollama (mistral) when ALERTER_USE_OLLAMA=true.
    """

    def __init__(self, model: str = "mistral"):
        super().__init__(role=AgentRole.ALERTER, model=model, provider="ollama")
        self.ollama_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
        self.use_ollama = _env_bool("ALERTER_USE_OLLAMA")
        self.delivery = AlertDeliveryService()

    async def handle_request(self, message: AgentMessage) -> AgentMessage:
        strategy_data = dict(message.payload.data.get("input", {}) or {})
        print(f"\n   [Alerter] Preparing notification...")

        content = build_slack_message(strategy_data)
        cost = 0.0

        if self.use_ollama:
            formatted, cost = await self._format_with_local_llm(strategy_data)
            if formatted and "Error" not in formatted:
                content = formatted

        result = await self.delivery.send_alert(strategy_data, alert_content=content)
        print(
            f"   [Alerter] severity={result.get('severity')} "
            f"status={result.get('delivery_status')} "
            f"channels={result.get('channels_attempted')}"
        )
        return self.create_response(
            message,
            result,
            message_type=MessageType.APPROVAL,
            cost=cost,
        )

    async def _format_with_local_llm(self, data: Dict[str, Any]) -> Tuple[str, float]:
        preview = data.get("notification_preview") or json.dumps(data)[:500]
        payload = {
            "model": "mistral",
            "prompt": (
                f"Polish this Slack alert (keep under 400 chars, keep emojis):\n{preview}"
            ),
            "stream": False,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.ollama_url}/api/generate", json=payload
                ) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        text = (result.get("response") or "").strip()
                        if text:
                            return text, 0.0
        except Exception as exc:
            print(f"   [Alerter] Ollama unavailable: {exc}")
        return build_slack_message(data), 0.0


async def send_stream_event_alert(
    event: Dict[str, Any],
    *,
    delivery: Optional[AlertDeliveryService] = None,
) -> Optional[Dict[str, Any]]:
    """Alert on critical crawler stream events (optional hook)."""
    etype = event.get("type", "")
    if etype not in _CRITICAL_EVENT_TYPES:
        return None
    strategy = {
        "merchant": event.get("merchant", ""),
        "merchant_slug": _normalize_slug(event.get("merchant")),
        "priority": "HIGH",
        "threat_level": "HIGH",
        "analyst_risk_level": "HIGH",
        "recommendation": f"Crawler event: {etype} — review immediately.",
        "urgency_hours": 4,
        "executive_summary": [f"Event {etype} on {event.get('merchant', 'unknown')}"],
        "notification_preview": (
            f":rotating_light: *{event.get('merchant', 'Merchant')}* — `{etype}` detected"
        ),
    }
    svc = delivery or AlertDeliveryService()
    return await svc.send_alert(strategy)


def register_alerter_on_stream(stream) -> None:
    """
    Subscribe alerter to intelligence stream for critical crawler events.

    Primary production path remains orchestrator → alerter after strategist alignment.
    Stream hook covers real-time spikes without waiting for full pipeline.
    """
    svc = AlertDeliveryService()

    async def _handler(event: Dict[str, Any]) -> None:
        await send_stream_event_alert(event, delivery=svc)

    if not any(getattr(h, "__name__", "") == "_alerter_stream_handler" for h in stream._handlers):
        _handler.__name__ = "_alerter_stream_handler"
        stream.subscribe(_handler)


def _fixture_strategist_payload() -> Dict[str, Any]:
    return {
        "recommendation": "Increase GrabOn rate by 2% to beat Myntra competitive spike.",
        "priority": "HIGH",
        "negotiation_brief": "Competitive threat detected at 12% rate vs client 5%.",
        "threat_level": "HIGH",
        "merchant": "Myntra",
        "merchant_slug": "myntra",
        "analyst_risk_level": "HIGH",
        "urgency_hours": 4,
        "executive_summary": [
            "Myntra: HIGH competitive threat.",
            "Gap: competitor 12% vs GrabOn client 5%.",
            "Trend escalating; act within 4h.",
        ],
        "notification_preview": (
            ":red_circle: *Myntra* | Risk HIGH → Priority HIGH | Act within 4h\n"
            "> Increase GrabOn rate by 2% to beat Myntra competitive spike."
        ),
        "negotiation_leverage": "high",
        "counter_offer_suggestion": "Authorize up to +7.0 pt emergency match on flagged SKUs.",
        "aligned_with_analyst": True,
    }


if __name__ == "__main__":
    import asyncio

    from messaging.schemas import Payload

    async def _demo() -> None:
        print("\n--- [ALERTER DEMO] ---")
        os.environ.setdefault("ALERTER_SLACK_ENABLED", "false")
        os.environ.setdefault("ALERTER_WEBHOOK_ENABLED", "false")
        agent = AlertAgent()
        msg = AgentMessage(
            message_id=str(uuid.uuid4()),
            sender=AgentRole.ORCHESTRATOR,
            receiver=AgentRole.ALERTER,
            message_type=MessageType.REQUEST,
            payload=Payload(data={"input": _fixture_strategist_payload()}),
        )
        resp = await agent.handle_request(msg)
        print(json.dumps(resp.payload.data, indent=2))
        audit = os.getenv("ALERTER_AUDIT_PATH", "data/alerts.jsonl")
        if os.path.isfile(audit):
            print(f"\nAudit tail ({audit}):")
            with open(audit, encoding="utf-8") as f:
                lines = f.readlines()
            for line in lines[-2:]:
                print(line.rstrip())

    asyncio.run(_demo())
