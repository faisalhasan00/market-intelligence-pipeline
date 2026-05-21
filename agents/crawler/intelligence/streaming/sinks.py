"""Pluggable intelligence stream sinks — webhook and JSONL file."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib import error, request


def format_stream_envelope(event: Dict[str, Any]) -> Dict[str, Any]:
    severity = "info"
    etype = event.get("type", "event")
    if etype in (
        "cashback_spike_detected",
        "rate_anomaly_detected",
        "market_sweep_initiated",
        "evidence_mismatch",
    ):
        severity = "critical"
    elif etype in ("visual_sale_detected", "campaign_started", "offer_started"):
        severity = "warning"

    action = None
    if etype == "cashback_spike_detected":
        action = "market_sweep"
    elif etype == "rate_anomaly_detected":
        action = "recrawl"
    elif etype == "evidence_mismatch":
        action = "revalidate"

    return {
        "type": etype,
        "merchant": event.get("merchant", ""),
        "source": event.get("source", ""),
        "severity": severity,
        "recommended_action": action,
        "payload": event,
        "ts": event.get("stream_ts") or datetime.now(timezone.utc).isoformat(),
    }


class WebhookSink:
    def __init__(self, url: str, *, timeout_sec: float = 8.0):
        self.url = url.strip()
        self.timeout_sec = timeout_sec

    async def __call__(self, event: Dict[str, Any]) -> None:
        envelope = format_stream_envelope(event)
        body = json.dumps(envelope, default=str).encode("utf-8")
        req = request.Request(
            self.url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            request.urlopen(req, timeout=self.timeout_sec)
        except error.URLError:
            pass


class FileSink:
    def __init__(self, path: str):
        self.path = path
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)

    async def __call__(self, event: Dict[str, Any]) -> None:
        envelope = format_stream_envelope(event)
        line = json.dumps(envelope, default=str) + "\n"
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(line)


def configure_stream_sinks(stream) -> None:
    """Register sinks from environment (idempotent per handler type)."""
    webhook = os.getenv("CRAWLER_EVENT_WEBHOOK", "").strip()
    log_path = os.getenv("CRAWLER_EVENT_LOG_PATH", "").strip()

    if webhook:
        sink = WebhookSink(webhook)
        if not any(isinstance(h, WebhookSink) for h in stream._handlers):
            stream.subscribe(sink)

    if log_path:
        sink = FileSink(log_path)
        if not any(isinstance(h, FileSink) for h in stream._handlers):
            stream.subscribe(sink)
