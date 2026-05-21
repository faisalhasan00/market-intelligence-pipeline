"""HTTP webhook receiver for crawler intelligence events."""
from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, Optional, TYPE_CHECKING

from aiohttp import web
from loguru import logger

if TYPE_CHECKING:
    from orchestrator.orchestrator import SwarmOrchestrator

logger.add("logs/swarm_timeline.json", format="{message}", level="INFO", serialize=True)


def _expected_secret() -> str:
    return os.getenv("ORCHESTRATOR_WEBHOOK_SECRET", "").strip()


def _check_secret(request: web.Request) -> bool:
    secret = _expected_secret()
    if not secret:
        return True
    header = request.headers.get("X-Orchestrator-Secret", "")
    if header == secret:
        return True
    auth = request.headers.get("Authorization", "")
    if auth == f"Bearer {secret}":
        return True
    return False


def normalize_webhook_envelope(body: Dict[str, Any]) -> Dict[str, Any]:
    """Map POST JSON to hot-pipeline envelope."""
    merchant = (
        body.get("merchant")
        or body.get("merchant_slug")
        or body.get("slug")
        or ""
    )
    event_type = body.get("type") or body.get("event") or "webhook_event"
    severity = str(body.get("severity", "critical")).lower()
    return {
        "type": event_type,
        "merchant": str(merchant).strip(),
        "severity": severity,
        "ts": body.get("ts") or time.time(),
        "source": body.get("source", "webhook"),
        "recommended_action": body.get("recommended_action"),
        "payload": body,
    }


async def enqueue_webhook_event(
    orchestrator: "SwarmOrchestrator",
    body: Dict[str, Any],
) -> Dict[str, Any]:
    envelope = normalize_webhook_envelope(body)
    if not envelope["merchant"]:
        raise ValueError("merchant or merchant_slug required")
    await orchestrator.enqueue_hot_event(envelope)
    log_entry = {
        "timestamp": time.time(),
        "event": "WEBHOOK_ENQUEUED",
        "message": f"Hot pipeline queued for {envelope['merchant']}",
        "metadata": {"type": envelope["type"], "source": envelope.get("source")},
    }
    logger.info(json.dumps(log_entry))
    return envelope


def create_webhook_app(orchestrator: "SwarmOrchestrator") -> web.Application:
    async def health(_request: web.Request) -> web.Response:
        return web.json_response({"status": "ok"})

    async def ingest(request: web.Request) -> web.Response:
        if not _check_secret(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        try:
            body = await request.json()
        except json.JSONDecodeError:
            return web.json_response({"error": "invalid json"}, status=400)
        if not isinstance(body, dict):
            return web.json_response({"error": "body must be a JSON object"}, status=400)
        try:
            envelope = await enqueue_webhook_event(orchestrator, body)
        except ValueError as exc:
            return web.json_response({"error": str(exc)}, status=400)
        return web.json_response({"queued": True, "envelope": envelope})

    app = web.Application()
    app.router.add_get("/health", health)
    app.router.add_post("/webhook", ingest)
    app.router.add_post("/events", ingest)
    return app


async def run_webhook_server(
    orchestrator: "SwarmOrchestrator",
    *,
    host: str = "0.0.0.0",
    port: Optional[int] = None,
) -> None:
    port = port or int(os.getenv("ORCHESTRATOR_WEBHOOK_PORT", "8081"))
    await orchestrator.start_event_integration()
    app = create_webhook_app(orchestrator)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    print(f"[Webhook] Listening on http://{host}:{port} (/webhook, /events, /health)")
    stop = __import__("asyncio").Event()
    await stop.wait()
