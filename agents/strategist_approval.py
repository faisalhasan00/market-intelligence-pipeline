"""
File-based approval queue for strategist negotiation briefs (MVP, no UI).
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

def _approval_dir() -> Path:
    return Path(os.getenv("STRATEGIST_APPROVAL_DIR", "data/approvals"))


def approval_queue_enabled() -> bool:
    return os.getenv("STRATEGIST_APPROVAL_QUEUE", "false").lower() in ("1", "true", "yes")


def _safe_merchant(merchant: str) -> str:
    return re.sub(r"[^\w\-]+", "_", (merchant or "merchant").strip().lower())[:64]


def submit_for_approval(strategy: Dict[str, Any], *, merchant: str) -> Dict[str, Any]:
    """Write brief to data/approvals/{merchant}_{timestamp}.json with status pending."""
    approval_dir = _approval_dir()
    approval_dir.mkdir(parents=True, exist_ok=True)
    approval_id = str(uuid4())[:8]
    submitted_at = datetime.now(timezone.utc).isoformat()
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe = _safe_merchant(merchant)
    path = approval_dir / f"{safe}_{ts}.json"

    record = {
        **strategy,
        "approval_id": approval_id,
        "approval_status": "pending",
        "submitted_at": submitted_at,
        "merchant": merchant,
    }
    path.write_text(json.dumps(record, indent=2), encoding="utf-8")
    record["_approval_path"] = str(path)
    return record


def list_pending() -> List[Dict[str, Any]]:
    approval_dir = _approval_dir()
    if not approval_dir.is_dir():
        return []
    pending: List[Dict[str, Any]] = []
    for path in sorted(approval_dir.glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if str(data.get("approval_status", "")).lower() == "pending":
            data["_approval_path"] = str(path)
            pending.append(data)
    return pending


def _find_by_id(approval_id: str) -> Optional[Path]:
    approval_dir = _approval_dir()
    if not approval_dir.is_dir():
        return None
    needle = approval_id.strip().lower()
    for path in approval_dir.glob("*.json"):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if str(data.get("approval_id", "")).lower() == needle:
            return path
    return None


def resolve_approval(approval_id: str, *, approved: bool) -> Dict[str, Any]:
    path = _find_by_id(approval_id)
    if path is None:
        raise FileNotFoundError(f"No approval record for id={approval_id}")
    data = json.loads(path.read_text(encoding="utf-8"))
    data["approval_status"] = "approved" if approved else "vetoed"
    data["resolved_at"] = datetime.now(timezone.utc).isoformat()
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    data["_approval_path"] = str(path)
    return data
