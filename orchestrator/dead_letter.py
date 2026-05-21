"""Append contract / validation failures to logs/dead_letter.jsonl."""
from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, Optional

DEAD_LETTER_PATH = os.getenv("ORCHESTRATOR_DEAD_LETTER_PATH", "logs/dead_letter.jsonl")


def _snippet(payload: Any, max_len: int = 500) -> Any:
    try:
        text = json.dumps(payload, default=str)
    except (TypeError, ValueError):
        text = repr(payload)
    if len(text) > max_len:
        return text[:max_len] + "…"
    return payload if isinstance(payload, (dict, list)) else text


def append_dead_letter(
    *,
    agent: str,
    error: str,
    payload: Optional[Any] = None,
    context: Optional[Dict[str, Any]] = None,
) -> None:
    os.makedirs(os.path.dirname(DEAD_LETTER_PATH) or ".", exist_ok=True)
    entry = {
        "timestamp": time.time(),
        "iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "agent": agent,
        "error": error,
        "payload_snippet": _snippet(payload),
        "context": context or {},
    }
    with open(DEAD_LETTER_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, default=str) + "\n")
