"""Real-time intelligence stream — emit events as they are detected."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional

StreamHandler = Callable[[Dict[str, Any]], Awaitable[None]]


class IntelligenceStream:
    def __init__(self) -> None:
        self._handlers: List[StreamHandler] = []
        self._buffer: List[Dict[str, Any]] = []
        self._max_buffer = 500

    def subscribe(self, handler: StreamHandler) -> None:
        self._handlers.append(handler)

    async def emit(self, event: Dict[str, Any]) -> None:
        payload = {
            **event,
            "stream_ts": datetime.now(timezone.utc).isoformat(),
        }
        self._buffer.append(payload)
        if len(self._buffer) > self._max_buffer:
            self._buffer = self._buffer[-self._max_buffer :]
        for handler in self._handlers:
            try:
                await handler(payload)
            except Exception:
                pass
        etype = payload.get("type", "event")
        merchant = payload.get("merchant", "")
        print(f"   [Stream] {etype} @ {merchant}")

    def recent(self, limit: int = 50) -> List[Dict[str, Any]]:
        return list(self._buffer[-limit:])

    def format_sse(self, event: Dict[str, Any]) -> str:
        return f"data: {json.dumps(event, default=str)}\n\n"


_default_stream: Optional[IntelligenceStream] = None


def get_intelligence_stream() -> IntelligenceStream:
    global _default_stream
    if _default_stream is None:
        _default_stream = IntelligenceStream()
        from agents.crawler.intelligence.streaming.sinks import configure_stream_sinks

        configure_stream_sinks(_default_stream)
    return _default_stream
