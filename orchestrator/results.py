from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class MerchantPipelineResult:
    merchant: str
    slug: str
    success: bool
    depth: str  # full | crawl_only | hot_intel | stream_hot
    cost_delta: float = 0.0
    events: List[Dict[str, Any]] = field(default_factory=list)
    alerts_sent: int = 0
    error: Optional[str] = None


@dataclass
class PipelineResult:
    query: str
    success: bool
    total_cost: float
    state_version: int
    merchants: List[MerchantPipelineResult] = field(default_factory=list)
    events_processed: int = 0
    alerts_sent: int = 0
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "query": self.query,
            "success": self.success,
            "total_cost": self.total_cost,
            "state_version": self.state_version,
            "events_processed": self.events_processed,
            "alerts_sent": self.alerts_sent,
            "error": self.error,
            "merchants": [
                {
                    "merchant": m.merchant,
                    "slug": m.slug,
                    "success": m.success,
                    "depth": m.depth,
                    "cost_delta": m.cost_delta,
                    "events": m.events,
                    "alerts_sent": m.alerts_sent,
                    "error": m.error,
                }
                for m in self.merchants
            ],
        }
