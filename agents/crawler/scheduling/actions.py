"""Post-crawl actions — closed-loop responses to intelligence events."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional


class ActionType(str, Enum):
    CRAWL_ONLY = "crawl_only"
    FULL_SCRAPE = "full_scrape"
    MARKET_SWEEP = "market_sweep"
    SET_CRITICAL = "set_critical"
    SET_DORMANT = "set_dormant"
    RECRAWL_IN = "recrawl_in"
    ROTATE_PROXY = "rotate_proxy"
    RECRAWL_TRUSTED = "recrawl_trusted"


@dataclass(frozen=True)
class CrawlPlan:
    merchant_slug: str
    full_scrape: bool
    priority: int
    mode: str
    reason: str
    follow_links: bool = False


@dataclass
class CrawlAction:
    action: ActionType
    merchant_slug: str
    delay_sec: int = 0
    priority: int = 0
    reason: str = ""
    payload: Dict[str, Any] = field(default_factory=dict)

    def label(self) -> str:
        base = f"{self.action.value}@{self.merchant_slug}"
        if self.delay_sec:
            return f"{base} in {self.delay_sec}s"
        return base
