"""Browser locale / timezone from CRAWLER_GEO (default IN)."""
from __future__ import annotations

import os
from typing import Any, Dict

_GEO_PROFILES: Dict[str, Dict[str, str]] = {
    "IN": {"locale": "en-IN", "timezone_id": "Asia/Kolkata"},
    "US": {"locale": "en-US", "timezone_id": "America/New_York"},
    "UK": {"locale": "en-GB", "timezone_id": "Europe/London"},
    "AE": {"locale": "en-AE", "timezone_id": "Asia/Dubai"},
}


def browser_geo_context() -> Dict[str, Any]:
    """Playwright context kwargs for geo targeting."""
    code = os.getenv("CRAWLER_GEO", "IN").strip().upper() or "IN"
    profile = _GEO_PROFILES.get(code, _GEO_PROFILES["IN"])
    return {
        "locale": profile["locale"],
        "timezone_id": profile["timezone_id"],
        "geo": code,
    }
