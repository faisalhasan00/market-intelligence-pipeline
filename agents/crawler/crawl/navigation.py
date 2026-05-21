"""Discover internal campaign / sale URLs from competitor store pages."""
from __future__ import annotations

import os
import re
from typing import List
from urllib.parse import urljoin, urlparse

LINK_RE = re.compile(r"""href=["']([^"']+)["']""", re.IGNORECASE)

CAMPAIGN_HINTS = (
    "sale",
    "offer",
    "campaign",
    "promo",
    "deal",
    "fest",
    "cashback",
    "coupon",
    "clearance",
    "billion",
    "diwali",
    "black-friday",
    "black_friday",
    "end-of-season",
    "flash",
    "mega",
)

SKIP_EXTENSIONS = (".jpg", ".jpeg", ".png", ".gif", ".svg", ".css", ".js", ".pdf", ".zip")


def discover_campaign_urls(
    html: str,
    base_url: str,
    merchant_slug: str,
    *,
    limit: int | None = None,
) -> List[str]:
    """
    Return same-host links that look like merchant campaigns (not store index only).
    """
    max_links = limit if limit is not None else int(os.getenv("CRAWLER_CAMPAIGN_LINK_LIMIT", "2"))
    if not html or not base_url:
        return []

    base_host = urlparse(base_url).netloc.lower()
    slug = merchant_slug.lower().strip()
    seen: set[str] = set()
    ordered: List[str] = []

    for match in LINK_RE.finditer(html):
        href = (match.group(1) or "").strip()
        if not href or href.startswith("#") or href.lower().startswith("javascript:"):
            continue
        if any(href.lower().endswith(ext) for ext in SKIP_EXTENSIONS):
            continue

        full = urljoin(base_url, href)
        parsed = urlparse(full)
        if parsed.netloc.lower() != base_host:
            continue

        path_lower = (parsed.path or "").lower()
        query_lower = (parsed.query or "").lower()
        combined = f"{path_lower}?{query_lower}"

        if full.rstrip("/") == base_url.rstrip("/"):
            continue

        has_hint = any(h in combined for h in CAMPAIGN_HINTS)
        has_merchant = slug in combined
        if not has_hint and not has_merchant:
            continue

        if full in seen:
            continue
        seen.add(full)
        ordered.append(full)
        if len(ordered) >= max_links:
            break

    return ordered
