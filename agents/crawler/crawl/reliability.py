"""Source reliability ranking and crawl target ordering."""
from __future__ import annotations

import os
from typing import List

from agents.crawler.platform.categories import category_profile
from agents.crawler.crawl.profiles import COMPETITOR_STORE_URLS, CrawlTarget, DEFAULT_COMPETITORS
from agents.crawler.platform.memory import CrawlerMemory
from agents.crawler.platform.store import IntelligenceStore
import urllib.parse

# Only skip sources that are consistently blocked (not one timeout)
DEAD_BLOCK_THRESHOLD = int(os.getenv("CRAWLER_DEAD_BLOCK_THRESHOLD", "8"))


def ordered_competitors(
    store: IntelligenceStore,
    max_count: int,
    merchant_slug: str = "",
) -> List[str]:
    """
    Return up to max_count competitors, best reliability first.
    Still includes lower-ranked sites so we don't only ever crawl CashKaro + PaisaWapas.
    """
    rankings = {name: score for name, score in store.get_source_rankings()}
    block_counts = store.get_block_counts()

    preferred = set(category_profile(merchant_slug).preferred_sources) if merchant_slug else set()

    def sort_key(name: str) -> tuple:
        if block_counts.get(name, 0) >= DEAD_BLOCK_THRESHOLD:
            return (0, 0, rankings.get(name, 0.5))
        cat_boost = 1 if name in preferred else 0
        return (1, cat_boost, rankings.get(name, 0.5))

    names = sorted(DEFAULT_COMPETITORS, key=sort_key, reverse=True)
    return names[:max_count]


def build_targets_ranked(
    merchant_slug: str,
    store: IntelligenceStore,
    *,
    full_scrape: bool = False,
    max_competitors: int | None = None,
) -> List[CrawlTarget]:
    from agents.crawler.crawl.profiles import competitor_count_for_run

    count = max_competitors if max_competitors is not None else competitor_count_for_run(
        full_scrape=full_scrape
    )
    names = ordered_competitors(store, count, merchant_slug)
    names = CrawlerMemory(store).plan_sources(merchant_slug, names)
    targets: List[CrawlTarget] = []
    for name in names:
        template = COMPETITOR_STORE_URLS[name]
        url = template.format(slug=urllib.parse.quote(merchant_slug))
        targets.append(CrawlTarget(source_name=name, url=url, target_type="competitor"))
    return targets
