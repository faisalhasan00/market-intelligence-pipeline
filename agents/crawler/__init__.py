"""
Autonomous market intelligence crawler.

    agent.py              CrawlerAgent
    service.py            surveillance entrypoint
    intelligence/         post-crawl intelligence hub
    scheduling/           adaptive policy + scheduler
    crawl/                acquisition, parse, visual
    extraction/           validation + extractors
    platform/             store, memory, categories
"""
from agents.crawler.agent import CrawlerAgent
from agents.crawler.intelligence import IntelligenceHub, get_intelligence_stream
from agents.crawler.scheduling import AdaptiveCrawlPolicy, MarketScheduler
from agents.crawler.platform import CrawlBudget, CrawlerMemory, IntelligenceStore
from agents.crawler.platform.registry import MerchantRegistry
from agents.crawler.surveillance import SurveillanceEngine

__all__ = [
    "CrawlerAgent",
    "IntelligenceHub",
    "get_intelligence_stream",
    "AdaptiveCrawlPolicy",
    "MarketScheduler",
    "IntelligenceStore",
    "CrawlBudget",
    "CrawlerMemory",
    "MerchantRegistry",
    "SurveillanceEngine",
]
