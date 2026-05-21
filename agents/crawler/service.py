"""Entry point for autonomous crawler surveillance (SurveillanceEngine)."""
from __future__ import annotations

import asyncio

from dotenv import load_dotenv

from agents.crawler.agent import CrawlerAgent
from agents.crawler.surveillance import SurveillanceEngine

load_dotenv()


async def main() -> None:
    await SurveillanceEngine(CrawlerAgent()).run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nCrawler surveillance stopped.")
