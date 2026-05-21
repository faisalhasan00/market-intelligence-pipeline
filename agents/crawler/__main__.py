"""
Crawler CLI.

  python -m agents.crawler                  # one-shot Flipkart
  python -m agents.crawler Myntra           # one-shot merchant
  python -m agents.crawler --surveillance   # 24/7 closed-loop radar (recommended)
  python -m agents.crawler --autonomous     # alias for --surveillance
  python -m agents.crawler --status         # monitoring schedule
  python -m agents.crawler --metrics        # JSON metrics snapshot
  python -m agents.crawler --metrics --prometheus
  python -m agents.crawler --serve-metrics --port 9090
  python -m agents.crawler.worker           # distributed task worker
"""
from __future__ import annotations

import asyncio
import json
import sys

from agents.crawler.agent import CrawlerAgent
from agents.crawler.surveillance import SurveillanceEngine


async def _one_shot(query: str) -> None:
    data = await CrawlerAgent().collect_intelligence(query)
    print(f"\nMerchant: {data.get('merchant')}")
    print(f"Validated cashback: {data.get('cashback_rate')}")
    print(f"Offers: {len(data.get('offers', []))}")
    print(f"Events: {[e.get('type') for e in data.get('events', [])]}")
    mon = data.get("monitoring") or {}
    if mon:
        print(f"Next crawl: {mon.get('crawl_mode')} in {mon.get('crawl_interval_sec')}s")


async def _status() -> None:
    print(json.dumps(CrawlerAgent().monitoring_summary(), indent=2))


async def _surveillance() -> None:
    await SurveillanceEngine(CrawlerAgent()).run()


async def _autonomous() -> None:
    await _surveillance()


def _metrics() -> None:
    from agents.crawler.platform.metrics import print_metrics

    prometheus = "--prometheus" in sys.argv or "-p" in sys.argv
    print_metrics(prometheus=prometheus)


def _serve_metrics() -> None:
    from agents.crawler.platform.metrics_server import serve_metrics

    port = 9090
    args = sys.argv[1:]
    for i, a in enumerate(args):
        if a in ("--port", "-P") and i + 1 < len(args):
            port = int(args[i + 1])
        elif a.startswith("--port="):
            port = int(a.split("=", 1)[1])
    serve_metrics(port=port)


def main() -> None:
    args = sys.argv[1:]
    if not args:
        asyncio.run(_one_shot("Flipkart"))
        return
    if args[0] in ("--serve-metrics", "--metrics-server"):
        _serve_metrics()
        return
    if args[0] in ("--surveillance", "--autonomous", "-a", "--radar"):
        asyncio.run(_surveillance())
        return
    if args[0] in ("--status", "-s"):
        asyncio.run(_status())
        return
    if args[0] in ("--metrics", "-m"):
        _metrics()
        return
    query = " ".join(args).strip()
    if not query.lower().startswith("analyze"):
        query = f"Analyze {query} coupons"
    asyncio.run(_one_shot(query))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")
