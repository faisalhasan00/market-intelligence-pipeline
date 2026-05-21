"""CLI entry: python -m orchestrator [--once Merchant] [--watchlist] [--webhook-server]"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import time

from dotenv import load_dotenv

from orchestrator.factory import build_orchestrator
from orchestrator.webhook import run_webhook_server


async def run_once(merchant: str) -> int:
    orchestrator = build_orchestrator()
    query = merchant if merchant.lower().startswith("analyze") else f"Analyze {merchant} coupons"
    result = await orchestrator.run_pipeline(query)
    print(json.dumps(result.to_dict(), indent=2))
    return 0 if result.success else 1


async def run_webhook() -> None:
    port = int(os.getenv("ORCHESTRATOR_WEBHOOK_PORT", "8081"))
    orchestrator = build_orchestrator()
    try:
        await run_webhook_server(orchestrator, port=port)
    except KeyboardInterrupt:
        await orchestrator.stop_event_integration()


async def run_watchlist_loop() -> None:
    from messaging.schemas import AgentRole

    orchestrator = build_orchestrator()
    crawler = orchestrator.agents[AgentRole.CRAWLER]
    watchlist = crawler.policy.active_watchlist()
    await orchestrator.start_event_integration()

    print("=" * 50)
    print("SWARM ORCHESTRATOR — WATCHLIST MODE")
    print("=" * 50)
    print(f"Watchlist: {watchlist}")
    print("Event stream integration: ON\n")

    iteration = 1
    while True:
        print(f"--- [ITERATION {iteration}] {time.strftime('%H:%M:%S')} ---")
        results = await orchestrator.run_adaptive_iteration(watchlist)
        if not results:
            print("   [Scheduler] No merchants due — sleeping.")
        crawled = sum(1 for r in results if r.success)
        sleep_sec = crawler.policy.sleep_between_iterations(crawled)
        if orchestrator.total_cost >= orchestrator.budget_limit:
            print("🛑 [CRITICAL] Budget limit reached. Stopping.")
            break
        print(f"--- Done ({crawled} merchants). Sleep {sleep_sec}s ---\n")
        iteration += 1
        await asyncio.sleep(sleep_sec)


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Swarm orchestrator control plane")
    parser.add_argument("--once", metavar="MERCHANT", help="Run full pipeline once for a merchant")
    parser.add_argument(
        "--watchlist",
        action="store_true",
        help="Adaptive watchlist loop (same as main.py with orchestrator APIs)",
    )
    parser.add_argument(
        "--webhook-server",
        action="store_true",
        help="Start HTTP webhook receiver for crawler events (POST /webhook)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Webhook server port (default: ORCHESTRATOR_WEBHOOK_PORT or 8081)",
    )
    args = parser.parse_args(argv)

    if args.once:
        return asyncio.run(run_once(args.once))
    if args.webhook_server:
        if args.port is not None:
            os.environ["ORCHESTRATOR_WEBHOOK_PORT"] = str(args.port)
        try:
            asyncio.run(run_webhook())
        except KeyboardInterrupt:
            print("\n👋 Webhook server stopped.")
        return 0
    if args.watchlist:
        try:
            asyncio.run(run_watchlist_loop())
        except KeyboardInterrupt:
            print("\n👋 Orchestrator stopped.")
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
