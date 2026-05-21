import asyncio
import os
import sys
import time

if sys.platform.startswith("win"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from dotenv import load_dotenv

from messaging.schemas import AgentRole
from orchestrator.factory import build_orchestrator


async def main():
    load_dotenv(override=True)

    orchestrator = build_orchestrator()
    crawler = orchestrator.agents[AgentRole.CRAWLER]
    watchlist = crawler.policy.active_watchlist()

    if os.getenv("ORCHESTRATOR_EVENT_INTEGRATION", "true").lower() == "true":
        await orchestrator.start_event_integration()

    print("=" * 50)
    print("SWARM: ADAPTIVE AUTONOMOUS MARKET INTELLIGENCE")
    print("=" * 50)
    print(f"Watchlist: {watchlist}")
    print("Crawler self-decides due merchants, cadence, and pipeline depth.")
    print("Press Ctrl+C to stop.\n")

    iteration = 1
    BLUE, BOLD, RESET = "\033[94m", "\033[1m", "\033[0m"

    while True:
        print(f"{BLUE}{BOLD}--- [ITERATION {iteration}] {time.strftime('%H:%M:%S')} ---{RESET}")

        due = crawler.get_due_merchants(watchlist)
        if not due:
            print("   [Scheduler] No merchants due — sleeping.")
        else:
            skipped = [m for m in watchlist if m not in due]
            if skipped:
                print(f"   [Scheduler] Due (priority order): {due}")
                print(f"   [Scheduler] Waiting on interval: {skipped}")

        results = await orchestrator.run_adaptive_iteration(watchlist)
        crawled = sum(1 for r in results if r.success)

        if orchestrator.total_cost >= orchestrator.budget_limit:
            print("🛑 [CRITICAL] Budget limit reached. Stopping service.")
            return

        sleep_sec = crawler.policy.sleep_between_iterations(crawled)
        print(f"--- [ITERATION {iteration}] Done ({crawled} crawls). Sleep {sleep_sec}s ---\n")
        iteration += 1
        await asyncio.sleep(sleep_sec)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Swarm service stopped by user.")
