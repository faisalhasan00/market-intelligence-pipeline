"""
Unified 24/7 surveillance loop — registry, crawl planning, policy actions, task queue.
"""
from __future__ import annotations

import asyncio
import os
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from agents.crawler.crawl.profiles import merchant_display, normalize_query
from agents.crawler.platform.registry import MerchantRegistry
from agents.crawler.scheduling.actions import ActionType, CrawlAction, CrawlPlan

if TYPE_CHECKING:
    from agents.crawler.agent import CrawlerAgent


MAGENTA = "\033[95m"
CYAN = "\033[96m"
RESET = "\033[0m"


@dataclass
class SurveillanceRunSummary:
    iteration: int
    merchants_crawled: int
    events_total: int
    actions_taken: int
    task_queue_processed: int


class SurveillanceEngine:
    """Closed-loop market radar: plan → crawl → act → queue."""

    def __init__(self, agent: "CrawlerAgent"):
        self.agent = agent
        self.worker_id = f"surveillance-{uuid.uuid4().hex[:8]}"
        self._plan_overrides: Dict[str, CrawlPlan] = {}
        self.registry = MerchantRegistry(self.agent.store, self.agent.policy)
        self.registry.seed_from_env()

    async def run(self, merchants: Optional[List[str]] = None) -> None:
        watchlist = merchants or self.registry.get_active_merchants()
        print("=" * 55)
        print("CRAWLER — SURVEILLANCE ENGINE")
        print("=" * 55)
        print(f"Watchlist: {watchlist}")
        print(
            f"Workers: {self.agent.worker_pool.concurrency} | "
            f"Proxies: {'on' if self.agent.collector_proxy_enabled else 'off'}"
        )
        print("Press Ctrl+C to stop.\n")

        iteration = 0
        while True:
            iteration += 1
            summary = await self._run_iteration(iteration, watchlist)
            sleep_sec = self.agent.policy.sleep_between_iterations(summary.merchants_crawled)
            print(
                f"--- [SURVEILLANCE {iteration}] "
                f"{summary.merchants_crawled} crawl(s), "
                f"{summary.events_total} event(s), "
                f"{summary.actions_taken} action(s). "
                f"Sleep {sleep_sec}s ---"
            )
            await asyncio.sleep(sleep_sec)

    async def _run_iteration(
        self,
        iteration: int,
        watchlist: List[str],
    ) -> SurveillanceRunSummary:
        watchlist = self.registry.get_active_merchants() or watchlist
        reclaimed = self.agent.store.reclaim_stale_tasks()
        if reclaimed:
            print(f"   [Queue] Reclaimed {reclaimed} stale task(s)")
        due = self.agent.get_due_merchants(watchlist)
        waiting = [m for m in watchlist if m not in due]
        print(f"\n--- [SURVEILLANCE {iteration}] Due: {due or '(none)'} ---")
        if waiting:
            print(f"   On interval: {waiting}")

        for name in due:
            slug = normalize_query(name)
            plan = self.agent.policy.plan_crawl(slug, budget=self.agent.budget)
            self.agent.store.enqueue_crawl(slug, priority=plan.priority)

        crawled = 0
        events_total = 0
        actions_taken = 0

        limit = max(len(due), 1)
        tasks = self.agent.store.claim_tasks(self.worker_id, limit=limit)
        for task in tasks:
            slug = task["merchant_slug"]
            plan = self._plan_overrides.pop(slug, None) or self.agent.policy.plan_crawl(
                slug, budget=self.agent.budget
            )
            display = self._display_for_slug(slug, watchlist)
            try:
                payload = await self.agent.collect_intelligence(
                    f"Analyze {display} coupons",
                    full_scrape=plan.full_scrape,
                )
                actions = self.agent.policy.act(slug, payload)
                taken = await self.execute_actions(actions)
                actions_taken += taken
                events_total += len(payload.get("events") or [])
                self.agent.store.complete_task(
                    task["id"],
                    {
                        "merchant": slug,
                        "events": len(payload.get("events", [])),
                        "mode": (payload.get("monitoring") or {}).get("crawl_mode"),
                        "actions": [a.action.value for a in actions],
                    },
                )
                self._log_result(payload, actions)
                crawled += 1
            except Exception as e:
                print(f"   [Surveillance] Task {task['id']} failed: {e}")
                self.agent.store.complete_task(task["id"], {"error": str(e)}, failed=True)

        queue_extra = await self._drain_task_queue(max_rounds=4, batch_size=5)
        return SurveillanceRunSummary(
            iteration=iteration,
            merchants_crawled=crawled,
            events_total=events_total,
            actions_taken=actions_taken,
            task_queue_processed=queue_extra,
        )

    async def _drain_task_queue(self, *, max_rounds: int = 4, batch_size: int = 5) -> int:
        """Process sweep / recrawl tasks until queue is empty or round cap hit."""
        processed = 0
        for _ in range(max_rounds):
            batch = await self._drain_task_batch(batch_size)
            if batch == 0:
                break
            processed += batch
        return processed

    async def _drain_task_batch(self, batch_size: int) -> int:
        processed = 0
        tasks = self.agent.store.claim_tasks(self.worker_id, limit=batch_size)
        for task in tasks:
            slug = task["merchant_slug"]
            plan = self._plan_overrides.pop(slug, None) or self.agent.policy.plan_crawl(
                slug, budget=self.agent.budget
            )
            display = merchant_display(slug)
            try:
                payload = await self.agent.collect_intelligence(
                    f"Analyze {display} coupons",
                    full_scrape=plan.full_scrape,
                )
                actions = self.agent.policy.act(slug, payload)
                await self.execute_actions(actions)
                self.agent.store.complete_task(
                    task["id"],
                    {"merchant": slug, "events": len(payload.get("events", [])), "queued": True},
                )
                processed += 1
            except Exception as e:
                self.agent.store.complete_task(task["id"], {"error": str(e)}, failed=True)
        return processed

    async def execute_actions(self, actions: List[CrawlAction]) -> int:
        taken = 0
        for action in actions:
            if action.action == ActionType.CRAWL_ONLY:
                continue
            print(f"   {MAGENTA}[Action]{RESET} {action.label()} — {action.reason}")
            taken += 1
            slug = action.merchant_slug

            if action.action == ActionType.FULL_SCRAPE:
                plan = self.agent.policy.plan_crawl(
                    slug, force_full=True, budget=self.agent.budget
                )
                self._plan_overrides[slug] = plan
                self.agent.store.enqueue_crawl(slug, priority=action.priority or 0)

            elif action.action == ActionType.MARKET_SWEEP:
                self.agent.intelligence.sweep.plan_sweep(slug, action.payload.get("events") or [])

            elif action.action == ActionType.SET_CRITICAL:
                self.agent.policy.after_crawl(
                    slug,
                    [{"type": "cashback_spike_detected"}],
                )

            elif action.action == ActionType.SET_DORMANT:
                self.agent.store.increment_dormant_count(slug)

            elif action.action in (ActionType.RECRAWL_IN, ActionType.RECRAWL_TRUSTED):
                delay = action.delay_sec
                if delay > 0:
                    await asyncio.sleep(min(delay, 30))
                plan = self.agent.policy.plan_crawl(
                    slug, force_full=True, budget=self.agent.budget
                )
                self._plan_overrides[slug] = plan
                self.agent.store.enqueue_crawl(slug, priority=action.priority or 0)

            elif action.action == ActionType.ROTATE_PROXY:
                if self.agent.proxy_pool.enabled:
                    self.agent.proxy_pool.next()

        return taken

    def _display_for_slug(self, slug: str, merchants: List[str]) -> str:
        for name in merchants:
            if normalize_query(name) == slug:
                return name
        row = self.agent.store.get_merchant(slug)
        if row:
            return row["display_name"]
        return merchant_display(slug)

    def _log_result(self, payload: Dict[str, Any], actions: List[CrawlAction]) -> None:
        monitoring = payload.get("monitoring") or {}
        events = payload.get("events") or []
        if events:
            print(f"   {CYAN}Events:{RESET} {[e.get('type') for e in events[:8]]}")
        if monitoring:
            print(
                f"   Next: {monitoring.get('crawl_mode')} "
                f"every {monitoring.get('crawl_interval_sec')}s — "
                f"{monitoring.get('monitor_reason', '')}"
            )
        if actions:
            print(f"   Actions planned: {[a.action.value for a in actions]}")
