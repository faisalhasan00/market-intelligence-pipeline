"""Distributed crawl worker — SQLite or Redis queue (see platform/queue.py)."""
from __future__ import annotations

import asyncio
import os
import uuid
from typing import Optional

from dotenv import load_dotenv

from agents.crawler.agent import CrawlerAgent
from agents.crawler.crawl.profiles import merchant_display
from agents.crawler.surveillance import SurveillanceEngine

load_dotenv()


class CrawlTaskWorker:
    """Polls crawl_tasks, runs intelligence collection, completes tasks."""

    def __init__(self, agent: Optional[CrawlerAgent] = None, *, worker_id: Optional[str] = None):
        self.agent = agent or CrawlerAgent()
        self.worker_id = worker_id or os.getenv("CRAWLER_WORKER_ID") or f"worker-{uuid.uuid4().hex[:8]}"
        self.poll_sec = int(os.getenv("CRAWLER_WORKER_POLL_SEC", "5"))
        self.batch_size = int(os.getenv("CRAWLER_WORKER_BATCH", "3"))
        self._surveillance = SurveillanceEngine(self.agent)

    async def run_once(self) -> int:
        """Process one batch; returns number of tasks completed."""
        self.agent.store.reclaim_stale_tasks()
        tasks = self.agent.store.claim_tasks(self.worker_id, limit=self.batch_size)
        if not tasks:
            return 0
        done = 0
        for task in tasks:
            slug = task["merchant_slug"]
            display = merchant_display(slug)
            plan = self.agent.policy.plan_crawl(slug, budget=self.agent.budget)
            try:
                payload = await self.agent.collect_intelligence(
                    f"Analyze {display} coupons",
                    full_scrape=plan.full_scrape,
                )
                actions = self.agent.policy.act(slug, payload)
                await self._surveillance.execute_actions(actions)
                self.agent.store.complete_task(
                    task["id"],
                    {
                        "merchant": slug,
                        "events": len(payload.get("events") or []),
                        "worker": self.worker_id,
                    },
                )
                done += 1
            except Exception as e:
                print(f"[Worker {self.worker_id}] Task {task['id']} failed: {e}")
                self.agent.store.complete_task(task["id"], {"error": str(e)}, failed=True)
        return done

    async def run_forever(self) -> None:
        backend = os.getenv("CRAWLER_QUEUE_BACKEND", "sqlite").lower()
        print(f"Crawl worker {self.worker_id} | backend={backend} | batch={self.batch_size}")
        while True:
            n = await self.run_once()
            if n == 0:
                await asyncio.sleep(self.poll_sec)


async def main() -> None:
    await CrawlTaskWorker().run_forever()
