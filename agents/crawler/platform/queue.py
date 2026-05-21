"""Crawl task queue — SQLite (default) or optional Redis backend."""
from __future__ import annotations

import json
import os
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from agents.crawler.platform.store import IntelligenceStore

_PENDING = "crawler:queue:pending"
_SLUG = "crawler:queue:slug:"
_TASK = "crawler:queue:task:"
_RUNNING = "crawler:queue:running"
_ID = "crawler:queue:next_id"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _score(priority: int) -> float:
    return float(priority) * 1e12 + time.time()


class QueueBackend(ABC):
    @abstractmethod
    def enqueue_crawl(self, merchant_slug: str, priority: int = 5) -> int: ...

    @abstractmethod
    def claim_tasks(self, worker_id: str, limit: int = 3) -> List[Dict[str, Any]]: ...

    @abstractmethod
    def complete_task(self, task_id: int, result: Dict[str, Any], *, failed: bool = False) -> None: ...

    @abstractmethod
    def get_queue_stats(self) -> Dict[str, int]: ...

    @abstractmethod
    def reclaim_stale_tasks(self, *, stale_minutes: Optional[int] = None) -> int: ...


class SqliteQueueBackend(QueueBackend):
    def __init__(self, store: IntelligenceStore) -> None:
        self._store = store

    def enqueue_crawl(self, merchant_slug: str, priority: int = 5) -> int:
        return self._store._enqueue_crawl_sqlite(merchant_slug, priority)

    def claim_tasks(self, worker_id: str, limit: int = 3) -> List[Dict[str, Any]]:
        return self._store._claim_tasks_sqlite(worker_id, limit)

    def complete_task(self, task_id: int, result: Dict[str, Any], *, failed: bool = False) -> None:
        return self._store._complete_task_sqlite(task_id, result, failed=failed)

    def get_queue_stats(self) -> Dict[str, int]:
        return self._store._get_queue_stats_sqlite()

    def reclaim_stale_tasks(self, *, stale_minutes: Optional[int] = None) -> int:
        return self._store._reclaim_stale_tasks_sqlite(stale_minutes=stale_minutes)


class RedisQueueBackend(QueueBackend):
    def __init__(self, redis_url: str, store: IntelligenceStore) -> None:
        import redis

        self._r = redis.from_url(redis_url, decode_responses=True)
        self._r.ping()
        self._store = store

    def _next_id(self) -> int:
        return int(self._r.incr(_ID))

    def enqueue_crawl(self, merchant_slug: str, priority: int = 5) -> int:
        slug = merchant_slug.lower().strip()
        existing = self._r.get(f"{_SLUG}{slug}")
        if existing:
            return int(existing)
        task_id = self._next_id()
        now = _utc_now()
        pipe = self._r.pipeline()
        pipe.hset(
            f"{_TASK}{task_id}",
            mapping={
                "id": str(task_id),
                "merchant_slug": slug,
                "status": "pending",
                "priority": str(priority),
                "created_at": now,
            },
        )
        pipe.set(f"{_SLUG}{slug}", task_id)
        pipe.zadd(_PENDING, {str(task_id): _score(priority)})
        pipe.execute()
        return task_id

    def claim_tasks(self, worker_id: str, limit: int = 3) -> List[Dict[str, Any]]:
        claimed: List[Dict[str, Any]] = []
        now = _utc_now()
        candidates = self._r.zrange(_PENDING, 0, limit * 3 - 1)
        for tid in candidates:
            if len(claimed) >= limit:
                break
            key = f"{_TASK}{tid}"
            if not self._r.hget(key, "merchant_slug"):
                self._r.zrem(_PENDING, tid)
                continue
            if self._r.hsetnx(key, "claim_lock", worker_id) == 0:
                if self._r.hget(key, "claim_lock") != worker_id:
                    continue
            status = self._r.hget(key, "status")
            if status != "pending":
                self._r.zrem(_PENDING, tid)
                self._r.hdel(key, "claim_lock")
                continue
            self._r.hset(
                key,
                mapping={
                    "status": "running",
                    "worker_id": worker_id,
                    "started_at": now,
                },
            )
            self._r.hdel(key, "claim_lock")
            self._r.zrem(_PENDING, tid)
            self._r.zadd(_RUNNING, {tid: time.time()})
            row = self._r.hgetall(key)
            claimed.append(
                {
                    "id": int(tid),
                    "merchant_slug": row.get("merchant_slug", ""),
                    "priority": int(row.get("priority") or 5),
                }
            )
        return claimed

    def complete_task(self, task_id: int, result: Dict[str, Any], *, failed: bool = False) -> None:
        key = f"{_TASK}{task_id}"
        row = self._r.hgetall(key)
        slug = row.get("merchant_slug", "")
        status = "failed" if failed else "done"
        self._r.hset(
            key,
            mapping={
                "status": status,
                "finished_at": _utc_now(),
                "result_json": json.dumps(result),
            },
        )
        self._r.zrem(_RUNNING, str(task_id))
        if slug:
            self._r.delete(f"{_SLUG}{slug}")

    def get_queue_stats(self) -> Dict[str, int]:
        pending = self._r.zcard(_PENDING)
        running = self._r.zcard(_RUNNING)
        counts = {"pending": pending, "running": running, "done": 0, "failed": 0}
        for tid in self._r.keys(f"{_TASK}*"):
            st = self._r.hget(tid, "status") or ""
            if st in ("done", "failed"):
                counts[st] = counts.get(st, 0) + 1
        counts["total"] = sum(counts.values())
        return counts

    def reclaim_stale_tasks(self, *, stale_minutes: Optional[int] = None) -> int:
        minutes = stale_minutes or int(os.getenv("CRAWLER_TASK_STALE_MINUTES", "45"))
        cutoff = time.time() - minutes * 60
        reclaimed = 0
        stale = self._r.zrangebyscore(_RUNNING, 0, cutoff)
        for tid in stale:
            key = f"{_TASK}{tid}"
            if self._r.hget(key, "status") != "running":
                self._r.zrem(_RUNNING, tid)
                continue
            priority = int(self._r.hget(key, "priority") or 5)
            self._r.hset(
                key,
                mapping={"status": "pending", "worker_id": "", "started_at": ""},
            )
            self._r.zadd(_PENDING, {tid: _score(priority)})
            self._r.zrem(_RUNNING, tid)
            reclaimed += 1
        return reclaimed


def get_queue_backend(store: IntelligenceStore) -> QueueBackend:
    backend = os.getenv("CRAWLER_QUEUE_BACKEND", "sqlite").lower()
    if backend != "redis":
        return SqliteQueueBackend(store)
    url = os.getenv("CRAWLER_REDIS_URL", "").strip()
    if not url:
        print("[Queue] CRAWLER_QUEUE_BACKEND=redis but CRAWLER_REDIS_URL unset — using SQLite")
        return SqliteQueueBackend(store)
    try:
        return RedisQueueBackend(url, store)
    except Exception as exc:
        print(f"[Queue] Redis unavailable ({exc}) — falling back to SQLite")
        return SqliteQueueBackend(store)
