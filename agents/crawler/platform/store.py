"""SQLite persistence for crawl history, events, and source reliability."""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

DB_PATH = os.path.join("data", "market_intelligence.db")
_LEGACY_JSON = os.path.join("data", "crawler_history.json")


class IntelligenceStore:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self._lock = threading.Lock()
        self._init_schema()
        self._migrate_legacy_json()
        from agents.crawler.platform.queue import get_queue_backend

        self._queue = get_queue_backend(self)

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self._lock, self._conn() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS source_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    merchant_slug TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    cashback_rate TEXT,
                    offers_json TEXT NOT NULL,
                    confidence REAL,
                    blocked INTEGER DEFAULT 0,
                    collected_at TEXT NOT NULL,
                    UNIQUE(merchant_slug, source_name)
                );

                CREATE TABLE IF NOT EXISTS crawl_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    merchant_slug TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    offer_count INTEGER DEFAULT 0,
                    event_count INTEGER DEFAULT 0,
                    aggregate_confidence REAL,
                    cost_usd REAL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS intelligence_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    merchant_slug TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    source_name TEXT,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS source_reliability (
                    source_name TEXT PRIMARY KEY,
                    successes INTEGER DEFAULT 0,
                    failures INTEGER DEFAULT 0,
                    blocks INTEGER DEFAULT 0,
                    last_success_at TEXT,
                    last_failure_at TEXT,
                    reliability_score REAL DEFAULT 0.5
                );

                CREATE TABLE IF NOT EXISTS merchant_schedule (
                    merchant_slug TEXT PRIMARY KEY,
                    priority_tier INTEGER DEFAULT 2,
                    last_crawled_at TEXT,
                    next_crawl_at TEXT,
                    crawl_interval_sec INTEGER DEFAULT 3600,
                    hot_until TEXT,
                    analyst_risk TEXT DEFAULT 'LOW',
                    strategist_priority TEXT DEFAULT 'LOW',
                    crawl_mode TEXT DEFAULT 'normal',
                    monitor_reason TEXT
                );

                CREATE TABLE IF NOT EXISTS crawl_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    merchant_slug TEXT NOT NULL,
                    status TEXT DEFAULT 'pending',
                    priority INTEGER DEFAULT 5,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    worker_id TEXT,
                    result_json TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_events_merchant ON intelligence_events(merchant_slug, created_at);
                CREATE INDEX IF NOT EXISTS idx_runs_merchant ON crawl_runs(merchant_slug, started_at);
                CREATE TABLE IF NOT EXISTS offer_lifecycle (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    merchant_slug TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    offer_key TEXT NOT NULL,
                    offer_text TEXT NOT NULL,
                    change_type TEXT NOT NULL,
                    detected_at TEXT NOT NULL,
                    UNIQUE(merchant_slug, source_name, offer_key, change_type, detected_at)
                );

                CREATE INDEX IF NOT EXISTS idx_tasks_status ON crawl_tasks(status, priority);
                CREATE INDEX IF NOT EXISTS idx_lifecycle_merchant ON offer_lifecycle(merchant_slug, detected_at);

                CREATE TABLE IF NOT EXISTS parser_memory (
                    merchant_slug TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    selector_chain_json TEXT,
                    last_selector TEXT,
                    last_dom_fingerprint TEXT,
                    success_count INTEGER DEFAULT 0,
                    fail_count INTEGER DEFAULT 0,
                    last_offer_count INTEGER DEFAULT 0,
                    updated_at TEXT,
                    PRIMARY KEY (merchant_slug, source_name)
                );

                CREATE TABLE IF NOT EXISTS dom_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    merchant_slug TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    dom_fingerprint TEXT NOT NULL,
                    offer_count INTEGER DEFAULT 0,
                    recorded_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS merchant_memory (
                    merchant_slug TEXT PRIMARY KEY,
                    category TEXT,
                    volatility_score REAL DEFAULT 1.0,
                    anti_bot_json TEXT,
                    behavior_json TEXT,
                    updated_at TEXT
                );

                CREATE TABLE IF NOT EXISTS offer_state (
                    merchant_slug TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    offer_key TEXT NOT NULL,
                    offer_text TEXT NOT NULL,
                    stage TEXT DEFAULT 'active',
                    mention_count INTEGER DEFAULT 0,
                    first_seen_at TEXT,
                    last_seen_at TEXT,
                    PRIMARY KEY (merchant_slug, source_name, offer_key)
                );

                CREATE TABLE IF NOT EXISTS rate_samples (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    merchant_slug TEXT NOT NULL,
                    source_name TEXT,
                    rate_pct REAL NOT NULL,
                    recorded_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_rate_samples ON rate_samples(merchant_slug, recorded_at);
                CREATE INDEX IF NOT EXISTS idx_dom_history ON dom_history(merchant_slug, source_name);

                CREATE TABLE IF NOT EXISTS merchants (
                    merchant_slug TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    category TEXT,
                    tier INTEGER DEFAULT 2,
                    enabled INTEGER DEFAULT 1,
                    client_priority INTEGER DEFAULT 0,
                    promoted_until TEXT,
                    dormant_count INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS budget_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    merchant_slug TEXT,
                    cost_usd REAL DEFAULT 0,
                    recorded_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_budget_events_time
                    ON budget_events(event_type, recorded_at);

                CREATE TABLE IF NOT EXISTS analyst_outputs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    merchant_slug TEXT NOT NULL,
                    output_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_analyst_outputs_merchant
                    ON analyst_outputs(merchant_slug, created_at);

                CREATE TABLE IF NOT EXISTS analyst_shadow_deltas (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    merchant_slug TEXT NOT NULL,
                    primary_risk TEXT NOT NULL,
                    shadow_risk TEXT NOT NULL,
                    shadow_model TEXT,
                    delta_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_analyst_shadow_merchant
                    ON analyst_shadow_deltas(merchant_slug, created_at);
                """
            )
        self._migrate_schedule_columns()
        self._migrate_reliability_columns()
        self._migrate_snapshot_columns()

    def _migrate_schedule_columns(self) -> None:
        with self._lock, self._conn() as conn:
            existing = {row[1] for row in conn.execute("PRAGMA table_info(merchant_schedule)").fetchall()}
            for col, ddl in (
                ("analyst_risk", "TEXT DEFAULT 'LOW'"),
                ("strategist_priority", "TEXT DEFAULT 'LOW'"),
                ("crawl_mode", "TEXT DEFAULT 'normal'"),
                ("monitor_reason", "TEXT"),
            ):
                if col not in existing:
                    conn.execute(f"ALTER TABLE merchant_schedule ADD COLUMN {col} {ddl}")

    def _migrate_reliability_columns(self) -> None:
        with self._lock, self._conn() as conn:
            existing = {row[1] for row in conn.execute("PRAGMA table_info(source_reliability)").fetchall()}
            if "cooldown_until" not in existing:
                conn.execute("ALTER TABLE source_reliability ADD COLUMN cooldown_until TEXT")

    def _migrate_snapshot_columns(self) -> None:
        with self._lock, self._conn() as conn:
            existing = {row[1] for row in conn.execute("PRAGMA table_info(source_snapshots)").fetchall()}
            for col, ddl in (
                ("dom_fingerprint", "TEXT"),
                ("offer_fingerprint", "TEXT"),
                ("screenshot_hash", "TEXT"),
                ("rate_pct", "REAL"),
                ("dom_text_sample", "TEXT"),
                ("perceptual_hash", "TEXT"),
                ("hero_perceptual_hash", "TEXT"),
            ):
                if col not in existing:
                    conn.execute(f"ALTER TABLE source_snapshots ADD COLUMN {col} {ddl}")

    def _migrate_legacy_json(self) -> None:
        if not os.path.exists(_LEGACY_JSON):
            return
        try:
            with open(_LEGACY_JSON, "r", encoding="utf-8") as f:
                legacy = json.load(f)
        except (OSError, json.JSONDecodeError):
            return
        if not legacy:
            return
        now = _utc_now()
        with self._lock, self._conn() as conn:
            for slug, blob in legacy.items():
                for source, snap in blob.get("sources", {}).items():
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO source_snapshots
                        (merchant_slug, source_name, cashback_rate, offers_json, confidence, collected_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            slug,
                            source,
                            snap.get("cashback_rate"),
                            json.dumps(snap.get("offers") or []),
                            snap.get("confidence"),
                            now,
                        ),
                    )
                conn.execute(
                    """
                    INSERT OR IGNORE INTO merchant_schedule (merchant_slug, last_crawled_at)
                    VALUES (?, ?)
                    """,
                    (slug, now),
                )

    def get_snapshots(self, merchant_slug: str) -> Dict[str, Dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM source_snapshots WHERE merchant_slug = ?",
                (merchant_slug,),
            ).fetchall()
        out: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            out[row["source_name"]] = {
                "cashback_rate": row["cashback_rate"],
                "offers": json.loads(row["offers_json"] or "[]"),
                "confidence": row["confidence"],
                "dom_fingerprint": row["dom_fingerprint"] if "dom_fingerprint" in row.keys() else None,
                "offer_fingerprint": row["offer_fingerprint"] if "offer_fingerprint" in row.keys() else None,
                "screenshot_hash": row["screenshot_hash"] if "screenshot_hash" in row.keys() else None,
                "rate_pct": row["rate_pct"] if "rate_pct" in row.keys() else None,
                "dom_text_sample": row["dom_text_sample"] if "dom_text_sample" in row.keys() else None,
                "perceptual_hash": row["perceptual_hash"] if "perceptual_hash" in row.keys() else None,
                "hero_perceptual_hash": row["hero_perceptual_hash"] if "hero_perceptual_hash" in row.keys() else None,
            }
        return out

    def upsert_snapshot(
        self,
        merchant_slug: str,
        source_name: str,
        *,
        cashback_rate: Optional[str],
        offers: List[str],
        confidence: float,
        blocked: bool,
        dom_fingerprint: Optional[str] = None,
        offer_fingerprint: Optional[str] = None,
        screenshot_hash: Optional[str] = None,
        rate_pct: Optional[float] = None,
        dom_text_sample: Optional[str] = None,
        perceptual_hash: Optional[str] = None,
        hero_perceptual_hash: Optional[str] = None,
    ) -> None:
        now = _utc_now()
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT INTO source_snapshots
                (merchant_slug, source_name, cashback_rate, offers_json, confidence, blocked,
                 collected_at, dom_fingerprint, offer_fingerprint, screenshot_hash, rate_pct,
                 dom_text_sample, perceptual_hash, hero_perceptual_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(merchant_slug, source_name) DO UPDATE SET
                    cashback_rate = excluded.cashback_rate,
                    offers_json = excluded.offers_json,
                    confidence = excluded.confidence,
                    blocked = excluded.blocked,
                    collected_at = excluded.collected_at,
                    dom_fingerprint = excluded.dom_fingerprint,
                    offer_fingerprint = excluded.offer_fingerprint,
                    screenshot_hash = excluded.screenshot_hash,
                    rate_pct = excluded.rate_pct,
                    dom_text_sample = excluded.dom_text_sample,
                    perceptual_hash = excluded.perceptual_hash,
                    hero_perceptual_hash = excluded.hero_perceptual_hash
                """,
                (
                    merchant_slug,
                    source_name,
                    cashback_rate,
                    json.dumps(offers),
                    confidence,
                    1 if blocked else 0,
                    now,
                    dom_fingerprint,
                    offer_fingerprint,
                    screenshot_hash,
                    rate_pct,
                    dom_text_sample,
                    perceptual_hash,
                    hero_perceptual_hash,
                ),
            )

    def record_offer_lifecycle(
        self,
        merchant_slug: str,
        changes: List[Dict[str, Any]],
    ) -> None:
        now = _utc_now()
        with self._lock, self._conn() as conn:
            for ch in changes:
                conn.execute(
                    """
                    INSERT INTO offer_lifecycle
                    (merchant_slug, source_name, offer_key, offer_text, change_type, detected_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        merchant_slug,
                        ch.get("source_name"),
                        ch.get("offer_key"),
                        ch.get("offer_text"),
                        ch.get("change"),
                        now,
                    ),
                )

    def get_offer_lifecycle(
        self,
        merchant_slug: str,
        *,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT source_name, offer_key, offer_text, change_type, detected_at
                FROM offer_lifecycle WHERE merchant_slug = ?
                ORDER BY id DESC LIMIT ?
                """,
                (merchant_slug, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def record_crawl_run(
        self,
        merchant_slug: str,
        *,
        offer_count: int,
        event_count: int,
        confidence: float,
        cost_usd: float,
        started_at: str,
    ) -> int:
        now = _utc_now()
        with self._lock, self._conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO crawl_runs
                (merchant_slug, started_at, finished_at, offer_count, event_count, aggregate_confidence, cost_usd)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (merchant_slug, started_at, now, offer_count, event_count, confidence, cost_usd),
            )
            conn.execute(
                """
                INSERT INTO merchant_schedule (merchant_slug, last_crawled_at)
                VALUES (?, ?)
                ON CONFLICT(merchant_slug) DO UPDATE SET last_crawled_at = excluded.last_crawled_at
                """,
                (merchant_slug, now),
            )
            return int(cur.lastrowid)

    def insert_events(self, merchant_slug: str, events: List[Dict[str, Any]]) -> None:
        if not events:
            return
        now = _utc_now()
        with self._lock, self._conn() as conn:
            for ev in events:
                conn.execute(
                    """
                    INSERT INTO intelligence_events
                    (merchant_slug, event_type, source_name, payload_json, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        merchant_slug,
                        ev.get("type", "unknown"),
                        ev.get("source"),
                        json.dumps(ev),
                        now,
                    ),
                )

    def get_recent_events(self, merchant_slug: str, limit: int = 20) -> List[Dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT payload_json FROM intelligence_events
                WHERE merchant_slug = ?
                ORDER BY id DESC LIMIT ?
                """,
                (merchant_slug, limit),
            ).fetchall()
        return [json.loads(r["payload_json"]) for r in rows]

    def set_source_cooldown(
        self,
        source_name: str,
        *,
        minutes: int = 20,
        reason: Optional[str] = None,
    ) -> None:
        until = (datetime.now(timezone.utc) + timedelta(minutes=minutes)).isoformat()
        now = _utc_now()
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT INTO source_reliability (source_name, cooldown_until, last_failure_at)
                VALUES (?, ?, ?)
                ON CONFLICT(source_name) DO UPDATE SET
                    cooldown_until = excluded.cooldown_until,
                    last_failure_at = excluded.last_failure_at
                """,
                (source_name, until, now),
            )

    def is_source_in_cooldown(self, source_name: str) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT cooldown_until FROM source_reliability WHERE source_name = ?",
                (source_name,),
            ).fetchone()
        if not row or not row["cooldown_until"]:
            return False
        try:
            until = datetime.fromisoformat(str(row["cooldown_until"]).replace("Z", "+00:00"))
            return datetime.now(timezone.utc) < until
        except ValueError:
            return False

    def update_source_reliability(self, source_name: str, *, success: bool, blocked: bool) -> float:
        now = _utc_now()
        with self._lock, self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM source_reliability WHERE source_name = ?",
                (source_name,),
            ).fetchone()
            if not row:
                conn.execute(
                    "INSERT INTO source_reliability (source_name) VALUES (?)",
                    (source_name,),
                )
                successes, failures, blocks = 0, 0, 0
            else:
                successes = row["successes"]
                failures = row["failures"]
                blocks = row["blocks"]

            if blocked:
                blocks += 1
                failures += 1
                conn.execute(
                    """
                    UPDATE source_reliability SET blocks=?, failures=?, last_failure_at=?
                    WHERE source_name=?
                    """,
                    (blocks, failures, now, source_name),
                )
            elif success:
                successes += 1
                conn.execute(
                    """
                    UPDATE source_reliability SET successes=?, last_success_at=?
                    WHERE source_name=?
                    """,
                    (successes, now, source_name),
                )
            else:
                failures += 1
                conn.execute(
                    """
                    UPDATE source_reliability SET failures=?, last_failure_at=?
                    WHERE source_name=?
                    """,
                    (failures, now, source_name),
                )

            row = conn.execute(
                "SELECT successes, failures, blocks FROM source_reliability WHERE source_name = ?",
                (source_name,),
            ).fetchone()
            total = (row["successes"] or 0) + (row["failures"] or 0)
            score = (row["successes"] or 0) / total if total else 0.5
            if (row["blocks"] or 0) > 3:
                score *= 0.7
            conn.execute(
                "UPDATE source_reliability SET reliability_score = ? WHERE source_name = ?",
                (round(score, 3), source_name),
            )
            return score

    def get_block_counts(self) -> Dict[str, int]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT source_name, blocks FROM source_reliability"
            ).fetchall()
        return {r["source_name"]: r["blocks"] or 0 for r in rows}

    def get_source_rankings(self) -> List[Tuple[str, float]]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT source_name, reliability_score FROM source_reliability ORDER BY reliability_score DESC"
            ).fetchall()
        if rows:
            return [(r["source_name"], r["reliability_score"]) for r in rows]
        from agents.crawler.crawl.profiles import DEFAULT_COMPETITORS

        return [(n, 0.5) for n in DEFAULT_COMPETITORS]

    def get_last_crawl_time(self, merchant_slug: str) -> Optional[str]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT last_crawled_at FROM merchant_schedule WHERE merchant_slug = ?",
                (merchant_slug,),
            ).fetchone()
        return row["last_crawled_at"] if row else None

    def set_schedule(
        self,
        merchant_slug: str,
        *,
        interval_sec: int,
        hot_until: Optional[str] = None,
        priority_tier: int = 2,
        crawl_mode: Optional[str] = None,
        monitor_reason: Optional[str] = None,
    ) -> None:
        from datetime import timedelta

        next_at = (datetime.now(timezone.utc) + timedelta(seconds=interval_sec)).isoformat()
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT INTO merchant_schedule
                (merchant_slug, priority_tier, next_crawl_at, crawl_interval_sec, hot_until,
                 crawl_mode, monitor_reason)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(merchant_slug) DO UPDATE SET
                    crawl_interval_sec = excluded.crawl_interval_sec,
                    next_crawl_at = excluded.next_crawl_at,
                    hot_until = COALESCE(excluded.hot_until, merchant_schedule.hot_until),
                    priority_tier = excluded.priority_tier,
                    crawl_mode = COALESCE(excluded.crawl_mode, merchant_schedule.crawl_mode),
                    monitor_reason = COALESCE(excluded.monitor_reason, merchant_schedule.monitor_reason)
                """,
                (
                    merchant_slug,
                    priority_tier,
                    next_at,
                    interval_sec,
                    hot_until,
                    crawl_mode,
                    monitor_reason,
                ),
            )

    def get_merchant_schedule(self, merchant_slug: str) -> Optional[Dict[str, Any]]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM merchant_schedule WHERE merchant_slug = ?",
                (merchant_slug,),
            ).fetchone()
        return dict(row) if row else None

    def list_schedules(self) -> Dict[str, Dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute("SELECT * FROM merchant_schedule").fetchall()
        return {r["merchant_slug"]: dict(r) for r in rows}

    def is_in_hot_window(self, merchant_slug: str) -> bool:
        row = self.get_merchant_schedule(merchant_slug)
        if not row or not row.get("hot_until"):
            return False
        try:
            hot_until = datetime.fromisoformat(str(row["hot_until"]).replace("Z", "+00:00"))
            return datetime.now(timezone.utc) < hot_until
        except ValueError:
            return False

    def set_intelligence_feedback(
        self,
        merchant_slug: str,
        *,
        analyst_risk: Optional[str] = None,
        strategist_priority: Optional[str] = None,
    ) -> None:
        updates: List[str] = []
        params: List[Any] = []
        if analyst_risk is not None:
            updates.append("analyst_risk = ?")
            params.append(analyst_risk.upper())
        if strategist_priority is not None:
            updates.append("strategist_priority = ?")
            params.append(strategist_priority.upper())
        if not updates:
            return
        params.append(merchant_slug)
        with self._lock, self._conn() as conn:
            conn.execute(
                f"INSERT INTO merchant_schedule (merchant_slug) VALUES (?) "
                f"ON CONFLICT(merchant_slug) DO NOTHING",
                (merchant_slug,),
            )
            conn.execute(
                f"UPDATE merchant_schedule SET {', '.join(updates)} WHERE merchant_slug = ?",
                params,
            )

    def is_merchant_due(self, merchant_slug: str) -> bool:
        if self.is_in_hot_window(merchant_slug):
            row = self.get_merchant_schedule(merchant_slug)
            if row and row.get("last_crawled_at"):
                try:
                    last = datetime.fromisoformat(str(row["last_crawled_at"]).replace("Z", "+00:00"))
                    interval = int(row.get("crawl_interval_sec") or 300)
                    return datetime.now(timezone.utc) >= last + timedelta(seconds=interval)
                except ValueError:
                    pass
            return True
        with self._conn() as conn:
            row = conn.execute(
                "SELECT last_crawled_at, next_crawl_at FROM merchant_schedule WHERE merchant_slug = ?",
                (merchant_slug,),
            ).fetchone()
        if row is None or not row["last_crawled_at"]:
            return True
        if not row["next_crawl_at"]:
            return True
        try:
            next_dt = datetime.fromisoformat(row["next_crawl_at"].replace("Z", "+00:00"))
            return datetime.now(timezone.utc) >= next_dt
        except ValueError:
            return True

    def enqueue_crawl(self, merchant_slug: str, priority: int = 5) -> int:
        return self._queue.enqueue_crawl(merchant_slug, priority)

    def reclaim_stale_tasks(self, *, stale_minutes: Optional[int] = None) -> int:
        return self._queue.reclaim_stale_tasks(stale_minutes=stale_minutes)

    def claim_tasks(self, worker_id: str, limit: int = 3) -> List[Dict[str, Any]]:
        return self._queue.claim_tasks(worker_id, limit)

    def get_queue_stats(self) -> Dict[str, int]:
        return self._queue.get_queue_stats()

    def complete_task(self, task_id: int, result: Dict[str, Any], *, failed: bool = False) -> None:
        return self._queue.complete_task(task_id, result, failed=failed)

    def _enqueue_crawl_sqlite(self, merchant_slug: str, priority: int = 5) -> int:
        """Enqueue merchant crawl; skips if same slug already pending/running."""
        now = _utc_now()
        slug = merchant_slug.lower().strip()
        with self._lock, self._conn() as conn:
            row = conn.execute(
                """
                SELECT id FROM crawl_tasks
                WHERE merchant_slug = ? AND status IN ('pending', 'running')
                ORDER BY priority ASC, id ASC LIMIT 1
                """,
                (slug,),
            ).fetchone()
            if row:
                return int(row["id"])
            cur = conn.execute(
                """
                INSERT INTO crawl_tasks (merchant_slug, status, priority, created_at)
                VALUES (?, 'pending', ?, ?)
                """,
                (slug, priority, now),
            )
            return int(cur.lastrowid)

    def _reclaim_stale_tasks_sqlite(self, *, stale_minutes: Optional[int] = None) -> int:
        """Return running tasks stuck past stale window to pending (multi-worker safety)."""
        minutes = stale_minutes or int(os.getenv("CRAWLER_TASK_STALE_MINUTES", "45"))
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
        with self._lock, self._conn() as conn:
            cur = conn.execute(
                """
                UPDATE crawl_tasks
                SET status = 'pending', worker_id = NULL, started_at = NULL
                WHERE status = 'running' AND started_at IS NOT NULL AND started_at < ?
                """,
                (cutoff,),
            )
            return int(cur.rowcount or 0)

    def _claim_tasks_sqlite(self, worker_id: str, limit: int = 3) -> List[Dict[str, Any]]:
        """Atomically claim pending tasks (one UPDATE per row to avoid duplicate work)."""
        now = _utc_now()
        claimed: List[Dict[str, Any]] = []
        with self._lock, self._conn() as conn:
            rows = conn.execute(
                """
                SELECT id, merchant_slug, priority FROM crawl_tasks
                WHERE status = 'pending'
                ORDER BY priority ASC, id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            for row in rows:
                cur = conn.execute(
                    """
                    UPDATE crawl_tasks
                    SET status = 'running', worker_id = ?, started_at = ?
                    WHERE id = ? AND status = 'pending'
                    """,
                    (worker_id, now, row["id"]),
                )
                if cur.rowcount:
                    claimed.append(dict(row))
        return claimed

    def _get_queue_stats_sqlite(self) -> Dict[str, int]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT status, COUNT(*) AS c FROM crawl_tasks GROUP BY status
                """
            ).fetchall()
        counts = {r["status"]: int(r["c"]) for r in rows}
        return {
            "pending": counts.get("pending", 0),
            "running": counts.get("running", 0),
            "done": counts.get("done", 0),
            "failed": counts.get("failed", 0),
            "total": sum(counts.values()),
        }

    def get_crawl_run_stats(self, *, hours: float = 24.0) -> Dict[str, Any]:
        since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS c,
                       COALESCE(SUM(cost_usd), 0) AS cost,
                       COALESCE(SUM(event_count), 0) AS events
                FROM crawl_runs WHERE finished_at >= ?
                """,
                (since,),
            ).fetchone()
        if not row:
            return {"count": 0, "cost_usd": 0.0, "events": 0}
        return {
            "count": int(row["c"] or 0),
            "cost_usd": round(float(row["cost"] or 0), 4),
            "events": int(row["events"] or 0),
        }

    def get_event_stats(self, *, hours: float = 24.0) -> Dict[str, Any]:
        since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS c FROM intelligence_events WHERE created_at >= ?
                """,
                (since,),
            ).fetchone()
        return {"count": int(row["c"] or 0) if row else 0}

    def _complete_task_sqlite(self, task_id: int, result: Dict[str, Any], *, failed: bool = False) -> None:
        now = _utc_now()
        status = "failed" if failed else "done"
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                UPDATE crawl_tasks SET status=?, finished_at=?, result_json=?
                WHERE id=?
                """,
                (status, now, json.dumps(result), task_id),
            )

    def get_merchant_timeline(self, merchant_slug: str, limit: int = 10) -> List[Dict[str, Any]]:
        with self._conn() as conn:
            runs = conn.execute(
                """
                SELECT started_at, offer_count, event_count, aggregate_confidence, cost_usd
                FROM crawl_runs WHERE merchant_slug = ? ORDER BY id DESC LIMIT ?
                """,
                (merchant_slug, limit),
            ).fetchall()
        return [dict(r) for r in runs]

    # --- Crawler memory ---

    def default_selector_chain(self, source_name: str) -> List[str]:
        from agents.crawler.crawl.self_healing import DEFAULT_SELECTOR_CHAINS
        return list(DEFAULT_SELECTOR_CHAINS.get(source_name, DEFAULT_SELECTOR_CHAINS["default"]))

    def get_parser_memory(self, merchant_slug: str, source_name: str) -> Optional[Dict[str, Any]]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM parser_memory WHERE merchant_slug=? AND source_name=?",
                (merchant_slug, source_name),
            ).fetchone()
        return dict(row) if row else None

    def upsert_parser_memory(
        self,
        merchant_slug: str,
        source_name: str,
        *,
        selector_used: Optional[str],
        dom_fingerprint: str,
        success: bool,
        offer_count: int,
    ) -> None:
        now = _utc_now()
        with self._lock, self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM parser_memory WHERE merchant_slug=? AND source_name=?",
                (merchant_slug, source_name),
            ).fetchone()
            chain: List[str] = self.default_selector_chain(source_name)
            successes, fails = 0, 0
            if row:
                successes = row["success_count"] or 0
                fails = row["fail_count"] or 0
                if row["selector_chain_json"]:
                    try:
                        chain = json.loads(row["selector_chain_json"])
                    except json.JSONDecodeError:
                        pass
            if success:
                successes += 1
                if selector_used and selector_used not in chain:
                    chain.insert(0, selector_used)
            else:
                fails += 1
            conn.execute(
                """
                INSERT INTO parser_memory
                (merchant_slug, source_name, selector_chain_json, last_selector,
                 last_dom_fingerprint, success_count, fail_count, last_offer_count, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(merchant_slug, source_name) DO UPDATE SET
                    selector_chain_json=excluded.selector_chain_json,
                    last_selector=excluded.last_selector,
                    last_dom_fingerprint=excluded.last_dom_fingerprint,
                    success_count=excluded.success_count,
                    fail_count=excluded.fail_count,
                    last_offer_count=excluded.last_offer_count,
                    updated_at=excluded.updated_at
                """,
                (
                    merchant_slug,
                    source_name,
                    json.dumps(chain[:20]),
                    selector_used,
                    dom_fingerprint,
                    successes,
                    fails,
                    offer_count,
                    now,
                ),
            )

    def record_dom_history(
        self,
        merchant_slug: str,
        source_name: str,
        *,
        dom_fingerprint: str,
        offer_count: int,
    ) -> None:
        now = _utc_now()
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT INTO dom_history (merchant_slug, source_name, dom_fingerprint, offer_count, recorded_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (merchant_slug, source_name, dom_fingerprint, offer_count, now),
            )

    def get_merchant_memory(self, merchant_slug: str) -> Optional[Dict[str, Any]]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM merchant_memory WHERE merchant_slug=?",
                (merchant_slug,),
            ).fetchone()
        return dict(row) if row else None

    def upsert_merchant_memory(
        self,
        merchant_slug: str,
        *,
        category: str,
        volatility_score: float,
        behavior_json: str,
        anti_bot_json: str = "[]",
    ) -> None:
        now = _utc_now()
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT INTO merchant_memory (merchant_slug, category, volatility_score, anti_bot_json, behavior_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(merchant_slug) DO UPDATE SET
                    category=excluded.category,
                    volatility_score=excluded.volatility_score,
                    behavior_json=excluded.behavior_json,
                    updated_at=excluded.updated_at
                """,
                (merchant_slug, category, volatility_score, anti_bot_json, behavior_json, now),
            )

    def get_offer_states(self, merchant_slug: str, source_name: str) -> Dict[str, Dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM offer_state WHERE merchant_slug=? AND source_name=?",
                (merchant_slug, source_name),
            ).fetchall()
        return {r["offer_key"]: dict(r) for r in rows}

    def upsert_offer_state(
        self,
        merchant_slug: str,
        source_name: str,
        offer_key: str,
        offer_text: str,
        *,
        stage: str,
        mention_count: int,
    ) -> None:
        now = _utc_now()
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT INTO offer_state
                (merchant_slug, source_name, offer_key, offer_text, stage, mention_count, first_seen_at, last_seen_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(merchant_slug, source_name, offer_key) DO UPDATE SET
                    offer_text=excluded.offer_text,
                    stage=excluded.stage,
                    mention_count=excluded.mention_count,
                    last_seen_at=excluded.last_seen_at
                """,
                (merchant_slug, source_name, offer_key, offer_text, stage, mention_count, now, now),
            )

    def record_rate_sample(
        self,
        merchant_slug: str,
        source_name: Optional[str],
        rate_pct: float,
    ) -> None:
        now = _utc_now()
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT INTO rate_samples (merchant_slug, source_name, rate_pct, recorded_at)
                VALUES (?, ?, ?, ?)
                """,
                (merchant_slug, source_name, rate_pct, now),
            )

    def get_rate_history(self, merchant_slug: str, limit: int = 50) -> List[Dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT rate_pct, source_name, recorded_at FROM rate_samples
                WHERE merchant_slug=? ORDER BY id DESC LIMIT ?
                """,
                (merchant_slug, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    # --- Merchant registry ---

    def upsert_merchant(
        self,
        merchant_slug: str,
        *,
        display_name: str,
        category: Optional[str] = None,
        tier: int = 2,
        enabled: bool = True,
        client_priority: int = 0,
        promoted_until: Optional[str] = None,
    ) -> None:
        now = _utc_now()
        slug = merchant_slug.lower().strip()
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT INTO merchants
                (merchant_slug, display_name, category, tier, enabled, client_priority,
                 promoted_until, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(merchant_slug) DO UPDATE SET
                    display_name = excluded.display_name,
                    category = COALESCE(excluded.category, merchants.category),
                    tier = excluded.tier,
                    enabled = excluded.enabled,
                    client_priority = excluded.client_priority,
                    promoted_until = COALESCE(excluded.promoted_until, merchants.promoted_until),
                    updated_at = excluded.updated_at
                """,
                (
                    slug,
                    display_name,
                    category,
                    tier,
                    1 if enabled else 0,
                    client_priority,
                    promoted_until,
                    now,
                    now,
                ),
            )

    def list_merchants(self, *, enabled_only: bool = False) -> List[Dict[str, Any]]:
        with self._conn() as conn:
            if enabled_only:
                rows = conn.execute(
                    "SELECT * FROM merchants WHERE enabled = 1 ORDER BY tier ASC, display_name ASC"
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM merchants ORDER BY tier ASC, display_name ASC"
                ).fetchall()
        return [dict(r) for r in rows]

    def get_merchant(self, merchant_slug: str) -> Optional[Dict[str, Any]]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM merchants WHERE merchant_slug = ?",
                (merchant_slug.lower().strip(),),
            ).fetchone()
        return dict(row) if row else None

    def set_merchant_enabled(self, merchant_slug: str, enabled: bool) -> None:
        now = _utc_now()
        with self._lock, self._conn() as conn:
            conn.execute(
                "UPDATE merchants SET enabled = ?, updated_at = ? WHERE merchant_slug = ?",
                (1 if enabled else 0, now, merchant_slug.lower().strip()),
            )

    def promote_merchant(self, merchant_slug: str, *, hours: int = 24) -> None:
        until = (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()
        now = _utc_now()
        slug = merchant_slug.lower().strip()
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                UPDATE merchants SET promoted_until = ?, updated_at = ?
                WHERE merchant_slug = ?
                """,
                (until, now, slug),
            )

    def is_merchant_promoted(self, merchant_slug: str) -> bool:
        row = self.get_merchant(merchant_slug)
        if not row or not row.get("promoted_until"):
            return False
        try:
            until = datetime.fromisoformat(str(row["promoted_until"]).replace("Z", "+00:00"))
            return datetime.now(timezone.utc) < until
        except ValueError:
            return False

    def increment_dormant_count(self, merchant_slug: str) -> int:
        now = _utc_now()
        slug = merchant_slug.lower().strip()
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT INTO merchants (merchant_slug, display_name, tier, enabled, dormant_count, created_at, updated_at)
                VALUES (?, ?, 2, 1, 1, ?, ?)
                ON CONFLICT(merchant_slug) DO UPDATE SET
                    dormant_count = merchants.dormant_count + 1,
                    updated_at = excluded.updated_at
                """,
                (slug, slug.title(), now, now),
            )
            row = conn.execute(
                "SELECT dormant_count FROM merchants WHERE merchant_slug = ?",
                (slug,),
            ).fetchone()
        return int(row["dormant_count"] or 0) if row else 0

    def get_hourly_crawl_cost(self, hours: float = 1.0) -> float:
        since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT COALESCE(SUM(cost_usd), 0) AS total FROM crawl_runs
                WHERE finished_at >= ?
                """,
                (since,),
            ).fetchone()
        return float(row["total"] or 0.0) if row else 0.0

    def record_budget_event(
        self,
        event_type: str,
        *,
        merchant_slug: Optional[str] = None,
        cost_usd: float = 0.0,
    ) -> None:
        now = _utc_now()
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT INTO budget_events (event_type, merchant_slug, cost_usd, recorded_at)
                VALUES (?, ?, ?, ?)
                """,
                (event_type, merchant_slug, cost_usd, now),
            )

    def count_budget_events(
        self,
        event_type: str,
        *,
        merchant_slug: Optional[str] = None,
        hours: float = 24.0,
    ) -> int:
        since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        with self._conn() as conn:
            if merchant_slug:
                row = conn.execute(
                    """
                    SELECT COUNT(*) AS c FROM budget_events
                    WHERE event_type = ? AND merchant_slug = ? AND recorded_at >= ?
                    """,
                    (event_type, merchant_slug.lower().strip(), since),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT COUNT(*) AS c FROM budget_events
                    WHERE event_type = ? AND recorded_at >= ?
                    """,
                    (event_type, since),
                ).fetchone()
        return int(row["c"] or 0) if row else 0

    def has_baseline(self, merchant_slug: str) -> bool:
        """True after at least one completed intelligence run for this merchant."""
        slug = merchant_slug.lower().strip()
        with self._conn() as conn:
            run = conn.execute(
                "SELECT 1 FROM crawl_runs WHERE merchant_slug = ? LIMIT 1",
                (slug,),
            ).fetchone()
            if run:
                return True
            snap = conn.execute(
                "SELECT 1 FROM source_snapshots WHERE merchant_slug = ? LIMIT 1",
                (slug,),
            ).fetchone()
        return snap is not None

    def record_analyst_output(self, merchant_slug: str, output: Dict[str, Any]) -> None:
        """Persist a finalized analyst run for timeline / prompt history."""
        slug = merchant_slug.lower().strip()
        now = _utc_now()
        slim = {
            k: output.get(k)
            for k in (
                "risk_level",
                "gap_summary",
                "recommended_action",
                "confidence",
                "trend",
                "response_probability",
                "predicted_competitor_move",
                "competitor_advantage_pct",
                "gap_found",
                "analysis_mode",
            )
            if output.get(k) is not None
        }
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT INTO analyst_outputs (merchant_slug, output_json, created_at)
                VALUES (?, ?, ?)
                """,
                (slug, json.dumps(slim), now),
            )

    def get_analyst_history(
        self, merchant_slug: str, limit: int = 3
    ) -> List[Dict[str, Any]]:
        """Recent analyst outputs for a merchant (newest first)."""
        slug = merchant_slug.lower().strip()
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT output_json, created_at FROM analyst_outputs
                WHERE merchant_slug = ?
                ORDER BY id DESC LIMIT ?
                """,
                (slug, limit),
            ).fetchall()
        out: List[Dict[str, Any]] = []
        for row in rows:
            try:
                payload = json.loads(row["output_json"])
            except json.JSONDecodeError:
                continue
            payload["recorded_at"] = row["created_at"]
            out.append(payload)
        return out

    def record_analyst_shadow_delta(
        self,
        merchant_slug: str,
        *,
        primary_risk: str,
        shadow_risk: str,
        shadow_model: Optional[str] = None,
        delta: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log primary vs shadow risk mismatch for calibration."""
        slug = merchant_slug.lower().strip()
        now = _utc_now()
        delta_payload = delta or {"primary": primary_risk, "shadow": shadow_risk}
        try:
            with self._lock, self._conn() as conn:
                conn.execute(
                    """
                    INSERT INTO analyst_shadow_deltas
                    (merchant_slug, primary_risk, shadow_risk, shadow_model, delta_json, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        slug,
                        primary_risk.upper(),
                        shadow_risk.upper(),
                        shadow_model,
                        json.dumps(delta_payload),
                        now,
                    ),
                )
        except sqlite3.Error:
            self._append_analyst_shadow_jsonl(
                slug, primary_risk, shadow_risk, shadow_model, delta_payload, now
            )

    @staticmethod
    def _append_analyst_shadow_jsonl(
        merchant_slug: str,
        primary_risk: str,
        shadow_risk: str,
        shadow_model: Optional[str],
        delta: Dict[str, Any],
        created_at: str,
    ) -> None:
        path = os.path.join("logs", "analyst_shadow.jsonl")
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        row = {
            "merchant_slug": merchant_slug,
            "primary_risk": primary_risk,
            "shadow_risk": shadow_risk,
            "shadow_model": shadow_model,
            "delta": delta,
            "created_at": created_at,
        }
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(row) + "\n")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
