"""Meaningful crawler unit tests (no Playwright)."""
from __future__ import annotations

import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from agents.crawler.agent import CrawlerAgent
from agents.crawler.crawl.antibot import is_captcha_text
from agents.crawler.extraction.evidence_validator import EvidenceValidator
from agents.crawler.intelligence.change_detection.engine import is_cashback_spike
from agents.crawler.intelligence.consensus.validator import ConsensusValidator
from agents.crawler.platform.store import IntelligenceStore
from agents.crawler.scheduling.actions import ActionType
from agents.crawler.platform.metrics import format_prometheus, get_crawler_metrics
from agents.crawler.platform.queue import RedisQueueBackend
from agents.crawler.platform.session import (
    get_credentials,
    load_storage_state,
    save_storage_state,
    session_path,
)
from agents.crawler.scheduling.policy import AdaptiveCrawlPolicy, INTERVAL_CRITICAL


class SpikeDetectionTests(unittest.TestCase):
    def test_spike_points_threshold(self):
        self.assertTrue(is_cashback_spike(5.0, 12.0))
        self.assertFalse(is_cashback_spike(5.0, 5.5))

    def test_spike_ratio_with_min_delta(self):
        self.assertTrue(is_cashback_spike(10.0, 13.0))
        self.assertFalse(is_cashback_spike(10.0, 10.5))

    def test_no_spike_on_drop(self):
        self.assertFalse(is_cashback_spike(12.0, 5.0))


class ConsensusValidatorTests(unittest.TestCase):
    def setUp(self):
        self.validator = ConsensusValidator()

    def test_weighted_consensus_ignores_blocked(self):
        sources = [
            {
                "source_name": "A",
                "cashback_rate": "10%",
                "reliability_score": 0.9,
                "extraction": {"confidence": 0.9},
            },
            {
                "source_name": "B",
                "cashback_rate": "12%",
                "reliability_score": 0.8,
                "extraction": {"confidence": 0.8},
            },
            {"source_name": "C", "blocked": True, "cashback_rate": "50%"},
        ]
        out = self.validator.validate(sources)
        self.assertAlmostEqual(out["consensus_rate"], 10.89, places=1)
        self.assertEqual(out["source_count"], 2)

    def test_contradiction_when_outlier(self):
        sources = [
            {
                "source_name": "trusted",
                "cashback_rate": "5%",
                "reliability_score": 0.9,
                "extraction": {"confidence": 0.9},
            },
            {
                "source_name": "outlier",
                "cashback_rate": "25%",
                "reliability_score": 0.9,
                "extraction": {"confidence": 0.9},
            },
        ]
        out = self.validator.validate(sources)
        self.assertGreater(len(out["contradictions"]), 0)


class EvidenceValidatorTests(unittest.TestCase):
    def setUp(self):
        self.validator = EvidenceValidator()

    def test_impossible_dom_rate_downgrades(self):
        sources = [
            {
                "source_name": "x",
                "cashback_rate": "45%",
                "extraction": {"confidence": 0.9},
            }
        ]
        events, summary = self.validator.validate(
            "myntra", sources, {"consensus_rate": 8.0}
        )
        self.assertEqual(summary["mismatches"], 1)
        self.assertTrue(sources[0].get("requires_revalidation"))
        self.assertTrue(any(e["reason"] == "impossible_dom_rate" for e in events))

    def test_dom_consensus_mismatch(self):
        os.environ["CRAWLER_MAX_CASHBACK_PCT"] = "30"
        sources = [
            {
                "source_name": "a",
                "cashback_rate": "15%",
                "extraction": {"confidence": 0.8},
            }
        ]
        events, _ = self.validator.validate(
            "flipkart", sources, {"consensus_rate": 5.0}
        )
        self.assertTrue(any(e.get("reason") == "dom_vs_consensus" for e in events))


class PolicyActTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.store = IntelligenceStore(db_path=self.tmp.name)
        self.policy = AdaptiveCrawlPolicy(self.store)

    def tearDown(self):
        try:
            os.unlink(self.tmp.name)
        except OSError:
            pass

    def test_critical_spike_triggers_full_scrape_and_sweep(self):
        payload = {
            "events": [{"type": "cashback_spike_detected", "delta": 7}],
            "sweep": {},
        }
        actions = self.policy.act("myntra", payload)
        types = {a.action for a in actions}
        self.assertIn(ActionType.FULL_SCRAPE, types)
        self.assertIn(ActionType.SET_CRITICAL, types)
        self.assertIn(ActionType.MARKET_SWEEP, types)

    def test_anomaly_triggers_recrwal(self):
        payload = {
            "events": [],
            "anomaly": {"requires_revalidation": True},
        }
        actions = self.policy.act("ajio", payload)
        self.assertTrue(any(a.action == ActionType.RECRAWL_IN for a in actions))

    def test_quiet_crawl_only(self):
        actions = self.policy.act("nykaa", {"events": [], "sweep": {}})
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0].action, ActionType.CRAWL_ONLY)


class TaskQueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.store = IntelligenceStore(db_path=self.tmp.name)

    def tearDown(self):
        try:
            os.unlink(self.tmp.name)
        except OSError:
            pass

    def test_enqueue_dedupes_pending(self):
        a = self.store.enqueue_crawl("myntra", priority=3)
        b = self.store.enqueue_crawl("myntra", priority=1)
        self.assertEqual(a, b)
        stats = self.store.get_queue_stats()
        self.assertEqual(stats["pending"], 1)

    def test_atomic_claim_prevents_double_claim(self):
        self.store.enqueue_crawl("flipkart")
        w1 = self.store.claim_tasks("worker-a", limit=1)
        w2 = self.store.claim_tasks("worker-b", limit=1)
        self.assertEqual(len(w1), 1)
        self.assertEqual(len(w2), 0)


class SessionTests(unittest.TestCase):
    def setUp(self):
        self._prev = os.environ.get("CRAWLER_LOGIN_ENABLED")
        os.environ["CRAWLER_LOGIN_ENABLED"] = "true"

    def tearDown(self):
        if self._prev is None:
            os.environ.pop("CRAWLER_LOGIN_ENABLED", None)
        else:
            os.environ["CRAWLER_LOGIN_ENABLED"] = self._prev

    def test_storage_state_roundtrip(self):
        with tempfile.TemporaryDirectory() as session_dir:
            with patch("agents.crawler.platform.session.SESSION_DIR", session_dir):
                state = {"cookies": [], "origins": []}
                save_storage_state("myntra", state)
                loaded = load_storage_state("myntra")
                self.assertEqual(loaded, state)
                self.assertTrue(os.path.isfile(session_path("myntra")))

    def test_credentials_per_slug_and_generic(self):
        os.environ["CRAWLER_LOGIN_MYNTRA_USER"] = "u1"
        os.environ["CRAWLER_LOGIN_MYNTRA_PASS"] = "p1"
        creds = get_credentials("myntra")
        self.assertEqual(creds, ("u1", "p1"))
        os.environ.pop("CRAWLER_LOGIN_MYNTRA_USER", None)
        os.environ.pop("CRAWLER_LOGIN_MYNTRA_PASS", None)
        os.environ["CRAWLER_LOGIN_USER"] = "generic"
        os.environ["CRAWLER_LOGIN_PASS"] = "secret"
        self.assertEqual(get_credentials("ajio"), ("generic", "secret"))


class RedisQueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.store = IntelligenceStore(db_path=self.tmp.name)

    def tearDown(self):
        try:
            os.unlink(self.tmp.name)
        except OSError:
            pass

    def test_redis_enqueue_claim_complete(self):
        mock_redis = MagicMock()
        mock_redis.ping.return_value = True
        mock_redis.incr.return_value = 1
        mock_redis.get.return_value = None
        pipe = MagicMock()
        mock_redis.pipeline.return_value = pipe

        def zadd_side_effect(key, mapping):
            mock_redis._pending = list(mapping.keys())

        pipe.execute.side_effect = lambda: None
        mock_redis.zrange.return_value = ["1"]
        def _hget(key, field):
            data = {
                "merchant_slug": "flipkart",
                "status": "pending",
                "priority": "3",
            }
            if field == "claim_lock":
                return None
            return data.get(field)

        mock_redis.hget.side_effect = _hget
        mock_redis.hsetnx.return_value = 1
        mock_redis.hgetall.return_value = {
            "merchant_slug": "flipkart",
            "priority": "3",
        }

        with patch("redis.from_url", return_value=mock_redis):
            backend = RedisQueueBackend("redis://localhost:6379/0", self.store)
            tid = backend.enqueue_crawl("flipkart", priority=3)
            self.assertEqual(tid, 1)
            pipe.set.assert_called()
            claimed = backend.claim_tasks("worker-x", limit=1)
            self.assertEqual(len(claimed), 1)
            self.assertEqual(claimed[0]["merchant_slug"], "flipkart")
            backend.complete_task(1, {"ok": True})
            mock_redis.delete.assert_called()


class MetricsFormatTests(unittest.TestCase):
    def test_prometheus_gauges(self):
        text = format_prometheus(
            {
                "queue": {"pending": 2, "running": 1, "done": 5, "failed": 0},
                "budget": {"hourly_spend_usd": 0.5, "hourly_limit_usd": 2.0, "conserve": False},
                "crawl_runs_24h": {"count": 3, "cost_usd": 0.1},
                "events_24h": {"count": 7},
                "merchants_enabled": 4,
            }
        )
        self.assertIn("crawler_queue_pending 2", text)
        self.assertIn("crawler_merchants_enabled 4", text)

    def test_get_crawler_metrics_sqlite_backend(self):
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        try:
            store = IntelligenceStore(db_path=tmp.name)
            m = get_crawler_metrics(store)
            self.assertEqual(m["queue_backend"], "sqlite")
        finally:
            os.unlink(tmp.name)


class PipelineFeedbackTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.agent = CrawlerAgent(store=IntelligenceStore(db_path=self.tmp.name))

    def tearDown(self):
        try:
            os.unlink(self.tmp.name)
        except OSError:
            pass

    def test_high_risk_escalates_schedule(self):
        decision = self.agent.apply_pipeline_feedback(
            "myntra",
            analyst_risk="HIGH",
            strategist_priority="LOW",
        )
        self.assertIsNotNone(decision)
        self.assertEqual(decision.mode, "critical")
        self.assertEqual(decision.interval_sec, INTERVAL_CRITICAL)
        row = self.agent.store.get_merchant_schedule("myntra")
        self.assertEqual(row["analyst_risk"], "HIGH")
        self.assertEqual(row["crawl_mode"], "critical")

    def test_accepts_full_pipeline_dicts(self):
        decision = self.agent.apply_pipeline_feedback(
            "ajio",
            crawler_data={},
            analysis={"risk_level": "MEDIUM"},
            strategy={"priority": "MEDIUM"},
        )
        self.assertEqual(decision.mode, "hot")


class AntibotCaptchaTests(unittest.TestCase):
    def test_is_captcha_text(self):
        self.assertTrue(is_captcha_text("Please complete the captcha challenge"))
        self.assertFalse(is_captcha_text("10% cashback on fashion"))


if __name__ == "__main__":
    unittest.main()
