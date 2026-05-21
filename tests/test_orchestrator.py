"""Unit tests for SwarmOrchestrator control plane."""
from __future__ import annotations

import asyncio
import json
import os
import tempfile
import unittest
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, MagicMock, patch

from agents.crawler.agent import CrawlerAgent
from agents.crawler.platform.store import IntelligenceStore
from messaging.schemas import AgentMessage, AgentRole, MessageType, Payload
from orchestrator.orchestrator import SwarmOrchestrator
from orchestrator.webhook import enqueue_webhook_event
from state.state_manager import SharedState


class MockAgent:
    def __init__(
        self,
        role: AgentRole,
        *,
        output: Optional[Dict[str, Any]] = None,
        cost: float = 0.01,
        fail_times: int = 0,
    ):
        self.role = role
        self.output = output or {}
        self.cost = cost
        self.fail_times = fail_times
        self.calls = 0

    async def handle_request(self, message: AgentMessage) -> AgentMessage:
        self.calls += 1
        if self.fail_times > 0:
            self.fail_times -= 1
            raise RuntimeError(f"{self.role.value} simulated failure")
        data = dict(self.output)
        if self.role == AgentRole.CRAWLER:
            inp = message.payload.data.get("input", "")
            if isinstance(inp, str) and "REVISE" in inp:
                data.setdefault("merchant", "Myntra")
        return AgentMessage(
            message_id="resp",
            sender=self.role,
            receiver=AgentRole.ORCHESTRATOR,
            message_type=MessageType.RESPONSE,
            payload=Payload(data=data),
            cost=self.cost,
        )


def _valid_intel() -> Dict[str, Any]:
    return {
        "merchant": "Myntra",
        "merchant_slug": "myntra",
        "collected_at": "2026-01-01T00:00:00+00:00",
        "aggregate_confidence": 0.8,
        "offers": ["10% cashback"],
        "sources": [],
        "events": [{"type": "cashback_spike_detected"}],
        "summary": "test",
    }


class TestSwarmOrchestrator(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        os.makedirs("logs", exist_ok=True)

    def _orch(self, budget: float = 1.0) -> SwarmOrchestrator:
        return SwarmOrchestrator(SharedState(), budget_limit=budget)

    async def test_full_pipeline_success(self):
        orch = self._orch()
        orch.register_agent(
            AgentRole.CRAWLER,
            MockAgent(AgentRole.CRAWLER, output=_valid_intel(), cost=0.02),
        )
        orch.register_agent(
            AgentRole.ANALYST,
            MockAgent(
                AgentRole.ANALYST,
                output={"risk_level": "HIGH", "competitor_rate": "12%", "client_rate": "5%"},
            ),
        )
        orch.register_agent(
            AgentRole.STRATEGIST,
            MockAgent(AgentRole.STRATEGIST, output={"priority": "HIGH", "recommendation": "Match rate"}),
        )
        orch.register_agent(AgentRole.ALERTER, MockAgent(AgentRole.ALERTER, output={"sent": True}))

        result = await orch.run_pipeline("Analyze Myntra coupons")
        self.assertTrue(result.success)
        self.assertEqual(result.alerts_sent, 1)
        self.assertGreater(result.total_cost, 0)
        self.assertIn("myntra", orch.state.get_data("competitor_data"))

    async def test_budget_exceeded(self):
        orch = self._orch(budget=0.001)
        orch.total_cost = 0.001
        orch.register_agent(
            AgentRole.CRAWLER,
            MockAgent(AgentRole.CRAWLER, output=_valid_intel(), cost=0.05),
        )
        orch.register_agent(AgentRole.ANALYST, MockAgent(AgentRole.ANALYST, output={"risk_level": "LOW"}))
        result = await orch.run_pipeline("Analyze Myntra")
        self.assertFalse(result.success)
        self.assertIn("Budget", result.error or "")

    async def test_retry_on_failure(self):
        orch = self._orch()
        orch.max_retries = 2
        orch.retry_backoff_sec = 0.01
        orch.register_agent(
            AgentRole.CRAWLER,
            MockAgent(AgentRole.CRAWLER, output=_valid_intel(), cost=0.01, fail_times=1),
        )
        orch.register_agent(
            AgentRole.ANALYST,
            MockAgent(AgentRole.ANALYST, output={"risk_level": "LOW"}),
        )
        orch.register_agent(
            AgentRole.STRATEGIST,
            MockAgent(AgentRole.STRATEGIST, output={"priority": "LOW", "recommendation": "Hold"}),
        )
        orch.register_agent(AgentRole.ALERTER, MockAgent(AgentRole.ALERTER))

        result = await orch.run_pipeline("Analyze Myntra")
        self.assertTrue(result.success)
        crawler = orch.agents[AgentRole.CRAWLER]
        self.assertEqual(crawler.calls, 2)

    async def test_conflict_resolution(self):
        orch = self._orch()
        analysis = {"risk_level": "HIGH", "competitor_rate": "15%", "client_rate": "5%"}
        strategy = {"priority": "LOW", "recommendation": "Wait"}
        resolved = await orch._handle_conflicts(analysis, strategy)
        self.assertEqual(resolved["priority"], "HIGH")
        self.assertIn("REVISED", resolved["recommendation"])
        conflicts = orch.state.get_data("conflicts")
        self.assertIsInstance(conflicts, dict)

    async def test_should_run_full_pipeline_on_critical_event(self):
        orch = self._orch()
        self.assertTrue(
            orch.should_run_full_pipeline(
                events=[{"type": "cashback_spike_detected"}],
                crawl_mode="normal",
            )
        )

    async def test_stream_critical_enqueues_hot_pipeline(self):
        orch = self._orch()
        mock_crawler = AsyncMock()
        mock_crawler.collect_intelligence = AsyncMock(return_value=_valid_intel())
        orch.register_agent(AgentRole.CRAWLER, mock_crawler)
        orch.register_agent(AgentRole.ANALYST, MockAgent(AgentRole.ANALYST, output={"risk_level": "HIGH"}))
        orch.register_agent(
            AgentRole.STRATEGIST,
            MockAgent(AgentRole.STRATEGIST, output={"priority": "HIGH", "recommendation": "Act"}),
        )
        orch.register_agent(AgentRole.ALERTER, MockAgent(AgentRole.ALERTER))

        with patch.object(orch, "_commit_intelligence_to_state"):
            await orch._handle_stream_event(
                {"type": "cashback_spike_detected", "merchant": "myntra", "source": "test"}
            )
            envelope = await asyncio.wait_for(orch._hot_queue.get(), timeout=2.0)
            self.assertEqual(envelope["type"], "cashback_spike_detected")

            worker = asyncio.create_task(orch._hot_pipeline_worker())
            await asyncio.sleep(0.2)
            worker.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await worker

    async def test_apply_pipeline_feedback_called(self):
        orch = self._orch()
        mock_crawler = MagicMock()
        mock_crawler.apply_pipeline_feedback.return_value = MagicMock(
            mode="critical", interval_sec=300, reason="test"
        )
        orch.register_agent(AgentRole.CRAWLER, mock_crawler)
        crawler_data = _valid_intel()
        analysis = {"risk_level": "HIGH"}
        strategy = {"priority": "HIGH"}
        orch._apply_crawl_feedback(crawler_data, analysis, strategy)
        mock_crawler.apply_pipeline_feedback.assert_called_once_with(
            "myntra",
            analyst_risk="HIGH",
            strategist_priority="HIGH",
        )

    async def test_apply_pipeline_feedback_escalates_schedule(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "test.db")
            store = IntelligenceStore(db_path=db)
            crawler = CrawlerAgent(store=store)
            decision = crawler.apply_pipeline_feedback(
                "myntra", analyst_risk="HIGH", strategist_priority="LOW"
            )
            self.assertEqual(decision.mode, "critical")
            row = store.get_merchant_schedule("myntra")
            self.assertIsNotNone(row)
            self.assertEqual(row["analyst_risk"], "HIGH")
            self.assertEqual(row["crawl_mode"], "critical")

    async def test_dead_letter_on_invalid_intelligence(self):
        orch = self._orch()
        dl_path = os.path.join("logs", "test_dead_letter.jsonl")
        with patch("orchestrator.dead_letter.DEAD_LETTER_PATH", dl_path):
            if os.path.isfile(dl_path):
                os.remove(dl_path)
            orch._commit_intelligence_to_state({"merchant": "Bad", "offers": []})
            self.assertTrue(os.path.isfile(dl_path))
            with open(dl_path, encoding="utf-8") as f:
                line = f.readline()
            entry = json.loads(line)
            self.assertEqual(entry["agent"], "crawler")
            self.assertIn("stage", entry.get("context", {}))

    async def test_webhook_enqueues_hot_event(self):
        orch = self._orch()
        envelope = await enqueue_webhook_event(
            orch,
            {"merchant": "myntra", "type": "cashback_spike_detected", "severity": "critical"},
        )
        self.assertEqual(envelope["merchant"], "myntra")
        queued = await asyncio.wait_for(orch._hot_queue.get(), timeout=2.0)
        self.assertEqual(queued["type"], "cashback_spike_detected")


if __name__ == "__main__":
    unittest.main()
