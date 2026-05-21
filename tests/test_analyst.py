"""Analyst agent unit tests (no live Groq)."""
from __future__ import annotations

import json
import os
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from agents.analyst import AnalystAgent, EVENT_RISK_MATRIX
from messaging.schemas import AgentMessage, AgentRole, MessageType, Payload
from state.contracts import AnalystOutputContract, validate_analyst_output


def _crawler_payload(**overrides):
    base = {
        "merchant": "Myntra",
        "merchant_slug": "myntra",
        "cashback_rate": "12%",
        "client_rate": "5%",
        "aggregate_confidence": 0.85,
        "events": [],
        "sources": [],
        "consensus": {},
        "anomaly": {},
    }
    base.update(overrides)
    return base


class EventRiskMatrixTests(unittest.TestCase):
    def test_spike_maps_high(self):
        self.assertEqual(EVENT_RISK_MATRIX["cashback_spike_detected"], "HIGH")

    def test_spike_events_floor_high(self):
        agent = AnalystAgent()
        events = [{"type": "cashback_spike_detected"}]
        self.assertEqual(agent._event_risk_floor(events), "HIGH")

    def test_empty_events_no_event_floor(self):
        agent = AnalystAgent()
        self.assertIsNone(agent._event_risk_floor([]))

    def test_compute_floor_empty_events_low(self):
        agent = AnalystAgent()
        data = _crawler_payload(events=[], cashback_rate="3%", aggregate_confidence=0.9)
        self.assertEqual(agent._compute_rule_floor(data, []), "LOW")


class ConsensusAnomalyTests(unittest.TestCase):
    def test_anomaly_escalated_high(self):
        agent = AnalystAgent()
        data = _crawler_payload(anomaly={"escalated": True})
        self.assertEqual(agent._consensus_anomaly_floor(data), "HIGH")

    def test_consensus_contradiction_medium(self):
        agent = AnalystAgent()
        data = _crawler_payload(consensus={"contradictions": [{"source": "a"}]})
        self.assertEqual(agent._consensus_anomaly_floor(data), "MEDIUM")


class TrendDetectionTests(unittest.TestCase):
    def test_rising_rates_escalating(self):
        agent = AnalystAgent()
        history = {
            "rate_samples": [
                {"rate_pct": 5.0},
                {"rate_pct": 8.0},
                {"rate_pct": 12.0},
            ]
        }
        self.assertEqual(agent._detect_trend(history), "escalating")


class PredictiveSignalsTests(unittest.TestCase):
    def test_spike_escalating_high_probability(self):
        agent = AnalystAgent()
        history = {
            "rate_samples": [{"rate_pct": 5.0}, {"rate_pct": 8.0}, {"rate_pct": 12.0}],
            "recent_market_events": [{"type": "offer_added"}],
        }
        events = [{"type": "cashback_spike_detected"}]
        out = agent._compute_predictive_signals(history, events, "escalating", "HIGH")
        self.assertGreaterEqual(out["response_probability"], 0.6)
        self.assertIn("increase", out["predicted_competitor_move"].lower())

    def test_finalize_includes_predictive(self):
        agent = AnalystAgent()
        history = {"rate_samples": [{"rate_pct": 5.0}, {"rate_pct": 10.0}]}
        predictive = agent._compute_predictive_signals(
            history, [{"type": "new_offers_detected"}], "escalating", "MEDIUM"
        )
        data = _crawler_payload(events=[{"type": "new_offers_detected"}])
        result = agent._finalize_output(
            {},
            data,
            data["events"],
            rule_floor="MEDIUM",
            trend="escalating",
            history=history,
            predictive=predictive,
        )
        self.assertIsNotNone(result.get("response_probability"))
        self.assertTrue(result.get("predicted_competitor_move"))


class ContractValidationTests(unittest.TestCase):
    def test_validate_minimal(self):
        out = validate_analyst_output(
            {
                "risk_level": "HIGH",
                "gap_summary": "Competitor leads by 7 points.",
                "recommended_action": "Match rate.",
                "confidence": 0.8,
            },
            _crawler_payload(),
        )
        self.assertIsInstance(out, AnalystOutputContract)
        self.assertEqual(out.risk_level, "HIGH")

    def test_coerce_legacy_reasoning(self):
        out = validate_analyst_output(
            {
                "risk_level": "MEDIUM",
                "reasoning": "Gap detected.",
            },
            _crawler_payload(),
        )
        self.assertEqual(out.gap_summary, "Gap detected.")
        self.assertTrue(out.recommended_action)


class AnalystHandleRequestTests(unittest.IsolatedAsyncioTestCase):
    async def test_spike_mock_llm_high(self):
        agent = AnalystAgent()
        agent.use_history = False
        agent.shadow_enabled = False
        agent._call_llm = AsyncMock(
            return_value=(
                json.dumps(
                    {
                        "risk_level": "LOW",
                        "gap_summary": "Model underestimated.",
                        "recommended_action": "Wait.",
                        "confidence": 0.5,
                        "evidence_refs": [],
                        "trend": "stable",
                    }
                ),
                0.001,
            )
        )
        payload = _crawler_payload(
            events=[{"type": "cashback_spike_detected"}],
        )
        msg = AgentMessage(
            message_id="t1",
            sender=AgentRole.ORCHESTRATOR,
            receiver=AgentRole.ANALYST,
            message_type=MessageType.REQUEST,
            payload=Payload(data={"input": payload}),
        )
        resp = await agent.handle_request(msg)
        data = resp.payload.data
        self.assertEqual(data["risk_level"], "HIGH")
        self.assertEqual(data.get("event_risk_floor"), "HIGH")

    async def test_rules_only_empty_events_low(self):
        agent = AnalystAgent()
        agent.use_history = False
        agent.shadow_enabled = False
        agent._call_llm = AsyncMock(return_value=("", 0.0))
        payload = _crawler_payload(events=[], cashback_rate="3%", aggregate_confidence=0.9)
        msg = AgentMessage(
            message_id="t2",
            sender=AgentRole.ORCHESTRATOR,
            receiver=AgentRole.ANALYST,
            message_type=MessageType.REQUEST,
            payload=Payload(data={"input": payload}),
        )
        resp = await agent.handle_request(msg)
        self.assertEqual(resp.payload.data["risk_level"], "LOW")
        self.assertIn("analysis_mode", resp.payload.data)

    async def test_invalid_llm_falls_back(self):
        agent = AnalystAgent()
        agent.use_history = False
        agent.shadow_enabled = False
        agent._call_llm = AsyncMock(return_value=("not json at all", 0.0))
        payload = _crawler_payload(events=[{"type": "new_offers_detected"}])
        msg = AgentMessage(
            message_id="t3",
            sender=AgentRole.ORCHESTRATOR,
            receiver=AgentRole.ANALYST,
            message_type=MessageType.REQUEST,
            payload=Payload(data={"input": payload}),
        )
        resp = await agent.handle_request(msg)
        self.assertIn(resp.payload.data["risk_level"], ("HIGH", "MEDIUM", "LOW"))
        self.assertTrue(resp.payload.data.get("gap_summary"))

    async def test_semantic_cache_skips_primary_llm(self):
        agent = AnalystAgent()
        agent.use_history = False
        agent.shadow_enabled = False
        agent.cache_enabled = True
        env = patch.dict(os.environ, {"GROQ_API_KEY": "test-key"})
        llm_mock = AsyncMock(
            return_value=(
                json.dumps(
                    {
                        "risk_level": "MEDIUM",
                        "gap_summary": "Cached path test.",
                        "recommended_action": "Monitor.",
                        "confidence": 0.7,
                        "evidence_refs": [],
                        "trend": "stable",
                    }
                ),
                0.001,
            )
        )
        agent._call_llm = llm_mock
        payload = _crawler_payload(
            events=[],
            consensus={"consensus_rate": 8.0},
            cashback_rate="8%",
        )
        msg = AgentMessage(
            message_id="t4",
            sender=AgentRole.ORCHESTRATOR,
            receiver=AgentRole.ANALYST,
            message_type=MessageType.REQUEST,
            payload=Payload(data={"input": payload}),
        )
        with env:
            await agent.handle_request(msg)
            await agent.handle_request(msg)
            self.assertEqual(llm_mock.await_count, 1)
            self.assertEqual(
                (await agent.handle_request(msg)).payload.data.get("analysis_mode"),
                "cache_hit",
            )

    async def test_shadow_mismatch_calls_persist(self):
        agent = AnalystAgent()
        agent.use_history = False
        agent.cache_enabled = False
        agent._call_llm = AsyncMock(
            side_effect=[
                (
                    json.dumps(
                        {
                            "risk_level": "HIGH",
                            "gap_summary": "Primary high.",
                            "recommended_action": "Act.",
                            "confidence": 0.9,
                            "evidence_refs": [],
                            "trend": "escalating",
                        }
                    ),
                    0.001,
                ),
                (
                    json.dumps(
                        {
                            "risk_level": "LOW",
                            "gap_summary": "Shadow low.",
                            "recommended_action": "Wait.",
                            "confidence": 0.5,
                            "evidence_refs": [],
                            "trend": "stable",
                        }
                    ),
                    0.001,
                ),
            ]
        )
        with patch.dict(os.environ, {"GROQ_API_KEY": "test-key"}):
            with patch.object(agent, "_persist_shadow_delta") as persist_mock:
                payload = _crawler_payload(
                    merchant_slug="shadowtest",
                    events=[{"type": "cashback_spike_detected"}],
                )
                msg = AgentMessage(
                    message_id="t5",
                    sender=AgentRole.ORCHESTRATOR,
                    receiver=AgentRole.ANALYST,
                    message_type=MessageType.REQUEST,
                    payload=Payload(data={"input": payload}),
                )
                resp = await agent.handle_request(msg)
                self.assertFalse(resp.payload.data["shadow_test"]["match"])
                persist_mock.assert_called_once()


class StoreAnalystPersistenceTests(unittest.TestCase):
    def test_record_shadow_delta_sqlite(self):
        from agents.crawler.platform.store import IntelligenceStore

        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "test_intel.db")
            store = IntelligenceStore(db_path=db_path)
            store.record_analyst_shadow_delta(
                "myntra",
                primary_risk="HIGH",
                shadow_risk="LOW",
                shadow_model="test-model",
                delta={"primary": "HIGH", "shadow": "LOW"},
            )
            import sqlite3

            conn = sqlite3.connect(db_path)
            count = conn.execute(
                "SELECT COUNT(*) FROM analyst_shadow_deltas WHERE merchant_slug = ?",
                ("myntra",),
            ).fetchone()[0]
            conn.close()
            self.assertEqual(count, 1)

    def test_get_analyst_history(self):
        from agents.crawler.platform.store import IntelligenceStore

        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "test_intel.db")
            store = IntelligenceStore(db_path=db_path)
            store.record_analyst_output(
                "myntra",
                {"risk_level": "HIGH", "gap_summary": "Gap.", "recommended_action": "Act."},
            )
            hist = store.get_analyst_history("myntra", limit=3)
            self.assertEqual(len(hist), 1)
            self.assertEqual(hist[0]["risk_level"], "HIGH")


if __name__ == "__main__":
    unittest.main()
