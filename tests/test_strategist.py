"""Strategist agent unit tests (no live Groq)."""
from __future__ import annotations

import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from agents.alerter import AlertAgent
from agents.strategist import StrategistAgent, _realistic_analyst_fixture
from agents.strategist_approval import list_pending, resolve_approval, submit_for_approval
from agents.strategist_playbooks import get_playbook, reset_playbook_cache
from messaging.schemas import AgentMessage, AgentRole, MessageType, Payload
from state.contracts import (
    StrategistOutputContract,
    compute_counter_offer_suggestion,
    compute_negotiation_leverage,
    validate_strategist_output,
)


def _high_risk_analysis(**overrides):
    base = _realistic_analyst_fixture()
    base.update(overrides)
    return base


class StrategistContractTests(unittest.TestCase):
    def test_validate_high_risk(self):
        out = validate_strategist_output(
            {
                "recommendation": "Match competitor immediately.",
                "priority": "HIGH",
                "negotiation_brief": "Escalate to Myntra partnerships.",
                "threat_level": "HIGH",
                "merchant_actions": ["Call VP"],
                "confidence": 0.9,
                "based_on_events": ["cashback_spike_detected"],
                "executive_summary": ["a", "b", "c"],
                "talking_points": ["Rate gap 7pts"],
                "urgency_hours": 4,
            },
            _high_risk_analysis(),
        )
        self.assertIsInstance(out, StrategistOutputContract)
        self.assertEqual(out.priority, "HIGH")
        self.assertTrue(out.aligned_with_analyst)

    def test_coerce_defaults_from_analysis(self):
        out = validate_strategist_output({}, _high_risk_analysis())
        self.assertEqual(out.priority, "HIGH")
        self.assertEqual(out.threat_level, "HIGH")
        self.assertGreaterEqual(len(out.executive_summary), 1)
        self.assertEqual(out.negotiation_leverage, "high")
        self.assertIn("7", out.counter_offer_suggestion)
        self.assertIn("Myntra", out.notification_preview)

    def test_game_theory_low_gap(self):
        analysis = _high_risk_analysis(
            risk_level="LOW", competitor_advantage_pct=0, trend="stable"
        )
        self.assertEqual(compute_negotiation_leverage(analysis), "low")
        self.assertIn("Hold", compute_counter_offer_suggestion(analysis))


class StrategistRulesFallbackTests(unittest.TestCase):
    def test_high_risk_yields_high_priority(self):
        agent = StrategistAgent()
        agent.use_playbooks = False
        result = agent._rules_fallback(_high_risk_analysis())
        self.assertEqual(result["priority"], "HIGH")
        self.assertEqual(result["threat_level"], "HIGH")
        self.assertTrue(result["aligned_with_analyst"])
        self.assertEqual(result["urgency_hours"], 4)

    def test_crawler_summary_from_snapshots(self):
        agent = StrategistAgent()
        summary = agent._crawler_summary(_high_risk_analysis())
        self.assertEqual(summary["consensus_rate"], 12.0)
        self.assertEqual(summary["source_count"], 3)
        self.assertEqual(len(summary["top_events"]), 2)


class StrategistHandleRequestTests(unittest.IsolatedAsyncioTestCase):
    async def test_high_risk_mock_llm_under_priority_elevated(self):
        agent = StrategistAgent()
        agent.use_playbooks = False
        agent._call_llm = AsyncMock(
            return_value=(
                json.dumps(
                    {
                        "recommendation": "Wait and see.",
                        "priority": "LOW",
                        "negotiation_brief": "No rush.",
                        "threat_level": "LOW",
                        "merchant_actions": ["Do nothing"],
                        "confidence": 0.4,
                        "based_on_events": [],
                        "executive_summary": ["x", "y", "z"],
                        "talking_points": ["t1"],
                        "urgency_hours": 72,
                    }
                ),
                0.001,
            )
        )
        msg = AgentMessage(
            message_id="s1",
            sender=AgentRole.ORCHESTRATOR,
            receiver=AgentRole.STRATEGIST,
            message_type=MessageType.REQUEST,
            payload=Payload(data={"input": _high_risk_analysis()}),
        )
        resp = await agent.handle_request(msg)
        data = resp.payload.data
        self.assertEqual(data["priority"], "HIGH")
        self.assertTrue(data["aligned_with_analyst"])

    async def test_rules_only_no_api_key(self):
        import os

        prev = os.environ.pop("GROQ_API_KEY", None)
        try:
            agent = StrategistAgent()
            agent.use_playbooks = False
            msg = AgentMessage(
                message_id="s2",
                sender=AgentRole.ORCHESTRATOR,
                receiver=AgentRole.STRATEGIST,
                message_type=MessageType.REQUEST,
                payload=Payload(data={"input": _high_risk_analysis()}),
            )
            resp = await agent.handle_request(msg)
            data = resp.payload.data
            self.assertEqual(data["priority"], "HIGH")
            self.assertEqual(data.get("strategy_mode"), "rules_fallback")
        finally:
            if prev is not None:
                os.environ["GROQ_API_KEY"] = prev

    async def test_invalid_llm_falls_back(self):
        agent = StrategistAgent()
        agent.use_playbooks = False
        agent._call_llm = AsyncMock(return_value=("not json", 0.0))
        msg = AgentMessage(
            message_id="s3",
            sender=AgentRole.ORCHESTRATOR,
            receiver=AgentRole.STRATEGIST,
            message_type=MessageType.REQUEST,
            payload=Payload(data={"input": _high_risk_analysis(risk_level="MEDIUM")}),
        )
        resp = await agent.handle_request(msg)
        data = resp.payload.data
        self.assertIn(data["priority"], ("HIGH", "MEDIUM", "LOW"))
        self.assertTrue(data.get("negotiation_brief"))


class StrategistPlaybookEnvTests(unittest.TestCase):
    def tearDown(self):
        reset_playbook_cache()
        for key in (
            "STRATEGIST_PLAYBOOK_JSON",
            "STRATEGIST_PLAYBOOK_SLUGS",
        ):
            os.environ.pop(key, None)

    def test_playbook_json_override(self):
        os.environ["STRATEGIST_PLAYBOOK_JSON"] = json.dumps(
            {"myntra": {"tone": "ENV_OVERRIDE_TONE"}}
        )
        reset_playbook_cache()
        pb = get_playbook("myntra")
        self.assertIn("ENV_OVERRIDE_TONE", pb["tone"])

    def test_playbook_slug_alias(self):
        os.environ["STRATEGIST_PLAYBOOK_SLUGS"] = "custom_shop:flipkart"
        reset_playbook_cache()
        pb = get_playbook("custom_shop")
        self.assertEqual(pb["vertical"], "electronics")


class StrategistApprovalQueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.approval_dir = Path(self.tmp) / "approvals"
        self.env_patch = patch.dict(
            os.environ,
            {"STRATEGIST_APPROVAL_DIR": str(self.approval_dir)},
        )
        self.env_patch.start()

    def tearDown(self):
        self.env_patch.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_submit_list_resolve(self):
        strategy = validate_strategist_output(
            {"recommendation": "Act", "priority": "HIGH", "negotiation_brief": "Brief"},
            _high_risk_analysis(),
        ).model_dump()
        record = submit_for_approval(strategy, merchant="Myntra")
        self.assertEqual(record["approval_status"], "pending")
        self.assertTrue(record["approval_id"])

        pending = list_pending()
        self.assertEqual(len(pending), 1)
        aid = pending[0]["approval_id"]

        approved = resolve_approval(aid, approved=True)
        self.assertEqual(approved["approval_status"], "approved")
        self.assertEqual(len(list_pending()), 0)

    async def _run_with_queue(self):
        with patch.dict(
            os.environ,
            {
                "STRATEGIST_APPROVAL_QUEUE": "true",
                "STRATEGIST_APPROVAL_DIR": str(self.approval_dir),
            },
        ):
            agent = StrategistAgent()
            agent.use_playbooks = False
            prev = os.environ.pop("GROQ_API_KEY", None)
            try:
                msg = AgentMessage(
                    message_id="aq1",
                    sender=AgentRole.ORCHESTRATOR,
                    receiver=AgentRole.STRATEGIST,
                    message_type=MessageType.REQUEST,
                    payload=Payload(data={"input": _high_risk_analysis()}),
                )
                return await agent.handle_request(msg)
            finally:
                if prev is not None:
                    os.environ["GROQ_API_KEY"] = prev

    def test_handle_request_queues_when_enabled(self):
        async def _run():
            return await self._run_with_queue()

        import asyncio

        resp = asyncio.run(_run())
        data = resp.payload.data
        self.assertEqual(data["approval_status"], "pending")
        self.assertTrue(data.get("approval_id"))
        self.assertTrue(data.get("submitted_at"))


class StrategistAlerterPreviewTests(unittest.IsolatedAsyncioTestCase):
    async def test_alerter_uses_notification_preview(self):
        agent = AlertAgent()
        strategy = validate_strategist_output(
            {
                "recommendation": "Match now",
                "priority": "HIGH",
                "negotiation_brief": "Urgent brief",
                "threat_level": "HIGH",
            },
            _high_risk_analysis(),
        ).model_dump()
        msg = AgentMessage(
            message_id="a1",
            sender=AgentRole.ORCHESTRATOR,
            receiver=AgentRole.ALERTER,
            message_type=MessageType.REQUEST,
            payload=Payload(data={"input": strategy}),
        )
        resp = await agent.handle_request(msg)
        content = resp.payload.data["alert_content"]
        self.assertIn(strategy["notification_preview"], content)
        self.assertIn("Executive summary", content)
        self.assertEqual(resp.cost, 0.0)


if __name__ == "__main__":
    unittest.main()
