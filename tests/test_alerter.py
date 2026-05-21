"""Alerter agent unit tests — no live Slack/webhook."""
from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime
from unittest.mock import patch

from agents.alerter import (
    AlertDeliveryService,
    DedupeStore,
    build_dedupe_key,
    build_slack_message,
    channels_for_severity,
    in_quiet_hours,
    parse_quiet_hours,
)
from state.contracts import (
    AlerterOutputContract,
    map_risk_priority_to_severity,
    severity_meets_minimum,
    validate_alerter_output,
)


def _strategy(**overrides):
    base = {
        "recommendation": "Match competitor rate immediately.",
        "priority": "HIGH",
        "threat_level": "HIGH",
        "analyst_risk_level": "HIGH",
        "merchant": "Myntra",
        "merchant_slug": "myntra",
        "urgency_hours": 4,
        "executive_summary": ["Bullet one", "Bullet two"],
        "notification_preview": ":red_circle: *Myntra* | HIGH",
    }
    base.update(overrides)
    return base


class SeverityMappingTests(unittest.TestCase):
    def test_high_high_critical(self):
        self.assertEqual(map_risk_priority_to_severity("HIGH", "HIGH"), "critical")

    def test_high_medium_critical(self):
        self.assertEqual(map_risk_priority_to_severity("HIGH", "MEDIUM"), "critical")

    def test_medium_warning(self):
        self.assertEqual(map_risk_priority_to_severity("MEDIUM", "MEDIUM"), "warning")

    def test_low_info(self):
        self.assertEqual(map_risk_priority_to_severity("LOW", "LOW"), "info")

    def test_min_severity_gate(self):
        self.assertTrue(severity_meets_minimum("critical", "warning"))
        self.assertFalse(severity_meets_minimum("info", "warning"))


class ChannelRoutingTests(unittest.TestCase):
    def test_critical_all_enabled(self):
        ch = channels_for_severity("critical", slack_enabled=True, webhook_enabled=True)
        self.assertEqual(set(ch), {"file", "slack", "webhook"})

    def test_info_file_only(self):
        ch = channels_for_severity("info", slack_enabled=True, webhook_enabled=True)
        self.assertEqual(ch, ["file"])


class DedupeTests(unittest.TestCase):
    def test_dedupe_store_ttl(self):
        store = DedupeStore(":memory:", ttl_sec=60)
        key = "myntra:critical:abc"
        self.assertFalse(store.seen_recently(key))
        store.record(key)
        self.assertTrue(store.seen_recently(key))

    def test_dedupe_key_stable(self):
        s = _strategy()
        k1 = build_dedupe_key(s, "critical")
        k2 = build_dedupe_key(s, "critical")
        self.assertEqual(k1, k2)
        self.assertTrue(k1.startswith("myntra:"))


class QuietHoursTests(unittest.TestCase):
    def test_parse_valid(self):
        self.assertEqual(parse_quiet_hours("23-07"), (23, 7))

    def test_parse_invalid(self):
        self.assertIsNone(parse_quiet_hours("bad"))

    @patch.dict(os.environ, {"ALERTER_QUIET_HOURS": "23-07"})
    def test_inside_quiet_window(self):
        self.assertTrue(in_quiet_hours(datetime(2026, 5, 19, 2, 0, 0)))

    @patch.dict(os.environ, {"ALERTER_QUIET_HOURS": "23-07"})
    def test_outside_quiet_window(self):
        self.assertFalse(in_quiet_hours(datetime(2026, 5, 19, 12, 0, 0)))


class DeliveryServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.mkdtemp()
        self.audit = os.path.join(self.tmp, "alerts.jsonl")
        self.dedupe_store = DedupeStore(":memory:", ttl_sec=3600)
        self.svc = AlertDeliveryService(
            dedupe_store=self.dedupe_store,
            audit_path=self.audit,
        )
        self.svc.slack_enabled = False
        self.svc.webhook_enabled = False
        self.svc.min_severity = "info"

    async def asyncTearDown(self):
        if getattr(self.dedupe_store, "_mem_conn", None):
            self.dedupe_store._mem_conn.close()
            self.dedupe_store._mem_conn = None

    async def test_file_audit_always_written(self):
        result = await self.svc.send_alert(_strategy())
        self.assertEqual(result["delivery_status"], "sent")
        self.assertIn("file", result["channels_attempted"])
        self.assertTrue(os.path.isfile(self.audit))
        with open(self.audit, encoding="utf-8") as f:
            line = json.loads(f.readline())
        self.assertEqual(line["type"], "swarm_alert")
        self.assertEqual(line["severity"], "critical")

    async def test_dedupe_skips_second_send(self):
        r1 = await self.svc.send_alert(_strategy())
        self.assertEqual(r1["delivery_status"], "sent")
        r2 = await self.svc.send_alert(_strategy())
        self.assertTrue(r2["delivery_status"].startswith("skipped:dedupe"))

    async def test_below_min_severity_skipped(self):
        self.svc.min_severity = "warning"
        result = await self.svc.send_alert(
            _strategy(priority="LOW", analyst_risk_level="LOW", threat_level="LOW")
        )
        self.assertEqual(result["delivery_status"], "skipped:below_min_severity")
        self.assertEqual(result["severity"], "info")

    @patch.dict(os.environ, {"ALERTER_QUIET_HOURS": "23-07"})
    async def test_quiet_hours_skips_warning(self):
        self.svc.min_severity = "info"
        with patch("agents.alerter.in_quiet_hours", return_value=True):
            result = await self.svc.send_alert(
                _strategy(priority="MEDIUM", analyst_risk_level="MEDIUM")
            )
        self.assertEqual(result["delivery_status"], "skipped:quiet_hours")

    @patch("agents.alerter.deliver_slack", return_value=True)
    @patch("agents.alerter.deliver_webhook", return_value=True)
    async def test_slack_and_webhook_when_enabled(self, _wh, _sl):
        self.svc.slack_enabled = True
        self.svc.webhook_enabled = True
        self.svc.slack_url = "https://hooks.slack.com/fake"
        self.svc.webhook_url = "https://example.com/hook"
        result = await self.svc.send_alert(_strategy(), force=True)
        self.assertIn("slack", result["channels_attempted"])
        self.assertIn("webhook", result["channels_attempted"])
        self.assertEqual(result["delivery_status"], "sent")


class ContractTests(unittest.TestCase):
    def test_validate_output(self):
        data = {
            "alert_content": "Test alert",
            "channel": "file",
            "severity": "warning",
            "merchant_slug": "myntra",
            "dedupe_key": "myntra:warning:abc",
            "sent_at": "2026-01-01T00:00:00+00:00",
            "delivery_status": "sent",
            "channels_attempted": ["file"],
        }
        out = validate_alerter_output(data)
        self.assertIsInstance(out, AlerterOutputContract)

    def test_build_slack_uses_preview(self):
        text = build_slack_message(_strategy())
        self.assertIn("Myntra", text)
        self.assertIn("Executive summary", text)


if __name__ == "__main__":
    unittest.main()
