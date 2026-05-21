"""Build a fully registered orchestrator for CLI and main.py."""
from __future__ import annotations

import os
from typing import Optional

from agents.alerter import AlertAgent
from agents.analyst import AnalystAgent
from agents.crawler import CrawlerAgent
from agents.strategist import StrategistAgent
from messaging.schemas import AgentRole
from orchestrator.orchestrator import SwarmOrchestrator
from state.state_manager import SharedState


def build_orchestrator(
    state: Optional[SharedState] = None,
    *,
    budget_limit: Optional[float] = None,
) -> SwarmOrchestrator:
    state = state or SharedState()
    limit = budget_limit if budget_limit is not None else float(os.getenv("MAX_BUDGET_USD", "10.0"))
    orchestrator = SwarmOrchestrator(state, budget_limit=limit)
    crawler = CrawlerAgent()
    orchestrator.register_agent(AgentRole.CRAWLER, crawler)
    orchestrator.register_agent(AgentRole.ANALYST, AnalystAgent())
    orchestrator.register_agent(AgentRole.STRATEGIST, StrategistAgent())
    orchestrator.register_agent(AgentRole.ALERTER, AlertAgent())
    return orchestrator
