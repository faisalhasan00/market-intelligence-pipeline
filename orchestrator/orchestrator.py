from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import uuid
from typing import Any, Dict, List, Optional, Set, Tuple

if sys.platform.startswith("win"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from loguru import logger
from pydantic import ValidationError

from agents.crawler.crawl.profiles import merchant_display, normalize_query
from agents.crawler.intelligence.streaming.sinks import format_stream_envelope
from agents.crawler.scheduling.policy import CRITICAL_EVENT_TYPES, HOT_EVENT_TYPES
from messaging.schemas import AgentMessage, AgentRole, MessageType, Payload
from orchestrator.dead_letter import append_dead_letter
from orchestrator.results import MerchantPipelineResult, PipelineResult
from orchestrator.state_lock import state_commit_lock
from state.contracts import sanitize_crawler_payload, validate_intelligence_output
from state.state_manager import SharedState

logger.add("logs/swarm_timeline.json", format="{message}", level="INFO", serialize=True)

CRITICAL_STREAM_TYPES = frozenset(
    CRITICAL_EVENT_TYPES
    | {
        "rate_anomaly_detected",
        "market_sweep_initiated",
        "evidence_mismatch",
    }
)


class SwarmOrchestrator:
    """
    Control plane for the Swarm: execution order, retries, budget, conflicts,
    crawler stream hooks, and structured pipeline results.
    """

    def __init__(self, state: SharedState, budget_limit: Optional[float] = None):
        self.state = state
        env_budget = os.getenv("MAX_BUDGET_USD")
        self.budget_limit = budget_limit or (float(env_budget) if env_budget else 0.50)
        self.total_cost = 0.0
        self.agents: Dict[AgentRole, Any] = {}
        self.critical_path: List[Dict[str, Any]] = []

        self.max_retries = int(os.getenv("ORCHESTRATOR_MAX_RETRIES", "2"))
        self.retry_backoff_sec = float(os.getenv("ORCHESTRATOR_RETRY_BACKOFF_SEC", "2.0"))
        self.agent_timeout_sec = float(os.getenv("ORCHESTRATOR_AGENT_TIMEOUT_SEC", "60"))
        self.max_parallel_merchants = max(
            1, int(os.getenv("ORCHESTRATOR_MAX_PARALLEL_MERCHANTS", "1"))
        )
        self.event_log_poll_sec = float(os.getenv("ORCHESTRATOR_EVENT_POLL_SEC", "5.0"))

        self._hot_queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
        self._event_tasks: List[asyncio.Task] = []
        self._stream_seen: Set[Tuple[str, str, str]] = set()
        self._log_offset = 0
        self._events_integrated = False

    def register_agent(self, role: AgentRole, agent_instance: Any) -> None:
        self.agents[role] = agent_instance

    # ------------------------------------------------------------------ #
    # Event-driven integration (stream subscribe + JSONL poll)
    # ------------------------------------------------------------------ #

    async def start_event_integration(self) -> None:
        """Subscribe to crawler intelligence stream and poll optional JSONL sink."""
        if self._events_integrated:
            return
        self._events_integrated = True

        from agents.crawler.intelligence.streaming.stream import get_intelligence_stream

        stream = get_intelligence_stream()
        stream.subscribe(self._handle_stream_event)
        self._log_event("EVENT_INTEGRATION", "Subscribed to crawler intelligence stream")

        log_path = os.getenv("CRAWLER_EVENT_LOG_PATH", "").strip()
        if log_path and os.getenv("ORCHESTRATOR_POLL_EVENT_LOG", "true").lower() == "true":
            self._event_tasks.append(asyncio.create_task(self._poll_event_log(log_path)))

        self._event_tasks.append(asyncio.create_task(self._hot_pipeline_worker()))

    async def stop_event_integration(self) -> None:
        tasks = list(self._event_tasks)
        self._event_tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _handle_stream_event(self, event: Dict[str, Any]) -> None:
        envelope = format_stream_envelope(event)
        key = (envelope["type"], envelope["merchant"], envelope.get("ts", ""))
        if key in self._stream_seen:
            return
        self._stream_seen.add(key)
        if len(self._stream_seen) > 2000:
            self._stream_seen.clear()

        if envelope["severity"] == "critical" or envelope["type"] in CRITICAL_STREAM_TYPES:
            self._log_event(
                "STREAM_CRITICAL",
                f"Queued hot pipeline for {envelope['merchant']}",
                {"type": envelope["type"], "action": envelope.get("recommended_action")},
            )
            await self._hot_queue.put(envelope)

    async def _poll_event_log(self, path: str) -> None:
        while True:
            try:
                if os.path.isfile(path):
                    with open(path, "r", encoding="utf-8") as f:
                        f.seek(self._log_offset)
                        for line in f:
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                envelope = json.loads(line)
                            except json.JSONDecodeError:
                                continue
                            if envelope.get("severity") == "critical":
                                await self._hot_queue.put(envelope)
                        self._log_offset = f.tell()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._log_event("EVENT_POLL_ERROR", str(exc), level="WARNING")
            await asyncio.sleep(self.event_log_poll_sec)

    async def enqueue_hot_event(self, envelope: Dict[str, Any]) -> None:
        """Enqueue a merchant for the hot pipeline (webhook, stream, or tests)."""
        await self._hot_queue.put(envelope)

    async def _hot_pipeline_worker(self) -> None:
        while True:
            envelope = await self._hot_queue.get()
            merchant = (envelope.get("merchant") or "").strip()
            if not merchant:
                continue
            if self.total_cost >= self.budget_limit:
                self._log_event("BUDGET_EXCEEDED", "Skipping stream hot pipeline — budget cap")
                continue
            try:
                await self.run_hot_pipeline_for_merchant(merchant, trigger=envelope.get("type", "stream"))
            except Exception as exc:
                self._log_event("HOT_PIPELINE_ERROR", str(exc), {"merchant": merchant}, level="ERROR")

    async def run_hot_pipeline_for_merchant(self, merchant: str, *, trigger: str = "stream") -> MerchantPipelineResult:
        """Crawl + analyst/strategist when a critical stream event fires."""
        crawler = self.agents.get(AgentRole.CRAWLER)
        if not crawler:
            raise RuntimeError("Crawler agent not registered")

        slug = normalize_query(merchant)
        display = merchant_display(slug)
        cost_before = self.total_cost
        try:
            intel = await crawler.collect_intelligence(f"Analyze {display} coupons")
            self._commit_intelligence_to_state(intel)
            result = await self.run_intel_pipeline(intel, depth="stream_hot")
            result.events = intel.get("events") or []
            result.cost_delta = self.total_cost - cost_before
            self._log_event("HOT_PIPELINE_COMPLETE", f"{display} via {trigger}", {"slug": slug, "depth": result.depth})
            return result
        except Exception as exc:
            return MerchantPipelineResult(
                merchant=display,
                slug=slug,
                success=False,
                depth="stream_hot",
                cost_delta=self.total_cost - cost_before,
                error=str(exc),
            )

    # ------------------------------------------------------------------ #
    # Pipelines
    # ------------------------------------------------------------------ #

    async def run_pipeline(self, initial_query: str) -> PipelineResult:
        """Full crawler → analyst → strategist → conflicts → alerter."""
        CYAN, YELLOW, GREEN, MAGENTA, BLUE, BOLD, RESET = (
            "\033[96m", "\033[93m", "\033[92m", "\033[95m", "\033[94m", "\033[1m", "\033[0m"
        )
        alerts_sent = 0
        events_processed = 0

        try:
            print(f"\n{BLUE}{BOLD}=================================================={RESET}")
            print(f"{BLUE}{BOLD}[PIPELINE START] Target: {initial_query}{RESET}")
            print(f"{BLUE}{BOLD}=================================================={RESET}\n")

            print(f"{CYAN}{BOLD}[PHASE: CRAWLER]{RESET} Searching for live merchant deals...")
            crawler_data = await self._execute_agent(AgentRole.CRAWLER, initial_query)
            print("")
            self._commit_intelligence_to_state(crawler_data)

            events = crawler_data.get("events") or []
            events_processed += len(events)
            if events:
                self._log_event("MARKET_EVENTS", f"{len(events)} intelligence events", {"events": events})

            if not crawler_data or "merchant" not in crawler_data:
                print(f"{YELLOW}[VETO] Analyst rejected noisy output. Retrying...{RESET}")
                self._log_event("DATA_VETO", "Noisy crawler output — re-crawl", level="WARNING")
                crawler_data = await self._execute_agent(
                    AgentRole.CRAWLER, f"REVISE: Last extraction was noisy. Focus on {initial_query}"
                )
                self._commit_intelligence_to_state(crawler_data)
                print("")

            print(f"{YELLOW}{BOLD}[PHASE: ANALYST]{RESET} Computing market gaps and defection risk...")
            analysis_data = await self._execute_agent(AgentRole.ANALYST, crawler_data)
            print("")

            print(f"{GREEN}{BOLD}[PHASE: STRATEGIST]{RESET} Synthesizing negotiation briefs...")
            strategy_data = await self._execute_agent(AgentRole.STRATEGIST, analysis_data)
            print("")

            print(f"{BLUE}{BOLD}[PHASE: RESOLVE]{RESET} Checking for strategy conflicts...")
            resolved_strategy = await self._handle_conflicts(analysis_data, strategy_data)
            print("")
            self._apply_crawl_feedback(crawler_data, analysis_data, resolved_strategy)

            if analysis_data.get("risk_level") == resolved_strategy.get("priority"):
                print(f"{MAGENTA}{BOLD}[PHASE: ALERTER]{RESET} Dispatching notification to Slack...")
                alert_input = {
                    **resolved_strategy,
                    "analyst_risk_level": analysis_data.get("risk_level"),
                    "merchant_slug": crawler_data.get("merchant_slug")
                    or resolved_strategy.get("merchant_slug"),
                    "merchant": crawler_data.get("merchant")
                    or resolved_strategy.get("merchant"),
                    "client_rate": analysis_data.get("client_rate"),
                    "competitor_rate": analysis_data.get("competitor_rate"),
                    "evidence_refs": analysis_data.get("evidence_refs"),
                    "competitor_advantage_pct": analysis_data.get("competitor_advantage_pct"),
                    "reasoning": analysis_data.get("reasoning"),
                }
                await self._execute_agent(AgentRole.ALERTER, alert_input)
                alerts_sent = 1
            else:
                print(
                    f"{MAGENTA}! [ALERT SKIPPED] Severity Mismatch: Risk ({analysis_data.get('risk_level')}) "
                    f"vs Priority ({resolved_strategy.get('priority')}){RESET}"
                )
                self._log_event(
                    "ALERT_SKIPPED",
                    "Analyst and Strategist disagreed on severity. Alert withheld.",
                    {"risk": analysis_data.get("risk_level"), "priority": resolved_strategy.get("priority")},
                )

            slug = crawler_data.get("merchant_slug") or normalize_query(str(crawler_data.get("merchant", "")))
            merchant_result = MerchantPipelineResult(
                merchant=str(crawler_data.get("merchant", initial_query)),
                slug=slug,
                success=True,
                depth="full",
                cost_delta=self.total_cost,
                events=events,
                alerts_sent=alerts_sent,
            )

            print(
                f"\n{BLUE}{BOLD}[PIPELINE COMPLETE] Cost: ${self.total_cost:.4f} | "
                f"Ver: {self.state.get_version()}{RESET}\n"
            )
            self._log_event("PIPELINE_COMPLETE", f"Pipeline finished. Total Cost: ${self.total_cost:.4f}")

            return PipelineResult(
                query=initial_query,
                success=True,
                total_cost=self.total_cost,
                state_version=self.state.get_version(),
                merchants=[merchant_result],
                events_processed=events_processed,
                alerts_sent=alerts_sent,
            )
        except Exception as exc:
            self._log_event("PIPELINE_ERROR", str(exc), level="ERROR")
            print(f"[FATAL ERROR] {exc}")
            return PipelineResult(
                query=initial_query,
                success=False,
                total_cost=self.total_cost,
                state_version=self.state.get_version(),
                error=str(exc),
            )

    async def run_intel_pipeline(
        self,
        crawler_data: Dict[str, Any],
        *,
        depth: str = "hot_intel",
    ) -> MerchantPipelineResult:
        """Analyst → strategist → conflicts → optional alerter (no re-crawl)."""
        slug = crawler_data.get("merchant_slug") or normalize_query(str(crawler_data.get("merchant", "")))
        merchant = str(crawler_data.get("merchant", slug))
        cost_before = self.total_cost
        alerts_sent = 0

        analysis = await self._execute_agent(AgentRole.ANALYST, crawler_data)
        strategy = await self._execute_agent(AgentRole.STRATEGIST, analysis)
        resolved = await self._handle_conflicts(analysis, strategy)
        self._apply_crawl_feedback(crawler_data, analysis, resolved)

        if analysis.get("risk_level") == resolved.get("priority"):
            alert_input = {
                **resolved,
                "analyst_risk_level": analysis.get("risk_level"),
                "merchant_slug": crawler_data.get("merchant_slug")
                or resolved.get("merchant_slug"),
                "merchant": crawler_data.get("merchant") or resolved.get("merchant"),
                "client_rate": analysis.get("client_rate"),
                "competitor_rate": analysis.get("competitor_rate"),
                "evidence_refs": analysis.get("evidence_refs"),
                "competitor_advantage_pct": analysis.get("competitor_advantage_pct"),
                "reasoning": analysis.get("reasoning"),
            }
            await self._execute_agent(AgentRole.ALERTER, alert_input)
            alerts_sent = 1

        return MerchantPipelineResult(
            merchant=merchant,
            slug=slug,
            success=True,
            depth=depth,
            cost_delta=self.total_cost - cost_before,
            events=crawler_data.get("events") or [],
            alerts_sent=alerts_sent,
        )

    def should_run_full_pipeline(
        self,
        *,
        events: List[Dict[str, Any]],
        crawl_mode: Optional[str] = None,
    ) -> bool:
        crawler = self.agents.get(AgentRole.CRAWLER)
        if crawler and hasattr(crawler, "policy"):
            return crawler.policy.should_run_full_pipeline(events=events, crawl_mode=crawl_mode)
        if os.getenv("SWARM_ALWAYS_PIPELINE", "false").lower() == "true":
            return True
        if crawl_mode in ("critical", "hot"):
            return True
        types = {e.get("type") for e in events}
        return bool(types & HOT_EVENT_TYPES)

    async def run_adaptive_iteration(self, watchlist: List[str]) -> List[MerchantPipelineResult]:
        """One scheduler loop: crawl due merchants, hot pipeline when warranted."""
        crawler = self.agents.get(AgentRole.CRAWLER)
        if not crawler:
            raise RuntimeError("Crawler agent not registered")

        due = crawler.get_due_merchants(watchlist)
        if not due:
            return []

        if self.max_parallel_merchants <= 1:
            results: List[MerchantPipelineResult] = []
            for merchant in due:
                results.append(await self._process_due_merchant(merchant, crawler))
                if self.total_cost >= self.budget_limit:
                    break
            return results

        sem = asyncio.Semaphore(self.max_parallel_merchants)

        async def bounded(merchant: str) -> MerchantPipelineResult:
            async with sem:
                return await self._process_due_merchant(merchant, crawler)

        return list(await asyncio.gather(*[bounded(m) for m in due]))

    async def _process_due_merchant(self, merchant: str, crawler: Any) -> MerchantPipelineResult:
        slug = crawler.scheduler.slug_for_display_name(merchant)
        schedule = crawler.store.get_merchant_schedule(slug) or {}
        mode = schedule.get("crawl_mode", "normal")
        cost_before = self.total_cost

        try:
            intel = await crawler.collect_intelligence(f"Analyze {merchant} coupons")
            self._commit_intelligence_to_state(intel)
            events = intel.get("events") or []
            monitoring = intel.get("monitoring") or {}
            print(
                f"   [Scheduler] {merchant}: mode={monitoring.get('crawl_mode', mode)} "
                f"next in {monitoring.get('crawl_interval_sec')}s — {monitoring.get('monitor_reason', '')}"
            )

            if self.should_run_full_pipeline(
                events=events,
                crawl_mode=monitoring.get("crawl_mode"),
            ):
                print(f"   [Scheduler] Hot market → full pipeline for {merchant}")
                result = await self.run_intel_pipeline(intel, depth="hot_intel")
            else:
                print(f"   [Scheduler] Stable → crawl-only for {merchant} (saving LLM budget)")
                result = MerchantPipelineResult(
                    merchant=merchant,
                    slug=slug,
                    success=True,
                    depth="crawl_only",
                    events=events,
                )

            result.cost_delta = self.total_cost - cost_before
            return result
        except Exception as exc:
            print(f"⚠️ [ERROR] Failed for {merchant}: {exc}")
            return MerchantPipelineResult(
                merchant=merchant,
                slug=slug,
                success=False,
                depth="crawl_only",
                cost_delta=self.total_cost - cost_before,
                error=str(exc),
            )

    # ------------------------------------------------------------------ #
    # Agent execution (retries, budget, timeouts)
    # ------------------------------------------------------------------ #

    async def _execute_agent(self, role: AgentRole, input_data: Any) -> Dict[str, Any]:
        last_error: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                return await self._execute_agent_once(role, input_data)
            except Exception as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    self._log_event(
                        "AGENT_FAILED",
                        f"{role.value} failed after {attempt + 1} attempts",
                        {"error": str(exc)},
                        level="ERROR",
                    )
                    raise
                delay = self.retry_backoff_sec * (2**attempt)
                self._log_event(
                    "AGENT_RETRY",
                    f"{role.value} attempt {attempt + 1} failed — retry in {delay:.1f}s",
                    {"error": str(exc), "attempt": attempt + 1},
                    level="WARNING",
                )
                await asyncio.sleep(delay)
        raise last_error  # pragma: no cover

    async def _execute_agent_once(self, role: AgentRole, input_data: Any) -> Dict[str, Any]:
        if role not in self.agents:
            raise RuntimeError(f"Agent {role} not registered.")

        if self.total_cost >= self.budget_limit:
            self._log_event(
                "BUDGET_EXCEEDED",
                f"Current cost ${self.total_cost} exceeds limit ${self.budget_limit}",
            )
            raise RuntimeError("Budget exceeded")

        agent = self.agents[role]
        request = AgentMessage(
            message_id=str(uuid.uuid4()),
            sender=AgentRole.ORCHESTRATOR,
            receiver=role,
            message_type=MessageType.REQUEST,
            payload=Payload(data={"input": input_data}),
        )

        self._log_event("PLAN", f"Planning execution for {role.value}", {"input": input_data})
        self._log_event("AGENT_START", f"Activating {role.value}", {"input": input_data})

        try:
            start_time = time.time()
            response = await asyncio.wait_for(
                agent.handle_request(request), timeout=self.agent_timeout_sec
            )
            latency = time.time() - start_time

            agent_cost = float(getattr(response, "cost", 0.0) or 0.0)
            self.total_cost += agent_cost

            current_version = self.state.get_version()
            self._safe_state_update(
                agent=role,
                key=f"{role.value}_output",
                value=response.payload.data,
                message_type=MessageType.RESPONSE,
                expected_version=current_version,
            )

            self.critical_path.append({"agent": role.value, "latency": latency, "cost": agent_cost})
            self._log_event("OBSERVE", f"Observed output from {role.value}", {"output": response.payload.data})
            self._log_event(
                "DECIDE",
                f"Committing decision for {role.value} to state",
                {"version": self.state.get_version()},
            )
            self._log_event(
                "AGENT_SUCCESS",
                f"{role.value} completed",
                {"output": response.payload.data, "cost": agent_cost},
            )
            return response.payload.data
        except asyncio.TimeoutError:
            self._log_event(
                "AGENT_TIMEOUT",
                f"{role.value} stalled for >{self.agent_timeout_sec}s",
                level="WARNING",
            )
            raise RuntimeError(f"Agent {role.value} timed out.")

    def _safe_state_update(
        self,
        *,
        agent: AgentRole,
        key: str,
        value: Any,
        message_type: MessageType,
        expected_version: Optional[int] = None,
    ) -> None:
        """State commit with optional file lock and dead-letter on contract failure."""
        try:
            with state_commit_lock():
                self.state.update_state(
                    agent=agent,
                    key=key,
                    value=value,
                    message_type=message_type,
                    expected_version=expected_version,
                )
        except ValidationError as exc:
            append_dead_letter(
                agent=agent.value,
                error=str(exc),
                payload=value,
                context={"key": key, "kind": "validation_error"},
            )
            self._log_event(
                "DEAD_LETTER",
                f"Contract validation failed for {key}",
                {"agent": agent.value, "error": str(exc)},
                level="WARNING",
            )
        except ValueError as exc:
            if "contract" in str(exc).lower() or "Data contract" in str(exc):
                append_dead_letter(
                    agent=agent.value,
                    error=str(exc),
                    payload=value,
                    context={"key": key, "kind": "data_contract"},
                )
                self._log_event(
                    "DEAD_LETTER",
                    f"Data contract blocked {key}",
                    {"agent": agent.value, "error": str(exc)},
                    level="WARNING",
                )
                return
            raise

    def _commit_intelligence_to_state(self, intel: Dict[str, Any]) -> None:
        """Validate crawler output and persist under competitor_data."""
        if not intel or "merchant" not in intel:
            return
        try:
            validated = validate_intelligence_output(sanitize_crawler_payload(intel))
            payload = validated.model_dump()
        except ValidationError as exc:
            append_dead_letter(
                agent=AgentRole.CRAWLER.value,
                error=str(exc),
                payload=intel,
                context={"stage": "intelligence_contract"},
            )
            self._log_event(
                "CONTRACT_REJECT",
                f"Intelligence contract failed: {exc}",
                level="WARNING",
            )
            return
        except Exception as exc:
            self._log_event(
                "CONTRACT_REJECT",
                f"Intelligence contract failed: {exc}",
                level="WARNING",
            )
            return

        slug = payload.get("merchant_slug") or normalize_query(str(payload.get("merchant", "")))
        competitor_data = self.state.get_data("competitor_data")
        if not isinstance(competitor_data, dict):
            competitor_data = {}
        competitor_data[slug] = payload
        self._safe_state_update(
            agent=AgentRole.CRAWLER,
            key="competitor_data",
            value=competitor_data,
            message_type=MessageType.RESPONSE,
            expected_version=self.state.get_version(),
        )

    async def _handle_conflicts(self, analysis: Dict, strategy: Dict) -> Dict:
        risk_level = str(analysis.get("risk_level", "LOW")).upper()
        priority = str(strategy.get("priority", "LOW")).upper()
        strategy["aligned_with_analyst"] = risk_level == priority

        if risk_level == "HIGH" and priority == "LOW":
            self._log_event("CONFLICT_DETECTED", "Analyst (HIGH risk) vs Strategist (LOW priority)")

            strategy["priority"] = "HIGH"
            strategy["recommendation"] = (strategy.get("recommendation") or "") + (
                " (REVISED: Priority elevated due to Analyst High Risk flag)"
            )

            resolution_data = {
                "conflict": "Risk vs Priority Mismatch",
                "resolution": "Analyst risk assessment overruled Strategist priority.",
                "reasoning": (
                    f"Evidence: Competitor has {analysis.get('competitor_rate')} vs client "
                    f"{analysis.get('client_rate')}. Risk Tier: {risk_level}."
                ),
                "version": self.state.get_version(),
            }
            self._safe_state_update(
                agent=AgentRole.ORCHESTRATOR,
                key="conflicts",
                value=resolution_data,
                message_type=MessageType.ESCALATION,
                expected_version=self.state.get_version(),
            )
            self._log_event("CONFLICT_RESOLVED", "Risk-First Priority Applied", resolution_data)
            strategy["aligned_with_analyst"] = risk_level == str(strategy.get("priority", "")).upper()

        return strategy

    def _apply_crawl_feedback(
        self,
        crawler_data: Dict[str, Any],
        analysis_data: Dict[str, Any],
        strategy_data: Optional[Dict[str, Any]],
    ) -> None:
        crawler = self.agents.get(AgentRole.CRAWLER)
        if not crawler or not hasattr(crawler, "apply_pipeline_feedback"):
            return
        slug = crawler_data.get("merchant_slug") or crawler_data.get("query")
        if not slug and crawler_data.get("merchant"):
            slug = normalize_query(str(crawler_data["merchant"]))
        if not slug:
            return
        priority = strategy_data.get("priority") if strategy_data else None
        decision = crawler.apply_pipeline_feedback(
            str(slug),
            analyst_risk=analysis_data.get("risk_level"),
            strategist_priority=priority,
        )
        if decision:
            print(
                f"   [Scheduler] Autonomous boost → {slug}: "
                f"{decision.mode} every {decision.interval_sec}s ({decision.reason})"
            )

    def _log_event(
        self,
        event_type: str,
        message: str,
        data: Optional[Dict] = None,
        level: str = "INFO",
    ) -> None:
        log_entry = {
            "timestamp": time.time(),
            "event": event_type,
            "message": message,
            "metadata": data or {},
            "cumulative_cost": self.total_cost,
            "active_version": self.state.get_version(),
        }
        if level == "INFO":
            logger.info(json.dumps(log_entry))
        elif level == "WARNING":
            logger.warning(json.dumps(log_entry))
        else:
            logger.error(json.dumps(log_entry))
