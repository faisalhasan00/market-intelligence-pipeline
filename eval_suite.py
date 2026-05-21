import asyncio
import sys
import time
import json
import os

if sys.platform.startswith("win"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from orchestrator.orchestrator import SwarmOrchestrator
from messaging.schemas import AgentRole
from agents.crawler import CrawlerAgent
from agents.analyst import AnalystAgent
from agents.strategist import StrategistAgent
from agents.alerter import AlertAgent

from state.state_manager import SharedState

class EvalSuite:
    def __init__(self):
        self.state = SharedState()
        self.orchestrator = SwarmOrchestrator(state=self.state)
        self.orchestrator.register_agent(AgentRole.CRAWLER, CrawlerAgent())
        self.orchestrator.register_agent(AgentRole.ANALYST, AnalystAgent())
        self.orchestrator.register_agent(AgentRole.STRATEGIST, StrategistAgent())
        self.orchestrator.register_agent(AgentRole.ALERTER, AlertAgent())
        
        self.results = []

    async def run_test_case(self, name: str, query: str, expected_risk: str):
        print(f"[TESTING] {name}...")
        start_time = time.time()
        
        try:
            # Run the pipeline
            await self.orchestrator.run_pipeline(query)
            
            # Extract results from state
            final_state = self.orchestrator.state.get_data()
            latency = time.time() - start_time
            cost = self.orchestrator.total_cost
            
            # Validation Logic
            analysis = final_state.get("analyst_output") or final_state.get("analysis_results") or {}
            actual_risk = analysis.get("risk_level", "UNKNOWN")
            passed = (actual_risk == expected_risk) if expected_risk != "ANY" else True
            
            report = {
                "test_case": name,
                "status": "PASS" if passed else "FAIL",
                "metrics": {
                    "latency_sec": round(latency, 2),
                    "cost_usd": round(cost, 6),
                    "expected_risk": expected_risk,
                    "actual_risk": actual_risk
                },
                "audit": {
                    "version": self.orchestrator.state.get_version(),
                    "agent_trace": list(final_state.keys())
                }
            }
            self.results.append(report)
            print(f"DONE {name}: {report['status']} (Cost: ${cost:.6f})")
            
        except Exception as e:
            print(f"FAIL {name}: CRASHED - {str(e)}")
            self.results.append({"test_case": name, "status": "CRASH", "error": str(e)})

    async def run_all(self):
        # 1. Test Accuracy & Precision
        await self.run_test_case("High Competition Test", "Analyze Myntra coupons", "HIGH")
        
        # 2. Test Conflict Resolution & Consensus
        await self.run_test_case("Consensus Test", "Analyze Ajio deals", "ANY")
        
        # 3. Test Resilience (Veto logic)
        await self.run_test_case("Veto Recovery Test", "Search for 123456789 NonExistentStore", "UNKNOWN")

        # Save Raw Report
        os.makedirs("reports", exist_ok=True)
        with open("reports/eval_report.json", "w") as f:
            json.dump(self.results, f, indent=4)
        
        print("\n" + "="*50)
        print("EVALUATION COMPLETE")
        print(f"Report saved to: reports/eval_report.json")
        print("="*50)

if __name__ == "__main__":
    suite = EvalSuite()
    asyncio.run(suite.run_all())
