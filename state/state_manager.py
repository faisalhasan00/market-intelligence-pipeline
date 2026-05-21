import threading
from typing import Any, Dict, List, Optional
from datetime import datetime
from messaging.schemas import AgentRole, MessageType
from pydantic import ValidationError
from state.contracts import validate_competitor_entry

class SharedState:
    """
    Centralized State Store with versioning and audit trails.
    Satisfies Requirement: 'Shared state store with optimistic locking or version vectors.'
    """
    def __init__(self):
        self._data: Dict[str, Any] = {
            "competitor_data": {},
            "analysis_results": {},
            "strategy_briefs": {},
            "alerts_dispatched": [],
            "conflicts": []
        }
        self._version: int = 0
        self._history: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

    def update_state(self, agent: AgentRole, key: str, value: Any, message_type: MessageType, expected_version: Optional[int] = None):
        with self._lock:
            # Optimistic locking check
            if expected_version is not None and expected_version != self._version:
                raise ValueError(f"Optimistic Locking Failure: Agent {agent.value} attempted update with version {expected_version}, but current version is {self._version}")
            
            # --- DATA GOVERNANCE ENFORCEMENT ---
            # If the crawler is updating raw competitor data, enforce the Pydantic Contract
            if key == "competitor_data" and isinstance(value, dict):
                try:
                    for target, payload in value.items():
                        if isinstance(payload, dict):
                            validate_competitor_entry(target, payload)
                except ValidationError as e:
                    print(
                        f"   [Data Governance] CRITICAL WARNING: {agent.value} attempted to inject malformed data! "
                        "Blocking state mutation."
                    )
                    print(f"   Contract Violation Details: {e.errors()}")
                    raise ValueError(
                        f"Data contract violation for competitor_data/{target}: {e}"
                    ) from e

            self._version += 1
            self._data[key] = value
            
            # Audit trail
            entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "agent": agent.value,
                "action": message_type.value,
                "key": key,
                "version": self._version
            }
            self._history.append(entry)
            return self._version

    def get_data(self, key: Optional[str] = None) -> Any:
        with self._lock:
            if key:
                return self._data.get(key)
            return self._data

    def get_version(self) -> int:
        return self._version

    def get_history(self) -> List[Dict[str, Any]]:
        return self._history

    def inspect(self):
        """Returns a snapshot for observability."""
        return {
            "version": self._version,
            "data": self._data,
            "history": self._history
        }
