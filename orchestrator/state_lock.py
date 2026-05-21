"""
Cross-process lock for orchestrator state commits.

Enable with ORCHESTRATOR_STATE_LOCK=true. Uses an exclusive lock file under
logs/orchestrator.state.lock. Recommended: single writer per deployment.
"""
from __future__ import annotations

import os
import time
from contextlib import contextmanager
from typing import Iterator, Optional

DEFAULT_LOCK_PATH = "logs/orchestrator.state.lock"
DEFAULT_TIMEOUT_SEC = 30.0
DEFAULT_POLL_SEC = 0.05


def _lock_enabled() -> bool:
    return os.getenv("ORCHESTRATOR_STATE_LOCK", "false").lower() == "true"


@contextmanager
def state_commit_lock(
    *,
    enabled: Optional[bool] = None,
    path: Optional[str] = None,
    timeout_sec: Optional[float] = None,
) -> Iterator[None]:
    use_lock = _lock_enabled() if enabled is None else enabled
    if not use_lock:
        yield
        return

    lock_path = path or os.getenv("ORCHESTRATOR_STATE_LOCK_PATH", DEFAULT_LOCK_PATH)
    timeout = timeout_sec if timeout_sec is not None else float(
        os.getenv("ORCHESTRATOR_STATE_LOCK_TIMEOUT_SEC", str(DEFAULT_TIMEOUT_SEC))
    )
    os.makedirs(os.path.dirname(lock_path) or ".", exist_ok=True)

    fd: Optional[int] = None
    deadline = time.monotonic() + timeout
    while True:
        try:
            if os.name == "nt":
                import msvcrt

                fd = os.open(lock_path, os.O_CREAT | os.O_RDWR)
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fd = os.open(lock_path, os.O_CREAT | os.O_RDWR)
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except (OSError, BlockingIOError):
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass
                fd = None
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Could not acquire state lock: {lock_path}")
            time.sleep(DEFAULT_POLL_SEC)

    try:
        yield
    finally:
        if fd is not None:
            try:
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)
