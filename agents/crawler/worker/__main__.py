"""CLI: python -m agents.crawler.worker"""
from __future__ import annotations

import asyncio
import sys

from agents.crawler.worker import main


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nWorker stopped.", file=sys.stderr)
