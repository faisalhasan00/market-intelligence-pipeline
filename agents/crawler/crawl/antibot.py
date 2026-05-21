"""Captcha / block recovery — cooldown, proxy rotation, captcha events."""
from __future__ import annotations

import asyncio
import os
import random
from typing import TYPE_CHECKING, Optional

from agents.crawler.crawl.collector import PageSnapshot, PlaywrightCollector
from agents.crawler.crawl.proxy import ProxyPool
from agents.crawler.extraction.confidence import is_blocked_text
from agents.crawler.platform.store import IntelligenceStore

if TYPE_CHECKING:
    pass

YELLOW = "\033[93m"
RESET = "\033[0m"

CAPTCHA_SIGNALS = (
    "captcha",
    "recaptcha",
    "hcaptcha",
    "verify you are human",
    "are you a human",
    "challenge-platform",
)


def is_captcha_text(text: str) -> bool:
    lower = (text or "").lower()
    return any(sig in lower for sig in CAPTCHA_SIGNALS)


def captcha_solver_configured() -> bool:
    """Paid solver stub — only active when API key env is set."""
    return bool(os.getenv("CRAWLER_CAPTCHA_SOLVER_KEY", "").strip())


class AntibotRecovery:
    def __init__(
        self,
        store: IntelligenceStore,
        proxy_pool: ProxyPool,
    ):
        self.store = store
        self.proxy_pool = proxy_pool
        self.cooldown_min = int(os.getenv("CRAWLER_SOURCE_COOLDOWN_MIN", "20"))
        self.captcha_cooldown_min = int(os.getenv("CRAWLER_CAPTCHA_COOLDOWN_MIN", "45"))
        self.retry_delay_sec = float(os.getenv("CRAWLER_ANTIBOT_RETRY_DELAY_SEC", "4.0"))
        self.captcha_retry_delay_sec = float(
            os.getenv("CRAWLER_CAPTCHA_RETRY_DELAY_SEC", "8.0")
        )
        self.double_proxy = (
            os.getenv("CRAWLER_ANTIBOT_DOUBLE_PROXY", "true").lower() == "true"
        )

    def should_skip(self, source_name: str) -> bool:
        return self.store.is_source_in_cooldown(source_name)

    def _is_captcha(self, snap: PageSnapshot) -> bool:
        return is_captcha_text(snap.raw_text or "")

    def _emit_captcha_event(self, merchant_slug: str, source_name: str, snap: PageSnapshot) -> None:
        self.store.insert_events(
            merchant_slug,
            [
                {
                    "type": "captcha_blocked",
                    "source": source_name,
                    "merchant": merchant_slug,
                    "solver_configured": captcha_solver_configured(),
                    "url": snap.url,
                }
            ],
        )

    async def _rotate_proxy(self) -> None:
        if self.proxy_pool.enabled:
            self.proxy_pool.next()

    async def try_recover(
        self,
        collector: PlaywrightCollector,
        *,
        source_name: str,
        url: str,
        snap: PageSnapshot,
        merchant_slug: str,
    ) -> PageSnapshot:
        if not snap.blocked:
            return snap
        if self.should_skip(source_name):
            print(f"   {YELLOW}[Antibot] {source_name} in cooldown — skip retry{RESET}")
            return snap

        captcha = self._is_captcha(snap)
        if captcha:
            self._emit_captcha_event(merchant_slug, source_name, snap)
            if captcha_solver_configured():
                print(
                    f"   {YELLOW}[Antibot] Captcha on {source_name} — solver key set but "
                    f"no paid solver integrated (stub only){RESET}"
                )
            else:
                print(
                    f"   {YELLOW}[Antibot] Captcha on {source_name} — "
                    f"no CRAWLER_CAPTCHA_SOLVER_KEY; retry with longer cooldown{RESET}"
                )

        delay = self.captcha_retry_delay_sec if captcha else self.retry_delay_sec
        if self.proxy_pool.enabled:
            await self._rotate_proxy()
            print(f"   {YELLOW}[Antibot] Rotating proxy, retry {source_name}{RESET}")
        else:
            print(f"   {YELLOW}[Antibot] Retry {source_name} after block{RESET}")

        await asyncio.sleep(delay + random.uniform(0.5, 2.0))
        retry_snap = await collector.fetch(source_name, url, merchant_slug=merchant_slug)

        if retry_snap.blocked and captcha and self.double_proxy and self.proxy_pool.enabled:
            await self._rotate_proxy()
            print(f"   {YELLOW}[Antibot] Second proxy rotation for captcha {source_name}{RESET}")
            await asyncio.sleep(self.captcha_retry_delay_sec * 0.5)
            retry_snap = await collector.fetch(source_name, url, merchant_slug=merchant_slug)

        if retry_snap.blocked:
            cooldown = self.captcha_cooldown_min if captcha else self.cooldown_min
            reason = "captcha_after_retry" if captcha else "block_after_retry"
            self.store.set_source_cooldown(
                source_name,
                minutes=cooldown,
                reason=reason,
            )
            self.store.insert_events(
                merchant_slug,
                [
                    {
                        "type": "source_cooldown",
                        "source": source_name,
                        "merchant": merchant_slug,
                        "minutes": cooldown,
                        "reason": reason,
                    }
                ],
            )
        return retry_snap
