"""Playwright acquisition: long-lived browser, context pool, proxy rotation, geo."""
from __future__ import annotations

import asyncio
import os
import random
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, Optional

from agents.crawler.extraction.confidence import is_blocked_text
from agents.crawler.crawl.evidence import EvidenceRecord, save_screenshot
from agents.crawler.crawl.profiles import USER_AGENTS
from agents.crawler.crawl.proxy import ProxyPool
from agents.crawler.platform.geo import browser_geo_context
from agents.crawler.platform.session import (
    apply_session_to_context,
    get_credentials,
    login_enabled,
    persist_context_state,
    try_form_login,
)


@dataclass
class PageSnapshot:
    source_name: str
    url: str
    raw_text: str
    status_code: int
    blocked: bool
    evidence: Optional[EvidenceRecord]
    html: str = ""
    error: Optional[str] = None
    proxy_used: Optional[str] = None


@dataclass
class _PooledContext:
    context: Any
    proxy_key: Optional[str] = None


class PlaywrightCollector:
    """Reuses one Chromium instance and a bounded pool of browser contexts."""

    def __init__(self, proxy_pool: Optional[ProxyPool] = None):
        self._playwright = None
        self._browser = None
        self.proxy_pool = proxy_pool or ProxyPool()
        self._pool: Deque[_PooledContext] = deque()
        self._pool_lock = asyncio.Lock()
        self._max_contexts = int(os.getenv("CRAWLER_CONTEXT_POOL_SIZE", "4"))
        self._geo = browser_geo_context()

    async def __aenter__(self):
        from playwright.async_api import async_playwright
        from playwright_stealth import Stealth

        self._stealth_cls = Stealth
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        if self.proxy_pool.enabled:
            print(f"      [Collector] Proxy pool: {len(self.proxy_pool._proxies)} endpoints")
        print(f"      [Collector] Geo={self._geo.get('geo')} locale={self._geo.get('locale')}")
        return self

    async def __aexit__(self, *args):
        async with self._pool_lock:
            while self._pool:
                pooled = self._pool.popleft()
                try:
                    await pooled.context.close()
                except Exception:
                    pass
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    def _context_options(
        self,
        proxy_cfg: Optional[Dict[str, str]],
        merchant_slug: Optional[str] = None,
    ) -> Dict[str, Any]:
        opts: Dict[str, Any] = {
            "viewport": {"width": 1280, "height": 800},
            "user_agent": random.choice(USER_AGENTS),
            "locale": self._geo["locale"],
            "timezone_id": self._geo["timezone_id"],
        }
        if proxy_cfg:
            opts["proxy"] = proxy_cfg
        if merchant_slug and login_enabled():
            opts = apply_session_to_context(opts, merchant_slug)
        return opts

    async def _acquire_context(
        self,
        proxy_cfg: Optional[Dict[str, str]],
        merchant_slug: Optional[str] = None,
    ) -> _PooledContext:
        proxy_key = (proxy_cfg or {}).get("server")
        if not login_enabled():
            async with self._pool_lock:
                for i, pooled in enumerate(self._pool):
                    if pooled.proxy_key == proxy_key:
                        del self._pool[i]
                        return pooled
        if not self._browser:
            raise RuntimeError("Browser not initialized")
        context = await self._browser.new_context(
            **self._context_options(proxy_cfg, merchant_slug)
        )
        return _PooledContext(context=context, proxy_key=proxy_key)

    async def _release_context(
        self,
        pooled: _PooledContext,
        merchant_slug: Optional[str] = None,
    ) -> None:
        if login_enabled():
            try:
                await pooled.context.close()
            except Exception:
                pass
            return
        async with self._pool_lock:
            if len(self._pool) < self._max_contexts:
                self._pool.append(pooled)
                return
        try:
            await pooled.context.close()
        except Exception:
            pass

    async def fetch(
        self,
        source_name: str,
        url: str,
        *,
        merchant_slug: Optional[str] = None,
    ) -> PageSnapshot:
        if not self._browser:
            return PageSnapshot(
                source_name=source_name,
                url=url,
                raw_text="",
                status_code=0,
                blocked=True,
                evidence=None,
                error="Browser not initialized",
            )

        proxy_cfg = self.proxy_pool.next() if self.proxy_pool.enabled else None
        proxy_url = proxy_cfg.get("server") if proxy_cfg else None
        pooled = await self._acquire_context(proxy_cfg, merchant_slug)
        page = await pooled.context.new_page()
        slug = (merchant_slug or "").lower().strip()
        try:
            await self._stealth_cls().apply_stealth_async(page)
            print(f"      [Collector] {url}")
            response = await page.goto(url, wait_until="domcontentloaded", timeout=25000)
            status = response.status if response else 200

            if (
                slug
                and login_enabled()
                and get_credentials(slug)
                and is_blocked_text((await page.evaluate("document.body.innerText") or "")[:2000])
            ):
                if await try_form_login(page, slug):
                    response = await page.goto(url, wait_until="domcontentloaded", timeout=25000)
                    status = response.status if response else status

            await asyncio.sleep(2.5)
            await page.keyboard.press("Escape")
            await asyncio.sleep(0.3)

            for _ in range(3):
                await page.evaluate(f"window.scrollBy(0, {random.randint(350, 750)})")
                await asyncio.sleep(random.uniform(0.4, 1.2))

            raw_text = await page.evaluate("document.body.innerText") or ""
            html = ""
            try:
                html = (await page.content())[:80000]
            except Exception:
                pass
            blocked = is_blocked_text(raw_text) or status >= 400

            if blocked and proxy_url:
                self.proxy_pool.mark_dead(proxy_url)

            evidence = await save_screenshot(page, source_name, url)

            snap = PageSnapshot(
                source_name=source_name,
                url=url,
                raw_text=raw_text[:8000],
                status_code=200 if not blocked else status,
                blocked=blocked,
                evidence=evidence,
                html=html,
                proxy_used=proxy_url,
            )
            if slug and login_enabled() and not blocked:
                await persist_context_state(pooled.context, slug)
            return snap
        except Exception as e:
            if proxy_url:
                self.proxy_pool.mark_dead(proxy_url)
            print(f"      [Collector] Failed {source_name}: {e}")
            return PageSnapshot(
                source_name=source_name,
                url=url,
                raw_text="",
                status_code=0,
                blocked=True,
                evidence=None,
                error=str(e),
                proxy_used=proxy_url,
            )
        finally:
            try:
                await page.close()
            except Exception:
                pass
            await self._release_context(pooled, merchant_slug)
            await asyncio.sleep(random.uniform(0.3, 0.8))
