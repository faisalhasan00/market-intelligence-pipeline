"""Session / login — Playwright storage state per merchant slug."""
from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional, Tuple

SESSION_DIR = os.path.join("data", "sessions")


def login_enabled() -> bool:
    return os.getenv("CRAWLER_LOGIN_ENABLED", "false").lower() == "true"


def session_path(merchant_slug: str) -> str:
    slug = merchant_slug.lower().strip()
    os.makedirs(SESSION_DIR, exist_ok=True)
    return os.path.join(SESSION_DIR, f"{slug}.json")


def load_storage_state(merchant_slug: str) -> Optional[Dict[str, Any]]:
    path = session_path(merchant_slug)
    if not os.path.isfile(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def save_storage_state(merchant_slug: str, state: Dict[str, Any]) -> None:
    path = session_path(merchant_slug)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f)
    os.replace(tmp, path)


def get_credentials(merchant_slug: str) -> Optional[Tuple[str, str]]:
    """Per-slug CRAWLER_LOGIN_{SLUG}_USER/PASS or generic CRAWLER_LOGIN_USER/PASS."""
    key = merchant_slug.upper().replace("-", "_")
    user = os.getenv(f"CRAWLER_LOGIN_{key}_USER") or os.getenv("CRAWLER_LOGIN_USER")
    password = os.getenv(f"CRAWLER_LOGIN_{key}_PASS") or os.getenv("CRAWLER_LOGIN_PASS")
    if user and password:
        return user, password
    return None


def login_url_for(merchant_slug: str) -> Optional[str]:
    key = merchant_slug.upper().replace("-", "_")
    return os.getenv(f"CRAWLER_LOGIN_{key}_URL") or os.getenv("CRAWLER_LOGIN_URL")


def storage_state_for_context(merchant_slug: str) -> Optional[Dict[str, Any]]:
    if not login_enabled():
        return None
    return load_storage_state(merchant_slug)


async def persist_context_state(context: Any, merchant_slug: str) -> None:
    if not login_enabled():
        return
    try:
        state = await context.storage_state()
        save_storage_state(merchant_slug, state)
    except Exception:
        pass


async def try_form_login(page: Any, merchant_slug: str) -> bool:
    """Best-effort generic login; returns True if navigation suggests success."""
    creds = get_credentials(merchant_slug)
    if not creds:
        return False
    user, password = creds
    login_url = login_url_for(merchant_slug)
    if login_url:
        try:
            await page.goto(login_url, wait_until="domcontentloaded", timeout=25000)
        except Exception:
            return False

    selectors_user = (
        'input[type="email"]',
        'input[name="email"]',
        'input[name="username"]',
        'input[id*="email" i]',
        'input[id*="user" i]',
    )
    selectors_pass = ('input[type="password"]', 'input[name="password"]')
    selectors_submit = (
        'button[type="submit"]',
        'input[type="submit"]',
        'button:has-text("Sign in")',
        'button:has-text("Log in")',
        'button:has-text("Login")',
    )

    filled = False
    for sel in selectors_user:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                await loc.fill(user)
                filled = True
                break
        except Exception:
            continue
    if not filled:
        return False

    for sel in selectors_pass:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                await loc.fill(password)
                break
        except Exception:
            continue

    for sel in selectors_submit:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                await loc.click()
                await page.wait_for_timeout(3000)
                return True
        except Exception:
            continue
    return False


def apply_session_to_context(context_options: Dict[str, Any], merchant_slug: str) -> Dict[str, Any]:
    """Merge saved storage_state into Playwright context options when login enabled."""
    if not login_enabled():
        return context_options
    state = load_storage_state(merchant_slug)
    if state:
        context_options = dict(context_options)
        context_options["storage_state"] = state
    return context_options


def session_status(merchant_slug: Optional[str] = None) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "login_enabled": login_enabled(),
        "session_dir": SESSION_DIR,
    }
    if merchant_slug:
        path = session_path(merchant_slug)
        out["merchant_slug"] = merchant_slug.lower().strip()
        out["session_file"] = path
        out["session_exists"] = os.path.isfile(path)
        out["has_credentials"] = get_credentials(merchant_slug) is not None
    return out
