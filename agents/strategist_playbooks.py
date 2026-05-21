"""
Merchant vertical playbooks for strategist tone and negotiation angles.

Enable via STRATEGIST_USE_PLAYBOOKS=true.

Runtime overrides:
- STRATEGIST_PLAYBOOK_JSON: JSON object ``{"slug": {"tone": "..."}}`` merged into playbooks.
- STRATEGIST_PLAYBOOK_SLUGS: comma-separated ``slug:base_slug`` aliases or ``slug:{...}`` inline JSON.
"""
from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

# slug → playbook (tone + angles for LLM prompt injection)
MERCHANT_PLAYBOOKS: Dict[str, Dict[str, Any]] = {
    "myntra": {
        "vertical": "fashion",
        "tone": "Trend-led, seasonal urgency; emphasize exclusivity and influencer campaigns.",
        "negotiation_angle": "Match festive sale cashback; bundle with fashion bank offers.",
        "sample_actions": [
            "Request parity on Big Fashion Days cashback tiers",
            "Propose co-branded style sale landing page",
        ],
    },
    "ajio": {
        "vertical": "fashion",
        "tone": "Youth-focused, fast-fashion velocity; highlight flash sales and BNPL.",
        "negotiation_angle": "Counter with limited-time elevated cashback on Ajio-exclusive SKUs.",
        "sample_actions": [
            "Negotiate weekend flash cashback bump",
            "Align on premium brand capsule exclusives",
        ],
    },
    "nykaa": {
        "vertical": "beauty",
        "tone": "Premium beauty, loyalty-driven; reference membership and gift-with-purchase.",
        "negotiation_angle": "Protect beauty margin with tiered cashback on high-AOV carts.",
        "sample_actions": [
            "Match competitor on top beauty brands only",
            "Offer GrabOn-exclusive vanity coupon stack",
        ],
    },
    "amazon": {
        "vertical": "general_marketplace",
        "tone": "Scale and Prime ecosystem; focus on category-wide defection risk.",
        "negotiation_angle": "Category-specific cashback rather than platform-wide match.",
        "sample_actions": [
            "Escalate category lead for electronics/fashion sub-vertical",
            "Request sponsored placement on deal pages",
        ],
    },
    "flipkart": {
        "vertical": "electronics",
        "tone": "Value and EMI-led; Big Billion Days cadence; price-match sensitivity.",
        "negotiation_angle": "Electronics SKU-level parity with bank + exchange stack messaging.",
        "sample_actions": [
            "Match BBD headline cashback on top 20 SKUs",
            "Co-fund exchange bonus with merchant marketing fund",
        ],
    },
    "default": {
        "vertical": "general",
        "tone": "Professional B2B partnership; data-backed competitive gap.",
        "negotiation_angle": "Structured cashback review with 30-day performance checkpoint.",
        "sample_actions": [
            "Schedule partnership review with category manager",
            "Propose A/B test on elevated cashback cohort",
        ],
    },
}

_SLUG_ALIASES: Dict[str, str] = {}
_RUNTIME_PLAYBOOKS: Optional[Dict[str, Dict[str, Any]]] = None


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def _parse_slug_aliases(raw: str) -> Dict[str, str]:
    aliases: Dict[str, str] = {}
    for part in raw.split(","):
        part = part.strip()
        if not part or ":" not in part:
            continue
        slug, target = part.split(":", 1)
        slug, target = slug.strip().lower(), target.strip()
        if slug and target and not target.startswith("{"):
            aliases[slug] = target.lower()
    return aliases


def _parse_inline_slug_overrides(raw: str) -> Dict[str, Dict[str, Any]]:
    overrides: Dict[str, Dict[str, Any]] = {}
    for part in raw.split(","):
        part = part.strip()
        if not part or ":" not in part:
            continue
        slug, payload = part.split(":", 1)
        slug = slug.strip().lower()
        payload = payload.strip()
        if payload.startswith("{"):
            try:
                overrides[slug] = json.loads(payload)
            except json.JSONDecodeError:
                pass
    return overrides


def _load_runtime_playbooks() -> Dict[str, Dict[str, Any]]:
    global _RUNTIME_PLAYBOOKS, _SLUG_ALIASES
    if _RUNTIME_PLAYBOOKS is not None:
        return _RUNTIME_PLAYBOOKS

    merged: Dict[str, Dict[str, Any]] = {
        k: dict(v) for k, v in MERCHANT_PLAYBOOKS.items()
    }

    json_raw = os.getenv("STRATEGIST_PLAYBOOK_JSON", "").strip()
    if json_raw:
        try:
            env_pb = json.loads(json_raw)
            if isinstance(env_pb, dict):
                for slug, pb in env_pb.items():
                    key = str(slug).strip().lower()
                    if isinstance(pb, dict):
                        base = merged.get(key, dict(MERCHANT_PLAYBOOKS["default"]))
                        merged[key] = _deep_merge(base, pb)
        except json.JSONDecodeError:
            pass

    slugs_raw = os.getenv("STRATEGIST_PLAYBOOK_SLUGS", "").strip()
    if slugs_raw:
        _SLUG_ALIASES.update(_parse_slug_aliases(slugs_raw))
        for slug, inline in _parse_inline_slug_overrides(slugs_raw).items():
            base = merged.get(slug, dict(MERCHANT_PLAYBOOKS["default"]))
            merged[slug] = _deep_merge(base, inline)

    _RUNTIME_PLAYBOOKS = merged
    return merged


def reset_playbook_cache() -> None:
    """Clear cached env merges (for tests)."""
    global _RUNTIME_PLAYBOOKS, _SLUG_ALIASES
    _RUNTIME_PLAYBOOKS = None
    _SLUG_ALIASES.clear()


def get_playbook(slug: Optional[str]) -> Dict[str, Any]:
    """Resolve playbook for slug with env overrides and keyword fallbacks."""
    playbooks = _load_runtime_playbooks()
    key = (slug or "").strip().lower()
    if key in _SLUG_ALIASES:
        alias = _SLUG_ALIASES[key]
        if alias in playbooks:
            return dict(playbooks[alias])
    if key in playbooks:
        return dict(playbooks[key])
    if any(k in key for k in ("myntra", "ajio", "fashion", "style")):
        return dict(playbooks.get("myntra", MERCHANT_PLAYBOOKS["myntra"]))
    if any(k in key for k in ("flipkart", "electronics", "mobile", "laptop")):
        return dict(playbooks.get("flipkart", MERCHANT_PLAYBOOKS["flipkart"]))
    if any(k in key for k in ("nykaa", "beauty", "cosmetic")):
        return dict(playbooks.get("nykaa", MERCHANT_PLAYBOOKS["nykaa"]))
    return dict(playbooks.get("default", MERCHANT_PLAYBOOKS["default"]))


def playbook_for_slug(slug: Optional[str]) -> Dict[str, Any]:
    return get_playbook(slug)


def playbooks_enabled() -> bool:
    return os.getenv("STRATEGIST_USE_PLAYBOOKS", "true").lower() in ("1", "true", "yes")
