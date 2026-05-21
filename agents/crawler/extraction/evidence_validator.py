"""Cross-check DOM, visual, and consensus rates before trusting intelligence."""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

from agents.crawler.intelligence.change_detection import parse_rate_pct

MISMATCH_PCT = float(os.getenv("CRAWLER_EVIDENCE_MISMATCH_PCT", "3.0"))
MAX_TRUST_PCT = float(os.getenv("CRAWLER_MAX_CASHBACK_PCT", "30"))


class EvidenceValidator:
    def validate(
        self,
        merchant_slug: str,
        sources: List[Dict[str, Any]],
        consensus: Dict[str, Any],
        *,
        crawl_mode: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        consensus_rate = consensus.get("consensus_rate")
        downgraded = 0
        mismatches = 0

        for src in sources:
            if src.get("blocked"):
                continue
            source_name = src.get("source_name", "")
            dom_rate = parse_rate_pct(src.get("cashback_rate"))
            visual_rate = parse_rate_pct(
                (src.get("visual_intelligence") or {}).get("cashback_rate")
            )
            ext = src.setdefault("extraction", {})
            conf = float(ext.get("confidence") or 0.5)

            if dom_rate and dom_rate > MAX_TRUST_PCT:
                src["requires_revalidation"] = True
                ext["confidence"] = round(conf * 0.4, 3)
                events.append({
                    "type": "evidence_mismatch",
                    "merchant": merchant_slug,
                    "source": source_name,
                    "reason": "impossible_dom_rate",
                    "dom_rate_pct": dom_rate,
                })
                mismatches += 1
                downgraded += 1
                continue

            if consensus_rate is not None and dom_rate is not None:
                dom_delta = abs(dom_rate - consensus_rate)
                visual_delta = (
                    abs(visual_rate - consensus_rate)
                    if visual_rate is not None
                    else None
                )
                if dom_delta >= MISMATCH_PCT:
                    visual_agrees = visual_delta is not None and visual_delta < MISMATCH_PCT
                    if visual_agrees or visual_rate is None:
                        src["requires_revalidation"] = True
                        ext["confidence"] = round(conf * 0.55, 3)
                        events.append({
                            "type": "evidence_mismatch",
                            "merchant": merchant_slug,
                            "source": source_name,
                            "reason": "dom_vs_consensus",
                            "dom_rate_pct": dom_rate,
                            "consensus_rate_pct": consensus_rate,
                            "visual_rate_pct": visual_rate,
                            "delta_pct": round(dom_delta, 2),
                        })
                        mismatches += 1
                        downgraded += 1

            if visual_rate is not None and dom_rate is not None:
                if abs(visual_rate - dom_rate) >= MISMATCH_PCT * 2:
                    events.append({
                        "type": "evidence_mismatch",
                        "merchant": merchant_slug,
                        "source": source_name,
                        "reason": "dom_vs_visual",
                        "dom_rate_pct": dom_rate,
                        "visual_rate_pct": visual_rate,
                    })
                    mismatches += 1

            if crawl_mode in ("critical", "hot") and not src.get("evidence"):
                ext["confidence"] = round(float(ext.get("confidence") or 0.5) * 0.85, 3)
                events.append({
                    "type": "evidence_gap",
                    "merchant": merchant_slug,
                    "source": source_name,
                    "reason": "no_screenshot_on_hot_crawl",
                })

        summary = {
            "mismatches": mismatches,
            "downgraded_sources": downgraded,
            "validated": len(sources) - downgraded,
        }
        return events, summary
