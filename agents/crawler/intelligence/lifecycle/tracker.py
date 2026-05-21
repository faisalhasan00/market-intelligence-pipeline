"""Offer lifecycle stages — started, peaked, declining, removed."""
from __future__ import annotations

from typing import Any, Dict, List

from agents.crawler.intelligence.change_detection import normalize_offer, offer_key
from agents.crawler.platform.store import IntelligenceStore


class OfferLifecycleTracker:
    STAGE_STARTED = "offer_started"
    STAGE_PEAKED = "offer_peaked"
    STAGE_DECLINING = "offer_declining"
    STAGE_REMOVED = "offer_removed"

    def __init__(self, store: IntelligenceStore):
        self.store = store

    def update(
        self,
        merchant_slug: str,
        sources: List[Dict[str, Any]],
        *,
        warmup: bool = False,
    ) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        baseline_offers = 0

        for src in sources:
            source_name = src.get("source_name", "")
            current_keys = {
                offer_key(o): normalize_offer(o)
                for o in (src.get("offers") or [])
                if o and o.strip()
            }
            states = self.store.get_offer_states(merchant_slug, source_name)

            for key, text in current_keys.items():
                st = states.get(key)
                if not st:
                    self.store.upsert_offer_state(
                        merchant_slug, source_name, key, text,
                        stage="active", mention_count=1,
                    )
                    baseline_offers += 1
                    if not warmup:
                        events.append({
                            "type": self.STAGE_STARTED,
                            "merchant": merchant_slug,
                            "source": source_name,
                            "offer_key": key,
                            "offer": text[:160],
                        })
                    continue

                mentions = int(st.get("mention_count") or 0) + 1
                stage = st.get("stage") or "active"
                if warmup:
                    self.store.upsert_offer_state(
                        merchant_slug, source_name, key, text,
                        stage=stage, mention_count=mentions,
                    )
                    continue
                if mentions >= 5 and stage == "active":
                    stage = "peaked"
                    events.append({
                        "type": self.STAGE_PEAKED,
                        "merchant": merchant_slug,
                        "source": source_name,
                        "offer_key": key,
                        "mentions": mentions,
                    })
                elif mentions >= 3 and stage == "peaked":
                    stage = "declining"
                    events.append({
                        "type": self.STAGE_DECLINING,
                        "merchant": merchant_slug,
                        "source": source_name,
                        "offer_key": key,
                    })

                self.store.upsert_offer_state(
                    merchant_slug, source_name, key, text,
                    stage=stage, mention_count=mentions,
                )

            for key, st in states.items():
                if key not in current_keys and st.get("stage") != "removed":
                    if not warmup:
                        events.append({
                            "type": self.STAGE_REMOVED,
                            "merchant": merchant_slug,
                            "source": source_name,
                            "offer_key": key,
                            "offer": (st.get("offer_text") or "")[:160],
                        })
                    self.store.upsert_offer_state(
                        merchant_slug, source_name, key,
                        st.get("offer_text") or "",
                        stage="removed",
                        mention_count=int(st.get("mention_count") or 0),
                    )

        if warmup and baseline_offers:
            events.append({
                "type": "baseline_collected",
                "merchant": merchant_slug,
                "offers_seeded": baseline_offers,
            })

        if events:
            self.store.insert_events(merchant_slug, events)
        return events
