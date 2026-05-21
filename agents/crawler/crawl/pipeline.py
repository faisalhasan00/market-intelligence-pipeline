"""Shared per-target intelligence pipeline (scrape → extract → visual → record)."""
from __future__ import annotations

from typing import Any, Dict, List, Tuple

from agents.crawler.crawl.collector import PageSnapshot, PlaywrightCollector
from agents.crawler.extraction.confidence import score_source
from agents.crawler.extraction.extractors import maybe_llm_extract
from agents.crawler.platform.memory import CrawlerMemory
from agents.crawler.crawl.self_healing import extract_with_healing
from agents.crawler.crawl.profiles import CrawlTarget
from agents.crawler.platform.store import IntelligenceStore
from agents.crawler.crawl.navigation import discover_campaign_urls
from agents.crawler.crawl.visual_intelligence import analyze_screenshot, should_run_visual, vision_enabled

GREEN = "\033[92m"
YELLOW = "\033[93m"
MAGENTA = "\033[95m"
RESET = "\033[0m"


async def process_target(
    agent,
    store: IntelligenceStore,
    collector: PlaywrightCollector,
    target: CrawlTarget,
    merchant_name: str,
    merchant_slug: str,
) -> Tuple[Dict[str, Any], float]:
    """Returns (source_record, cost_usd)."""
    cost = 0.0
    antibot = getattr(agent, "antibot", None)
    if antibot and antibot.should_skip(target.source_name):
        return _cooldown_record(target, merchant_name, merchant_slug), 0.0

    snap: PageSnapshot = await collector.fetch(
        target.source_name, target.url, merchant_slug=merchant_slug
    )
    if snap.blocked and antibot:
        snap = await antibot.try_recover(
            collector,
            source_name=target.source_name,
            url=target.url,
            snap=snap,
            merchant_slug=merchant_slug,
        )

    if getattr(agent, "_follow_links", False) and snap.html and not snap.blocked:
        snap, nav_cost = await _enrich_with_campaign_pages(
            agent,
            collector,
            antibot,
            snap,
            target,
            merchant_slug,
        )
        cost += nav_cost

    if snap.blocked and snap.error and "captcha" in (snap.raw_text or "").lower():
        store.insert_events(merchant_slug, [{
            "type": "captcha_detected",
            "source": target.source_name,
            "merchant": merchant_slug,
        }])

    prev_snap = store.get_snapshots(merchant_slug).get(target.source_name, {})
    memory = CrawlerMemory(store)

    extracted, parser_events = extract_with_healing(
        memory,
        merchant_slug=merchant_slug,
        merchant_name=merchant_name,
        source_name=target.source_name,
        raw_text=snap.raw_text,
        html=snap.html,
    )
    budget = getattr(agent, "budget", None)
    llm_cost = 0.0
    if budget is None or budget.can_use_llm():
        extracted, llm_cost = await maybe_llm_extract(
            agent,
            raw_text=snap.raw_text,
            url=snap.url,
            merchant=merchant_name,
            source_name=target.source_name,
            deterministic_result=extracted,
        )
        if budget and llm_cost > 0:
            budget.record_llm()
    elif budget and budget.should_conserve():
        print(f"   [Budget] LLM skipped for {target.source_name}")
    cost += llm_cost

    offers: List[str] = list(extracted.get("offers") or [])
    cashback = extracted.get("cashback_rate")
    visual_events: List[Dict[str, Any]] = []
    visual_intel: Dict[str, Any] = {}

    run_vision = (
        snap.evidence
        and vision_enabled()
        and should_run_visual(dom_offer_count=len(offers))
    )
    if budget is not None and not budget.can_use_vision(merchant_slug):
        run_vision = False
        if snap.evidence and vision_enabled():
            print(f"   [Budget] Vision skipped for {target.source_name}")
    if run_vision:
        vi, vcost = await analyze_screenshot(
            snap.evidence.path,
            merchant=merchant_name,
            source_name=target.source_name,
            hero_path=snap.evidence.hero_path,
            previous_phash=prev_snap.get("perceptual_hash"),
            previous_hero_phash=prev_snap.get("hero_perceptual_hash"),
        )
        cost += vcost
        if budget:
            budget.record_vision(merchant_slug)
        visual_intel = vi.to_dict()

        if vi.offers:
            offers = list(dict.fromkeys(offers + vi.offers))[:30]
        if not cashback and vi.cashback_rate:
            cashback = vi.cashback_rate
            extracted["cashback_rate"] = cashback

        if vi.comparison and vi.comparison.visually_changed and prev_snap.get("perceptual_hash"):
            visual_events.append({
                "type": "visual_campaign_changed",
                "source": target.source_name,
                "merchant": merchant_slug,
                "hamming_distance": vi.comparison.hamming_distance,
                "detector": "perceptual_hash",
            })
        if vi.comparison and vi.comparison.hero_changed:
            visual_events.append({
                "type": "hero_banner_changed",
                "source": target.source_name,
                "merchant": merchant_slug,
                "hero_hamming": vi.comparison.hero_hamming,
                "detector": "hero_phash",
            })
        if vi.sale_detected:
            visual_events.append({
                "type": "visual_sale_detected",
                "source": target.source_name,
                "merchant": merchant_slug,
                "headline": vi.hero_headline,
                "detector": "visual_intelligence",
            })
        if vi.campaign_detected and vi.campaigns:
            visual_events.append({
                "type": "visual_campaign_detected",
                "source": target.source_name,
                "merchant": merchant_slug,
                "campaigns": vi.campaigns[:5],
                "detector": "visual_intelligence",
            })
        dom_offer_set = set(extracted.get("offers") or [])
        for offer in vi.offers[:8]:
            if offer not in dom_offer_set:
                visual_events.append({
                    "type": "image_only_offer_detected",
                    "source": target.source_name,
                    "merchant": merchant_slug,
                    "offer": offer[:160],
                    "detector": "visual_intelligence",
                })

        if vi.offers or vi.campaigns:
            print(
                f"   {MAGENTA}[Visual] {target.source_name}: "
                f"{len(vi.offers)} image offers, {len(vi.campaigns)} campaigns{RESET}"
            )

    if visual_events:
        store.insert_events(merchant_slug, visual_events)

    method = extracted.get("extraction", {}).get("method", "deterministic")
    if visual_intel:
        method = "deterministic+visual" if method == "deterministic" else f"{method}+visual"

    blocked = snap.blocked or extracted.get("blocked", False)
    success = len(offers) > 0 and not blocked

    rel_score = store.update_source_reliability(
        target.source_name, success=success, blocked=blocked
    )

    conf = extracted.get("extraction", {}).get("confidence")
    if conf is None:
        conf = score_source(
            method=method,
            offers=offers,
            blocked=blocked,
            raw_text_len=len(snap.raw_text),
        )
    if visual_intel.get("offers"):
        conf = min(0.95, conf + 0.12)
    extracted["extraction"] = {"method": method, "confidence": conf}

    record: Dict[str, Any] = {
        "source_name": target.source_name,
        "target_type": target.target_type,
        "url": snap.url,
        "merchant": merchant_name,
        "cashback_rate": cashback,
        "offers": offers,
        "blocked": blocked,
        "extraction": extracted.get("extraction"),
        "status_code": snap.status_code if not blocked else 403,
        "reliability_score": rel_score,
    }
    if snap.evidence:
        record["evidence"] = snap.evidence.to_dict()
    if visual_intel:
        record["visual_intelligence"] = visual_intel
    if snap.error:
        record["error"] = snap.error
    record["_raw_text"] = snap.raw_text
    record["_html"] = snap.html
    if parser_events:
        record["_parser_events"] = parser_events

    if offers:
        print(f"   {GREEN}✓ {target.source_name}: {len(offers)} offers (conf={conf:.2f}, {method}){RESET}")
    else:
        print(f"   {YELLOW}! {target.source_name}: empty/blocked{RESET}")

    return record, cost


def _cooldown_record(
    target: CrawlTarget,
    merchant_name: str,
    merchant_slug: str,
) -> Dict[str, Any]:
    return {
        "source_name": target.source_name,
        "target_type": target.target_type,
        "url": target.url,
        "merchant": merchant_name,
        "cashback_rate": None,
        "offers": [],
        "blocked": True,
        "extraction": {"method": "skipped", "confidence": 0.0},
        "status_code": 403,
        "reliability_score": 0.0,
        "error": "source_in_cooldown",
        "_raw_text": "",
        "_html": "",
    }


async def _enrich_with_campaign_pages(
    agent,
    collector: PlaywrightCollector,
    antibot,
    snap: PageSnapshot,
    target: CrawlTarget,
    merchant_slug: str,
) -> Tuple[PageSnapshot, float]:
    extra_cost = 0.0
    urls = discover_campaign_urls(snap.html, snap.url, merchant_slug)
    if not urls:
        return snap, extra_cost

    combined_text = snap.raw_text or ""
    combined_html = snap.html or ""
    for campaign_url in urls:
        print(f"   [Nav] {target.source_name} → {campaign_url[:90]}")
        csnap = await collector.fetch(
            target.source_name, campaign_url, merchant_slug=merchant_slug
        )
        if csnap.blocked and antibot:
            csnap = await antibot.try_recover(
                collector,
                source_name=target.source_name,
                url=campaign_url,
                snap=csnap,
                merchant_slug=merchant_slug,
            )
        if not csnap.blocked:
            combined_text = f"{combined_text}\n{csnap.raw_text or ''}"
            combined_html = f"{combined_html}\n{csnap.html or ''}"

    return PageSnapshot(
        source_name=snap.source_name,
        url=snap.url,
        raw_text=combined_text[:12000],
        status_code=snap.status_code,
        blocked=snap.blocked,
        evidence=snap.evidence,
        html=combined_html[:100000],
        error=snap.error,
        proxy_used=snap.proxy_used,
    ), extra_cost
