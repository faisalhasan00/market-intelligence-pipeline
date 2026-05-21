It should be designed as:

Autonomous Market Intelligence Collection Agent

That changes its responsibilities completely.

Core Mission of the CrawlerAgent

The crawler’s job is:

continuously collect trustworthy,
evidence-backed competitive market intelligence
from the commerce ecosystem

Everything it does should support that mission.

MAIN TASKS OF THE CRAWLER AGENT
1. Website Discovery & Navigation

The crawler must:

open competitor websites
navigate merchant pages
handle dynamic JS websites
search merchants
follow campaign pages
detect redirects

Example:

CashKaro → Myntra page
GoPaisa → Amazon page
2. Dynamic Rendering

Modern sites use:

React
Vue
Angular

Crawler must:

render JS fully
wait for lazy loading
scroll dynamically
trigger UI interactions

This is why Playwright exists.

3. Anti-Bot Bypass

VERY important.

Crawler must:

rotate proxies
spoof browser fingerprints
avoid Cloudflare detection
handle captchas
simulate human behavior

Without this:
system dies quickly.

4. Data Extraction

Core responsibility.

Extract:

cashback %
coupon codes
bank offers
banners
sale campaigns
expiry dates
exclusive deals
category promotions

From:

DOM
HTML
images
screenshots
5. Deterministic Parsing

Before AI:

regex extraction
CSS selectors
XPath parsing
DOM analysis

Example:

10% Cashback
SAVE500

This should NOT require LLMs.

6. Semantic AI Extraction

ONLY when deterministic parsing fails.

LLM handles:

ambiguous offers
messy campaign text
semantic classification
offer categorization

Example:

festival campaign
new user bonus
wallet-based cashback
7. Screenshot Evidence Collection

VERY important.

Crawler must:

capture screenshots
timestamp evidence
hash screenshots
store immutable evidence

Purpose:

auditing
analyst validation
conflict resolution
8. OCR & Visual Intelligence

Some offers exist ONLY visually.

Crawler should:

OCR banners
read hero images
detect visual campaigns
compare screenshots

Example:

"BIG BILLION DAYS"

may exist only inside image banners.

9. Confidence Scoring

Crawler must estimate:

{
  "confidence": 0.91
}

Based on:

selector quality
OCR match
captcha detection
historical consistency
DOM stability
10. Change Detection

VERY important.

Instead of reprocessing everything:

detect cashback changes
detect banner changes
detect campaign changes
detect new offers

Example:

5% → 8% cashback

This creates intelligence events.

11. Market Event Detection

Crawler should emit:

campaign_started
cashback_spike_detected
exclusive_offer_detected

This feeds downstream agents.

12. Adaptive Monitoring

Crawler decides:

what to crawl
when to crawl
crawl frequency

Example:

Myntra during sale week:
every 10 minutes
13. Historical Intelligence Collection

Crawler stores:

past offers
cashback history
campaign timelines
competitor changes
category trends

This becomes your moat.

14. Source Reliability Tracking

Crawler learns:

which sites noisy
which selectors fail
which proxies work
which merchants change DOM often

Self-improving behavior.

15. Distributed Crawling

Crawler system should:

distribute tasks
manage workers
balance load
reuse browsers

At scale:

1000+ concurrent crawl jobs
16. Session & Identity Management

Crawler should manage:

cookies
localStorage
login sessions
geo targeting
device fingerprints

Some offers differ by:

user
region
login state
17. Evidence Validation

Crawler should compare:

DOM
OCR
screenshot
historical patterns

If mismatch:

raise anomaly
18. Category Intelligence Collection

Crawler should understand:

fashion merchants
electronics merchants
travel merchants

Each behaves differently.

19. Autonomous Surveillance

Crawler should NOT always wait for commands.

Eventually:

continuously monitor market autonomously

Like:

market radar
surveillance engine
20. Cost Optimization

Crawler must intelligently balance:

proxy cost
browser cost
AI token cost
crawl priority

Very important at scale.

SIMPLE WAY TO THINK ABOUT IT

Your CrawlerAgent is basically:

Autonomous market surveillance infrastructure

NOT:

simple scraping bot
FINAL ROLE OF THE CRAWLER

The crawler is responsible for:

collecting, validating, monitoring,
tracking, and continuously updating
market intelligence with evidence
and confidence scoring

## Implementation status

See [COMPLETION.md](COMPLETION.md) for the full checklist (~100% of spec items implemented).

Quick operator entry points:

- `python -m agents.crawler --surveillance` — 24/7 radar
- `python -m agents.crawler.worker` — distributed task worker
- `python -m agents.crawler --serve-metrics --port 9090` — Prometheus HTTP