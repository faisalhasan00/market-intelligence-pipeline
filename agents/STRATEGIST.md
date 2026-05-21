# Strategist Agent

Generates merchant negotiation briefs from analyst intelligence (risk, gap, crawler snapshots).

## Run

```bash
# Demo (rules-only without GROQ_API_KEY)
python -m agents.strategist

# Approval queue CLI
python -m agents.strategist --list-pending
python -m agents.strategist --approve <approval_id>
python -m agents.strategist --veto <approval_id>
```

## Environment

| Variable | Default | Purpose |
|----------|---------|---------|
| `STRATEGIST_MODEL` | `llama-3.3-70b-versatile` | Groq model |
| `STRATEGIST_USE_PLAYBOOKS` | `true` | Merchant vertical tone/actions |
| `STRATEGIST_PLAYBOOK_JSON` | — | JSON merge into playbooks, e.g. `{"myntra":{"tone":"Festive push"}}` |
| `STRATEGIST_PLAYBOOK_SLUGS` | — | `slug:base_slug` aliases or `slug:{"tone":"..."}` inline |
| `STRATEGIST_APPROVAL_QUEUE` | `false` | Write briefs to `data/approvals/` as `pending` |
| `STRATEGIST_APPROVAL_DIR` | `data/approvals` | Approval file directory |

## Output contract

`StrategistOutputContract` (`state/contracts.py`) includes:

- `recommendation`, `priority`, `negotiation_brief`, `threat_level`
- `merchant_actions`, `executive_summary`, `talking_points`, `urgency_hours`
- `aligned_with_analyst`, `notification_preview` (alerter-ready, no extra LLM)
- `negotiation_leverage` (`low` \| `medium` \| `high`), `counter_offer_suggestion`
- When approval queue is on: `approval_id`, `approval_status`, `submitted_at`

## Playbooks

Built-in slugs: `myntra`, `ajio`, `nykaa`, `amazon`, `flipkart`, `default`.  
Resolve via `get_playbook(slug)` in `agents/strategist_playbooks.py`.

## Alerter integration

If `notification_preview` is set on strategist output, `AlertAgent` sends it directly without calling Ollama.

## Tests

```bash
python -m unittest tests.test_strategist -v
```
