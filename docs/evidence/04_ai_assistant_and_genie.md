# Evidence 04 — AI assistant (Claude via Foundation Model APIs) and Genie, with real outputs

**Raw:** [`raw/copilot_answer_0b7fc9b7.txt`](raw/copilot_answer_0b7fc9b7.txt), [`raw/copilot_interactions.txt`](raw/copilot_interactions.txt),
[`raw/genie_conversation.json`](raw/genie_conversation.json)

## 1. AI Market Intelligence assistant

**Code:** [`app/routers/copilot.py`](../../app/routers/copilot.py) (`POST /api/chat`) and [`app/routers/copilot_tools.py`](../../app/routers/copilot_tools.py)
**Model:** `databricks-claude-sonnet-4-6` (Foundation Model API, pay-per-token), multi-turn tool loop.
**Audit log (inference table):** every turn is written to `gold.copilot_interactions`: question, each tool call with its arguments and a
result preview, the answer, tokens and latency. The app's service principal has `MODIFY` on that table only.

### Interaction `0b7fc9b7-56b1-4454-8b96-a37bb572e0bf` (2026-11-09 09:00 UTC, 23,395 in / 1,158 out tokens, 41.6 s)

**Question:** *"Using your tools, give me the current SA1 and NSW1 spot prices and the model's 1-hour-ahead price forecast for both
regions. Then tell me whether the forecast suggests the desk should be worried about a spike."*

**Tools the model called** (from `gold.copilot_interactions.tool_calls`):

| round | tool | arguments | returned |
|---|---|---|---|
| 1 | `query_spot_prices` | `{"region": "SA1"}` | latest RRP, 1-h avg/min/max, intervals > $300 from `gold.nem_prices_5min` |
| 1 | `query_spot_prices` | `{"region": "NSW1"}` | same |
| 1 | `query_price_forecasts` | `{"region": "SA1", "hours": 1}` | live forecast rows from UC model `nem_price_forecast_sa1` v6 + its held-out skill |
| 1 | `query_price_forecasts` | `{"region": "NSW1", "hours": 1}` | same |

**Answer (excerpt):**

> | Region | Latest RRP | 1-hr Avg | 1-hr Min | 1-hr Max | Intervals >$300 | Demand |
> |---|---|---|---|---|---|---|
> | **SA1** | **$115.90/MWh** | $119.83 | $112.45 | $137.44 | 0 | 1,384 MW |
> | **NSW1** | **$121.01/MWh** | $117.17 | $107.30 | $130.25 | 0 | 8,227 MW |
>
> | Region | 30-min Forecast | 60-min Forecast | 80% CI (60-min) | Spike Probability |
> |---|---|---|---|---|
> | **SA1** | $101.96 | $97.29 | $56.06 – $132.44 | 0.0% |
> | **NSW1** | $115.80 | $114.88 | $98.70 – $151.98 | 0.0% |
>
> **Model caveat:** At the 30-min horizon, SA1's model actually has *negative skill vs. persistence* (MAE 32.35 vs. persistence 29.64),
> meaning it is slightly *worse* than a naïve carry-forward at short horizons. … SA1 … the 24-hour data shows **20 spike events** … and an
> intraday high of **$3,290/MWh** today.

**Independent check** of the figures the assistant quoted, run directly against gold:

| region | max RRP (24 h) | intervals > $300 | std dev | min |
|---|---|---|---|---|
| SA1 | 3,290.00 | 20 | 225.3 | −150.09 |
| NSW1 | 145.66 | 0 | 37.5 | −10.50 |

These match the answer exactly.

### Fixes made to get here (found while capturing this evidence)

- The six core tools called UC functions in a schema `ai_tools` that does not exist, so they always returned "No data". They are now
  validated queries on gold tables in `copilot_tools.py`: regions are whitelisted, hours are clamped, and every result carries `as_of` and
  its source.
- Generation-mix, interconnector and weather tables are not refreshed in this build (last data 2026-10-01). A freshness gate now drops
  them from the model's context and lists them as **NOT AVAILABLE**, and the tools return `not_available`. Before this, the assistant
  presented that stale data as live.
- The system prompt no longer tells the model "never say you don't have data". It now tells it to say plainly when a source is unavailable
  and never invent values.

## 2. Genie (AI/BI) — natural language → SQL on governed tables

**Space:** *NEM Spot Market Intelligence* (`01f1bdbddc6b1db5854c987326a7e67f`) · conversation `01f1dc1cb03e13b9911787894150f467`,
message `01f1dc1cb0a714f0b085d0316ac50292` (status COMPLETED)

**Question:** *"For October 2026, which NEM region had the most 5-minute intervals with a spot price above 300 dollars per MWh, how many
such intervals were there, and what was that region's maximum price?"*

**SQL Genie generated:**

```sql
WITH region_counts AS (
  SELECT `region_id`, COUNT(*) AS `interval_count_above_300`, MAX(`rrp`) AS `max_price_aud_mwh`
  FROM `serverless_stable_2b9lj3_catalog`.`gold`.`nem_prices_5min`
  WHERE `interval_datetime` >= TIMESTAMP('2026-10-01 00:00:00')
    AND `interval_datetime` <  TIMESTAMP('2026-11-01 00:00:00')
    AND `rrp` > 300 AND `region_id` IS NOT NULL AND `rrp` IS NOT NULL
  GROUP BY `region_id`
), ranked_regions AS (
  SELECT `region_id`, `interval_count_above_300`, `max_price_aud_mwh`,
         RANK() OVER (ORDER BY `interval_count_above_300` DESC) AS `region_rank`
  FROM region_counts
)
SELECT `region_id`, `interval_count_above_300`, `max_price_aud_mwh`
FROM ranked_regions WHERE `region_rank` = 1 ORDER BY `region_id`
```

**Result rows:** `[["NSW1", "87", "23200.0"]]`
**Genie's answer:** *"For October 2026, NSW1 had the most 5-minute intervals with spot prices above $300/MWh, with 87 intervals. In NSW1,
the maximum spot price during those intervals was $23,200.0/MWh."*

**Independent check:**

| region | intervals > $300 (Oct 2026) | max RRP |
|---|---|---|
| **NSW1** | **87** | **23,200.00** |
| QLD1 | 20 | 860.05 |
| TAS1 | 8 | 359.12 |
| VIC1 | 8 | 406.11 |
| SA1 | 7 | 3,210.98 |
