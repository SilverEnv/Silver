# Control / Independence Report v0

This guide defines the next research cockpit build after a promising cell
survives the deep dive.

The first target is:

```text
avg_dollar_volume_63__h252
```

The question is not whether high dollar volume beat the baseline in the first
test. It did. The question is whether it adds independent evidence beyond
existing momentum, size, and liquidity-like exposure.

## Decision Anchor

Goal: decide whether `avg_dollar_volume_63__h252` is a distinct research path,
a useful control exposure, or a redundant proxy for momentum/liquidity.

User value: Michael can avoid letting the AI generate many variants of the
same hidden trade.

Constraints:

- Do not add new hypotheses during this report.
- Do not promote any cell to `accepted`.
- Do not mutate registry status.
- Do not write predictions or portfolio rows.
- Use stored point-in-time feature values, labels, walk-forward windows,
  costs, and linked backtest/model evidence.

Falsifier: if the result disappears inside momentum buckets, only works in the
largest/liquid names, or has no residual edge after simple controls, it should
not guide AI hypothesis generation as a standalone signal.

## Why This Comes Before AI Hypothesis Generation

The deep dive found:

| Field | Current read |
| --- | --- |
| Cell | `avg_dollar_volume_63__h252` |
| Edge | +1.6025% versus equal-weight baseline |
| Buckets | 21/35 positive, exactly 60.0% |
| Selected-ticker concentration | broad; no single ticker dominates |
| Momentum selected-ticker overlap | 39/39 target tickers overlap at h252 |
| Current decision | `watch` |

That means the obvious risk is no longer one-ticker concentration. The obvious
risk is proxy overlap:

```text
high dollar volume may be selecting the same large/liquid/momentum names.
```

The AI research loop should start only after the cockpit can tell the AI which
signals are genuinely distinct and which are controls or duplicates.

## Required Output

The first implementation should add a generated read-only section or report
with this shape:

```text
Control / Independence Report v0

Target: avg_dollar_volume_63__h252
Decision: continue | watch | demote
Reason: one-sentence operator reason

Controls:
- momentum_12_1__h252:
- momentum_6_1__h252:
- momentum_3_0__h252:

Bucket-Neutral Test:
...

Residual / Within-Control Test:
...

Selection Overlap:
...

Regime Read:
...

Decision:
...
```

The decision is an operator recommendation, not a registry status.

## Core Tests

### 1. Selection Overlap

Question:

```text
Does the target select the same names as momentum?
```

Use the same read-only reconstruction already used by the deep dive:

- target selected tickers for `avg_dollar_volume_63__h252`
- momentum selected tickers for `momentum_12_1__h252`
- momentum selected tickers for `momentum_6_1__h252`
- momentum selected tickers for `momentum_3_0__h252`

Output:

| Control | Overlap tickers | Selected-observation overlap | Top overlapping tickers |
| --- | --- | --- | --- |

Interpretation:

- high overlap means the target is not independent by selection set
- low overlap means it may capture a different part of the universe

### 2. Within-Momentum Buckets

Question:

```text
Among stocks with similar momentum, does high dollar volume still help?
```

Simple v0 method:

1. For each scored `asof_date`, rank eligible securities by a momentum control.
2. Split the universe into momentum buckets, starting with low/high halves.
3. Inside each momentum bucket, compare high dollar volume versus the bucket
   baseline.
4. Aggregate by walk-forward window.

Output:

| Control | Bucket | Windows positive | Mean edge | Verdict |
| --- | --- | --- | --- | --- |

Interpretation:

- if dollar volume only works in high-momentum buckets, it may be momentum
  reinforcement, not an independent signal
- if it works in both momentum buckets, it has stronger independent evidence

### 3. Residual Rank Test

Question:

```text
After removing simple momentum rank, does dollar-volume rank still explain
future return?
```

Simple v0 method:

- use cross-sectional ranks by date
- compute target rank minus control-rank relationship through a simple residual
  or bucket-neutral comparison
- evaluate the residual selection path with the same horizon and walk-forward
  windows

Do not start with a complex model. A readable rank residual is enough for v0.

Output:

| Control | Residual edge | Windows positive | Label scramble | Verdict |
| --- | --- | --- | --- | --- |

### 4. Size / Liquidity Proxy Check

Question:

```text
Is the target just selecting the biggest and most liquid part of the seed
universe?
```

Use available stored evidence first:

- selected ticker frequency
- average dollar volume rank
- selected universe breadth
- overlap with high-dollar-volume itself across horizons

If explicit market cap is not available, say so. Do not invent size exposure.

Output:

| Check | Read |
| --- | --- |
| selected breadth | ... |
| top ticker share | ... |
| top 5 share | ... |
| market-cap data | unavailable unless stored |

### 5. Regime Read

Question:

```text
Does any remaining edge exist across regimes, or only in one market period?
```

Use existing regime evidence from the explainer:

- weak in `2020-2021`
- strong in `2022-2023`
- positive in `2024-2026`

Output:

| Regime | Target edge | Control read | Interpretation |
| --- | --- | --- | --- |

## Recommendation Labels

| Label | Meaning |
| --- | --- |
| `continue` | The target has evidence beyond momentum/liquidity controls and deserves the next research build. |
| `watch` | The target remains useful context or a control exposure, but independence is not established. |
| `demote` | The target is redundant, fragile, or explained by controls. |

Default to `watch` when evidence is mixed.

## Initial Decision Rules

`continue` if:

- selected-ticker overlap with controls is not dominant
- within-momentum bucket edge remains positive
- residual/bucket-neutral evidence is positive
- edge is not limited to one regime
- cost cushion remains usable

`watch` if:

- raw edge remains positive
- selected-ticker overlap is high
- residual evidence is weak or unavailable
- the target may be useful as a liquidity/control exposure

`demote` if:

- within-control edge disappears
- residual/bucket-neutral edge is zero or negative
- selected-ticker overlap is effectively complete
- the same regimes and tickers explain the result as momentum

## What Not To Do

Do not:

- add AI-generated hypotheses yet
- build a generalized ML factor model first
- optimize thresholds to make the target survive
- add non-canonical horizons
- call the result alpha
- promote registry status
- use market-cap claims without stored market-cap evidence

## Build Order

Use the smallest useful build:

1. Add a generated `Control / Independence Report v0` section for
   `avg_dollar_volume_63__h252`.
2. Reuse the existing selected-ticker reconstruction for target and momentum
   controls.
3. Add explicit selection-overlap rows.
4. Add a simple within-momentum-half test for `momentum_12_1__h252`.
5. Add residual-rank or bucket-neutral comparison only after the first
   within-control test is readable.
6. Keep the AI hypothesis-generation loop paused until this report can label
   the target as `continue`, `watch`, or `demote`.

## Validation

For documentation-only changes:

```bash
git diff --check
```

For implementation:

```bash
python scripts/research_results_report.py --check
python scripts/research_results_report.py
python -m pytest tests/test_research_results_report.py
git diff --check
```
