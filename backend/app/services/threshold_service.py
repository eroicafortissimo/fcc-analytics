"""
threshold_service.py
Data-driven AML threshold calibration service.
Parses uploaded transaction CSVs/Excel, applies scenario filters,
computes statistical distributions, and simulates alert volumes.
"""
from __future__ import annotations
import io
import json
import re
import uuid
from datetime import datetime, timedelta
from typing import Any, Optional

import numpy as np
import pandas as pd

# ── In-memory dataset store ────────────────────────────────────────────────────
# Keyed by integer dataset_id. Data is not persisted — metadata only goes to DB.
_store: dict[int, dict] = {}

def _next_id() -> int:
    return max(_store.keys(), default=0) + 1


def store_dataset(dataset_id: int, df: pd.DataFrame, meta: dict) -> None:
    _store[dataset_id] = {"df": df, **meta}


def get_dataset(dataset_id: int) -> dict | None:
    return _store.get(dataset_id)


def delete_dataset(dataset_id: int) -> None:
    _store.pop(dataset_id, None)


# ── File parsing ───────────────────────────────────────────────────────────────

def parse_upload(file_bytes: bytes, filename: str) -> dict:
    """
    Parse CSV or Excel bytes into a DataFrame.
    Returns {df, columns, dtypes, sample_values, row_count, date_range_start, date_range_end}.
    """
    fname = filename.lower()
    if fname.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(file_bytes))
    elif fname.endswith((".xlsx", ".xls")):
        df = pd.read_excel(io.BytesIO(file_bytes))
    else:
        raise ValueError(f"Unsupported file type: {filename}")

    # Auto-detect and parse date columns
    for col in df.columns:
        if df[col].dtype == object:
            sample = df[col].dropna().head(20).astype(str)
            date_like = sample.str.match(
                r"^\d{4}-\d{2}-\d{2}|^\d{2}/\d{2}/\d{4}|^\d{2}-\d{2}-\d{4}"
            ).sum()
            if date_like >= min(5, len(sample)):
                try:
                    df[col] = pd.to_datetime(df[col], infer_datetime_format=True, errors="coerce")
                except Exception:
                    pass

    # Build column metadata
    columns = []
    sample_values: dict[str, list] = {}
    for col in df.columns:
        dtype = str(df[col].dtype)
        is_numeric = pd.api.types.is_numeric_dtype(df[col])
        is_datetime = pd.api.types.is_datetime64_any_dtype(df[col])
        if is_numeric:
            kind = "numeric"
        elif is_datetime:
            kind = "datetime"
        else:
            kind = "categorical"
        columns.append({"name": col, "dtype": dtype, "kind": kind})
        non_null = df[col].dropna()
        if kind == "categorical":
            sample_values[col] = [str(v) for v in non_null.value_counts().head(10).index.tolist()]
        elif kind == "numeric":
            sample_values[col] = [round(float(v), 2) for v in [non_null.min(), non_null.max(), non_null.median()] if not pd.isna(v)]
        else:
            sample_values[col] = [str(non_null.min()), str(non_null.max())] if len(non_null) else []

    # Date range
    date_range_start = date_range_end = None
    date_cols = [c["name"] for c in columns if c["kind"] == "datetime"]
    if date_cols:
        dc = date_cols[0]
        valid = df[dc].dropna()
        if len(valid):
            date_range_start = str(valid.min().date()) if hasattr(valid.min(), "date") else str(valid.min())
            date_range_end = str(valid.max().date()) if hasattr(valid.max(), "date") else str(valid.max())

    return {
        "df": df,
        "columns": columns,
        "sample_values": sample_values,
        "row_count": len(df),
        "date_range_start": date_range_start,
        "date_range_end": date_range_end,
    }


# ── Filter engine ──────────────────────────────────────────────────────────────

_OP_MAP = {
    "=": lambda col, val: col == val,
    "!=": lambda col, val: col != val,
    ">": lambda col, val: col > val,
    "<": lambda col, val: col < val,
    ">=": lambda col, val: col >= val,
    "<=": lambda col, val: col <= val,
    "contains": lambda col, val: col.astype(str).str.contains(str(val), case=False, na=False),
    "not contains": lambda col, val: ~col.astype(str).str.contains(str(val), case=False, na=False),
    "in": lambda col, val: col.isin(val if isinstance(val, list) else [val]),
    "not in": lambda col, val: ~col.isin(val if isinstance(val, list) else [val]),
    "between": lambda col, val: col.between(val[0], val[1]),
    "is null": lambda col, val: col.isna(),
    "is not null": lambda col, val: col.notna(),
}


def apply_filters(df: pd.DataFrame, filter_rules: dict) -> pd.DataFrame:
    """
    Apply filter_rules dict to df and return filtered DataFrame.
    filter_rules = {
        "group_operator": "AND",  # between groups
        "groups": [
            {"operator": "AND", "conditions": [{"column": ..., "operator": ..., "value": ...}]}
        ]
    }
    """
    if not filter_rules or not filter_rules.get("groups"):
        return df

    group_masks = []
    for group in filter_rules.get("groups", []):
        cond_masks = []
        for cond in group.get("conditions", []):
            col_name = cond.get("column")
            op = cond.get("operator", "=")
            val = cond.get("value")
            if col_name not in df.columns:
                continue
            series = df[col_name]
            # Coerce value type to match column
            if pd.api.types.is_numeric_dtype(series) and val is not None and op not in ("is null", "is not null"):
                try:
                    if isinstance(val, list):
                        val = [float(v) for v in val]
                    else:
                        val = float(val)
                except (ValueError, TypeError):
                    pass
            op_fn = _OP_MAP.get(op)
            if op_fn is None:
                continue
            mask = op_fn(series, val)
            cond_masks.append(mask)

        if not cond_masks:
            continue
        group_op = group.get("operator", "AND").upper()
        if group_op == "AND":
            group_mask = cond_masks[0]
            for m in cond_masks[1:]:
                group_mask = group_mask & m
        else:
            group_mask = cond_masks[0]
            for m in cond_masks[1:]:
                group_mask = group_mask | m
        group_masks.append(group_mask)

    if not group_masks:
        return df

    g_op = filter_rules.get("group_operator", "AND").upper()
    if g_op == "AND":
        final_mask = group_masks[0]
        for m in group_masks[1:]:
            final_mask = final_mask & m
    else:
        final_mask = group_masks[0]
        for m in group_masks[1:]:
            final_mask = final_mask | m

    return df[final_mask].copy()


# ── Aggregation ────────────────────────────────────────────────────────────────

def aggregate_transactions(
    df: pd.DataFrame,
    key_column: str,
    amount_column: str,
    period: str,
    agg_function: str = "SUM",
    date_column: str | None = None,
) -> pd.DataFrame:
    """
    Aggregate transactions by key + time window.
    period: "none" (just group by key), "daily", "rolling_7", "rolling_30", "custom_N"
    Returns one row per (key, period) with sum/count/avg/max/min of amount.
    """
    if key_column not in df.columns:
        raise ValueError(f"Key column '{key_column}' not found")
    if amount_column not in df.columns:
        raise ValueError(f"Amount column '{amount_column}' not found")

    agg_fn = agg_function.upper()

    if period == "none" or date_column is None or date_column not in df.columns:
        # Simple group-by with no time window
        grp = df.groupby(key_column)[amount_column]
        if agg_fn == "SUM":
            agg = grp.sum()
        elif agg_fn == "COUNT":
            agg = grp.count()
        elif agg_fn == "AVG":
            agg = grp.mean()
        elif agg_fn == "MAX":
            agg = grp.max()
        elif agg_fn == "MIN":
            agg = grp.min()
        else:
            agg = grp.sum()
        result = agg.reset_index()
        result.columns = [key_column, "agg_value"]
        result["txn_count"] = df.groupby(key_column)[amount_column].count().values
        return result

    # Rolling window aggregation
    df2 = df[[key_column, amount_column, date_column]].copy()
    df2[date_column] = pd.to_datetime(df2[date_column], errors="coerce")
    df2 = df2.dropna(subset=[date_column])
    df2 = df2.sort_values([key_column, date_column])

    if period == "daily":
        df2["period_key"] = df2[date_column].dt.date
        grp = df2.groupby([key_column, "period_key"])[amount_column]
    elif period.startswith("rolling_") or period.startswith("custom_"):
        days = int(period.split("_")[1])
        # Per-key rolling window
        rows = []
        for key_val, group in df2.groupby(key_column):
            group = group.sort_values(date_column)
            dates = group[date_column].values
            amounts = group[amount_column].values
            # For each transaction, compute window [date, date+N] — transaction is window start
            for i, (d, a) in enumerate(zip(dates, amounts)):
                cutoff = pd.Timestamp(d) + timedelta(days=days)
                window = amounts[
                    (dates >= d) & (dates <= np.datetime64(cutoff))
                ]
                if agg_fn == "SUM":
                    val = float(window.sum())
                elif agg_fn == "COUNT":
                    val = float(len(window))
                elif agg_fn == "AVG":
                    val = float(window.mean()) if len(window) else 0.0
                elif agg_fn == "MAX":
                    val = float(window.max()) if len(window) else 0.0
                elif agg_fn == "MIN":
                    val = float(window.min()) if len(window) else 0.0
                else:
                    val = float(window.sum())
                rows.append({
                    key_column: key_val,
                    date_column: d,
                    "agg_value": val,
                    "txn_count": len(window),
                })
        result = pd.DataFrame(rows)
        return result

    # Finalize daily aggregation
    if agg_fn == "SUM":
        agg_val = grp.sum()
    elif agg_fn == "COUNT":
        agg_val = grp.count()
    elif agg_fn == "AVG":
        agg_val = grp.mean()
    elif agg_fn == "MAX":
        agg_val = grp.max()
    elif agg_fn == "MIN":
        agg_val = grp.min()
    else:
        agg_val = grp.sum()

    txn_count = grp.count()
    result = agg_val.reset_index()
    result.columns = [key_column, "period_key", "agg_value"]
    result["txn_count"] = txn_count.values
    return result


# ── Statistical analysis ───────────────────────────────────────────────────────

def _safe_float(v) -> float | None:
    try:
        f = float(v)
        return None if (np.isnan(f) or np.isinf(f)) else f
    except (TypeError, ValueError):
        return None


def compute_statistics(series: pd.Series) -> dict:
    """Compute full statistical summary for a numeric series."""
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) == 0:
        return {"count": 0, "error": "no numeric data"}

    q25, q50, q75 = float(s.quantile(0.25)), float(s.quantile(0.50)), float(s.quantile(0.75))
    mean = float(s.mean())
    std = float(s.std())
    p85 = float(s.quantile(0.85))
    p90 = float(s.quantile(0.90))
    p95 = float(s.quantile(0.95))
    p99 = float(s.quantile(0.99))

    mild_lower = mean - 2 * std
    mild_upper = mean + 2 * std
    extreme_lower = mean - 3 * std
    extreme_upper = mean + 3 * std

    outliers_mild = int(((s < mild_lower) | (s > mild_upper)).sum())
    outliers_extreme = int(((s < extreme_lower) | (s > extreme_upper)).sum())

    # Trimmed series — within mild (μ±2σ) and extreme (μ±3σ) bounds
    s_trim_mild    = s[(s >= mild_lower)    & (s <= mild_upper)]
    s_trim_extreme = s[(s >= extreme_lower) & (s <= extreme_upper)]

    def trim_pct(series, p):
        return _safe_float(series.quantile(p)) if len(series) else None

    return {
        "count": int(len(s)),
        "min": _safe_float(s.min()),
        "max": _safe_float(s.max()),
        "mean": _safe_float(s.mean()),
        "median": _safe_float(s.median()),
        "std": _safe_float(s.std()),
        "variance": _safe_float(s.var()),
        "p25": _safe_float(q25),
        "p50": _safe_float(q50),
        "p75": _safe_float(q75),
        "p85": _safe_float(p85),
        "p90": _safe_float(p90),
        "p95": _safe_float(p95),
        "p99": _safe_float(p99),
        "outliers_mild": outliers_mild,
        "outliers_extreme": outliers_extreme,
        "outlier_mild_lower": _safe_float(mild_lower),
        "outlier_mild_upper": _safe_float(mild_upper),
        "outlier_extreme_lower": _safe_float(extreme_lower),
        "outlier_extreme_upper": _safe_float(extreme_upper),
        # Excl. mild outliers (μ ± 2σ)
        "trim_min":  _safe_float(s_trim_mild.min()) if len(s_trim_mild) else None,
        "trim_max":  _safe_float(s_trim_mild.max()) if len(s_trim_mild) else None,
        "trim_p25":  trim_pct(s_trim_mild, 0.25),
        "trim_p50":  trim_pct(s_trim_mild, 0.50),
        "trim_p75":  trim_pct(s_trim_mild, 0.75),
        "trim_p85":  trim_pct(s_trim_mild, 0.85),
        "trim_p90":  trim_pct(s_trim_mild, 0.90),
        "trim_p95":  trim_pct(s_trim_mild, 0.95),
        "trim_p99":  trim_pct(s_trim_mild, 0.99),
        # Excl. extreme outliers (μ ± 3σ)
        "xtrim_min": _safe_float(s_trim_extreme.min()) if len(s_trim_extreme) else None,
        "xtrim_max": _safe_float(s_trim_extreme.max()) if len(s_trim_extreme) else None,
        "xtrim_p25": trim_pct(s_trim_extreme, 0.25),
        "xtrim_p50": trim_pct(s_trim_extreme, 0.50),
        "xtrim_p75": trim_pct(s_trim_extreme, 0.75),
        "xtrim_p85": trim_pct(s_trim_extreme, 0.85),
        "xtrim_p90": trim_pct(s_trim_extreme, 0.90),
        "xtrim_p95": trim_pct(s_trim_extreme, 0.95),
        "xtrim_p99": trim_pct(s_trim_extreme, 0.99),
        # Keep s_trim_mild reference for caller (router uses it for trim_tranches)
        "_s_trim_mild": s_trim_mild,
    }


def auto_tranches(series: pd.Series, n: int = 8) -> list[float]:
    """Generate natural breakpoints for a numeric series."""
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) == 0:
        return []
    mn, mx = float(s.min()), float(s.max())
    if mn == mx:
        return [mn, mx]

    # Start with percentile-based boundaries
    pcts = [0, 10, 25, 50, 75, 90, 95, 99, 100]
    raw = sorted(set(float(s.quantile(p / 100)) for p in pcts))

    # Round to nice numbers
    def nice(v):
        if v == 0:
            return 0
        mag = 10 ** (len(str(int(abs(v)))) - 1)
        return round(v / mag) * mag

    boundaries = sorted(set(nice(v) for v in raw))
    if len(boundaries) < 2:
        boundaries = [mn, mx]
    # Ensure min and max are included
    boundaries = sorted(set([mn] + boundaries + [mx]))
    return boundaries


def tranche_distribution(series: pd.Series, boundaries: list[float]) -> list[dict]:
    """Build tranche table with counts and percentages."""
    s = pd.to_numeric(series, errors="coerce").dropna()
    total = len(s)
    if total == 0 or len(boundaries) < 2:
        return []

    rows = []
    cumulative = 0
    for i in range(len(boundaries) - 1):
        lo, hi = boundaries[i], boundaries[i + 1]
        if i == len(boundaries) - 2:
            count = int(((s >= lo) & (s <= hi)).sum())
        else:
            count = int(((s >= lo) & (s < hi)).sum())
        pct = round(count / total * 100, 1) if total else 0
        cumulative += count
        rows.append({
            "label": f"{_fmt(lo)} – {_fmt(hi)}",
            "lo": lo,
            "hi": hi,
            "count": count,
            "pct": pct,
            "cumulative_count": cumulative,
            "cumulative_pct": round(cumulative / total * 100, 1) if total else 0,
        })
    return rows


def _fmt(v: float) -> str:
    if abs(v) >= 1_000_000:
        return f"${v/1_000_000:.1f}M"
    if abs(v) >= 1_000:
        return f"${v/1_000:.0f}K"
    return f"${v:,.0f}" if v == int(v) else f"{v:.2f}"


def categorical_distribution(series: pd.Series, top_n: int = 15) -> dict:
    """Frequency distribution for a categorical column."""
    counts = series.dropna().value_counts()
    total = int(counts.sum())
    top = counts.head(top_n)
    other_count = int(counts.iloc[top_n:].sum()) if len(counts) > top_n else 0
    rows = [{"value": str(k), "count": int(v), "pct": round(int(v) / total * 100, 1)} for k, v in top.items()]
    if other_count:
        rows.append({"value": "Other", "count": other_count, "pct": round(other_count / total * 100, 1)})
    return {"rows": rows, "total": total, "unique_count": int(series.nunique())}


# ── Threshold simulation ───────────────────────────────────────────────────────

def simulate_thresholds(
    series: pd.Series,
    thresholds: list[float],
    key_series: pd.Series | None = None,
    date_series: pd.Series | None = None,
) -> list[dict]:
    """
    For each threshold, compute: alert count, unique keys, % txns captured,
    % volume captured, estimated monthly alerts.
    """
    s = pd.to_numeric(series, errors="coerce")
    valid_mask = s.notna()
    s_valid = s[valid_mask]
    total_events = len(s_valid)
    total_volume = float(s_valid.sum()) if len(s_valid) else 0.0

    # Estimate date range for monthly scaling
    monthly_scale = 1.0
    if date_series is not None:
        try:
            dates = pd.to_datetime(date_series[valid_mask], errors="coerce").dropna()
            if len(dates) >= 2:
                days_span = (dates.max() - dates.min()).days + 1
                monthly_scale = 30.0 / days_span if days_span > 0 else 1.0
        except Exception:
            pass

    results = []
    for t in sorted(thresholds):
        above_mask = s_valid >= t
        alert_count = int(above_mask.sum())
        pct_events = round(alert_count / total_events * 100, 1) if total_events else 0.0
        pct_volume = round(float(s_valid[above_mask].sum()) / total_volume * 100, 1) if total_volume else 0.0
        unique_keys = 0
        if key_series is not None:
            k = key_series[valid_mask]
            unique_keys = int(k[above_mask].nunique())
        est_monthly = round(alert_count * monthly_scale, 0)
        results.append({
            "threshold": t,
            "alert_count": alert_count,
            "unique_keys": unique_keys,
            "pct_events_captured": pct_events,
            "pct_volume_captured": pct_volume,
            "est_monthly_alerts": int(est_monthly),
        })
    return results


def recommend_threshold(
    sim_results: list[dict],
    target_monthly_alerts: int | None = None,
    min_pct_volume: float = 20.0,
) -> dict:
    """
    Pick the recommended threshold balancing alert manageability and volume coverage.
    """
    if not sim_results:
        return {"threshold": None, "reason": "No simulation results"}

    # If user has a target alert volume, find closest threshold
    if target_monthly_alerts and target_monthly_alerts > 0:
        best = min(sim_results, key=lambda r: abs(r["est_monthly_alerts"] - target_monthly_alerts))
        return {
            "threshold": best["threshold"],
            "reason": (
                f"Threshold ${best['threshold']:,.0f} produces approximately "
                f"{best['est_monthly_alerts']:,} alerts/month, closest to your target of "
                f"{target_monthly_alerts:,}. Captures {best['pct_volume_captured']}% of total volume."
            ),
        }

    # Otherwise: pick threshold that covers ≥min_pct_volume with fewest alerts
    candidates = [r for r in sim_results if r["pct_volume_captured"] >= min_pct_volume]
    if not candidates:
        candidates = sim_results

    best = min(candidates, key=lambda r: r["alert_count"])
    return {
        "threshold": best["threshold"],
        "reason": (
            f"Threshold ${best['threshold']:,.0f} balances coverage and manageability: "
            f"captures {best['pct_volume_captured']}% of volume with {best['alert_count']:,} alerts "
            f"({best['est_monthly_alerts']:,} estimated/month)."
        ),
    }


# ── ATL/BTL k-means analysis ───────────────────────────────────────────────────

def suggest_btl_kmeans(series: pd.Series, candidate_threshold: float) -> dict:
    """
    Use k-means with elbow method to segment the value distribution.
    BTL threshold = lower bound of the cluster containing the candidate threshold.
    """
    from sklearn.cluster import KMeans

    s = pd.to_numeric(series, errors="coerce").dropna()
    n = len(s)
    if n < 4:
        return {
            "btl_threshold": round(float(s.min()), 2) if n else 0.0,
            "candidate_threshold": round(candidate_threshold, 2),
            "optimal_k": 1,
            "tranches": [],
            "elbow_data": [],
            "rationale": "Insufficient data for clustering.",
        }

    values = s.values.reshape(-1, 1)
    max_k = min(10, n // 3)
    ks = list(range(2, max_k + 1))
    inertias = []
    for k in ks:
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        km.fit(values)
        inertias.append(float(km.inertia_))

    # Percent reduction in inertia at each step
    pct_reductions = [None] + [
        round((inertias[i - 1] - inertias[i]) / inertias[i - 1] * 100, 1)
        for i in range(1, len(inertias))
    ]

    # Elbow: second derivative peak = sharpest marginal drop-off
    if len(inertias) >= 3:
        d2 = np.diff(np.diff(inertias))
        elbow_pos = int(np.argmax(d2)) + 1
        optimal_k = ks[elbow_pos]
    else:
        elbow_pos = 0
        optimal_k = ks[0]

    # Final fit
    km = KMeans(n_clusters=optimal_k, random_state=42, n_init=10)
    km.fit(values)
    labels = km.labels_
    centers = km.cluster_centers_.flatten()

    # Build sorted tranches (rank 0 = lowest)
    center_order = np.argsort(centers)
    tranches = []
    for rank, orig_label in enumerate(center_order):
        mask = labels == orig_label
        cluster_vals = s.values[mask]
        lo = float(cluster_vals.min())
        hi = float(cluster_vals.max())
        tranches.append({
            "rank": rank,
            "lo": round(lo, 2),
            "hi": round(hi, 2),
            "center": round(float(centers[orig_label]), 2),
            "count": int(mask.sum()),
            "pct": round(int(mask.sum()) / n * 100, 1),
            "contains_candidate": lo <= candidate_threshold <= hi,
        })

    # Find anchor cluster
    anchor = next((t for t in tranches if t["contains_candidate"]), None)
    if anchor is None:
        anchor = min(tranches, key=lambda t: min(
            abs(t["lo"] - candidate_threshold), abs(t["hi"] - candidate_threshold)
        ))
        anchor = dict(anchor, contains_candidate=True)
        tranches = [anchor if t["rank"] == anchor["rank"] else t for t in tranches]

    btl_threshold = round(anchor["lo"], 2)

    # ── Rationale ──────────────────────────────────────────────────────────────
    elbow_pct_before = pct_reductions[elbow_pos] if elbow_pos < len(pct_reductions) else None
    elbow_pct_after  = pct_reductions[elbow_pos + 1] if elbow_pos + 1 < len(pct_reductions) else None

    lines = [
        f"Why k={optimal_k}?",
        "",
        "Inertia measures total within-cluster variance — lower is better, but adding more clusters always "
        "reduces it. The elbow method finds where additional clusters stop providing meaningful improvement.",
        "",
        "Inertia reduction by step:",
    ]
    for i, k in enumerate(ks):
        pct = pct_reductions[i]
        marker = "  <-- elbow" if k == optimal_k else ""
        if pct is None:
            lines.append(f"  k={k}:  {inertias[i]:>15,.0f}  (baseline){marker}")
        else:
            lines.append(f"  k={k}:  {inertias[i]:>15,.0f}  ({pct:+.1f}% vs k={ks[i-1]}){marker}")

    lines += [""]
    if elbow_pct_before is not None and elbow_pct_after is not None:
        lines.append(
            f"The second derivative of inertia peaks at k={optimal_k}: inertia dropped "
            f"{elbow_pct_before}% moving from k={optimal_k - 1} to k={optimal_k}, but only "
            f"{elbow_pct_after}% from k={optimal_k} to k={optimal_k + 1}. "
            f"This sharp deceleration marks the elbow — adding a {optimal_k + 1}th cluster "
            f"would yield {elbow_pct_after}% less compactness improvement for significant added complexity."
        )
    elif elbow_pct_before is not None:
        lines.append(
            f"k={optimal_k} was selected; inertia dropped {elbow_pct_before}% at this step "
            "and diminishing returns were observed beyond it."
        )
    else:
        lines.append(f"k={optimal_k} was selected as the minimum tested value.")

    lines += [
        "",
        f"BTL result: The candidate threshold {_fmt(candidate_threshold)} falls in cluster "
        f"{anchor['rank'] + 1} of {optimal_k} (range {_fmt(anchor['lo'])} to {_fmt(anchor['hi'])}, "
        f"center {_fmt(anchor['center'])}, {anchor['count']:,} transactions / {anchor['pct']}% of total). "
        f"The BTL threshold is set to the lower bound of this cluster: {_fmt(btl_threshold)}.",
    ]

    return {
        "btl_threshold": btl_threshold,
        "candidate_threshold": round(candidate_threshold, 2),
        "optimal_k": optimal_k,
        "tranches": tranches,
        "elbow_data": [
            {"k": k, "inertia": round(iner, 0), "pct_reduction": pct_reductions[i]}
            for i, (k, iner) in enumerate(zip(ks, inertias))
        ],
        "rationale": "
".join(lines),
    }


# ── AI scenario generation ─────────────────────────────────────────────────────

async def ai_generate_scenario(prompt: str, columns: list[dict], sample_values: dict) -> dict:
    """Use Claude to convert a natural-language scenario description into filter_rules JSON."""
    import os
    from anthropic import Anthropic

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not configured")

    col_descriptions = "\n".join(
        f"  - {c['name']} ({c['kind']}): sample values = {sample_values.get(c['name'], [])}"
        for c in columns
    )

    system = """You are an AML (Anti-Money Laundering) filter rule generator.
Convert a natural language scenario description into a structured filter rule JSON object.

Output ONLY valid JSON. No explanation, no markdown.

Filter rule format:
{
  "group_operator": "AND",
  "groups": [
    {
      "operator": "AND",
      "conditions": [
        {"column": "<column_name>", "operator": "<op>", "value": <value>}
      ]
    }
  ]
}

Valid operators: =, !=, >, <, >=, <=, contains, not contains, in, not in, between, is null, is not null
For "in" / "not in": value is a JSON array.
For "between": value is a [min, max] array.
For numeric comparisons: value is a number (no quotes).
For text: value is a string.

Also include these top-level fields in your JSON:
- "name": short scenario name (5 words max)
- "description": one sentence description
- "suggested_analysis_type": "single" or "aggregate"
- "suggested_key_column": best column for grouping (customer_id, account_id, etc.) or null
- "suggested_amount_column": best column for amounts or null"""

    user_msg = f"""Available columns:
{col_descriptions}

Scenario: {prompt}"""

    client = Anthropic(api_key=api_key)
    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1024,
        system=system,
        messages=[{"role": "user", "content": user_msg}],
    )
    text = msg.content[0].text.strip()
    import re as _re
    m = _re.search(r"\{.*\}", text, _re.DOTALL)
    return json.loads(m.group() if m else text)


async def ai_generate_report(context: dict) -> str:
    """Generate a professional AML threshold analysis report using Claude."""
    import os
    from anthropic import Anthropic

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return "Report generation unavailable — ANTHROPIC_API_KEY not configured."

    client = Anthropic(api_key=api_key)
    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=2048,
        messages=[{
            "role": "user",
            "content": f"""Generate a professional AML threshold calibration report based on this analysis:

{json.dumps(context, indent=2, default=str)}

Format the report with these sections:
1. Executive Summary (2-3 sentences)
2. Dataset Overview (date range, transaction volume, key statistics)
3. Scenario Definition (what was analyzed and why)
4. Statistical Findings (key distribution characteristics)
5. Threshold Analysis (comparison of tested thresholds)
6. Recommendation (recommended threshold with justification)
7. Methodology Note (brief note suitable for regulatory documentation)

Tone: Professional, compliance-oriented, suitable for model validation packages or regulatory submissions.
Length: 400-600 words."""
        }],
    )
    return msg.content[0].text.strip()
