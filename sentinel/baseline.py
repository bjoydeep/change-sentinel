"""
Baseline capture via Prometheus / Thanos HTTP API.

Queries the metric backend for the window [event_timestamp - 1h : event_timestamp - 5min]
and computes summary statistics (mean, stddev, slope) per metric.
These become the reference against which post-change values are compared.
"""

import logging
import statistics
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

log = logging.getLogger(__name__)


def _bearer_token(token_path: Optional[str]) -> Optional[str]:
    """Read bearer token from file, or from METRICS_TOKEN env var."""
    import os
    env_token = os.environ.get("METRICS_TOKEN")
    if env_token:
        return env_token
    if token_path:
        p = Path(token_path)
        if p.exists():
            return p.read_text().strip()
    return None


def _session(token_path: Optional[str], verify_tls: bool) -> requests.Session:
    """Return a requests Session pre-configured with auth and TLS settings."""
    s = requests.Session()
    s.verify = verify_tls
    token = _bearer_token(token_path)
    if token:
        s.headers["Authorization"] = f"Bearer {token}"
    return s

# How far back before the change to start the baseline window
BASELINE_LOOKBACK_MINUTES = 60
# How close to the change time to stop (avoid capturing the change itself)
BASELINE_GAP_MINUTES = 5
# Step size for range queries
BASELINE_STEP_SECONDS = 60


def capture(
    checkers: list[dict],
    namespace: str,
    event_timestamp: datetime,
    metrics_endpoint: str,
    token_path: Optional[str] = None,
    verify_tls: bool = False,
) -> dict:
    """
    Query Prometheus for each checker's metric over the pre-change window.
    Returns a dict keyed by metric label with summary stats.
    """
    start = event_timestamp - timedelta(minutes=BASELINE_LOOKBACK_MINUTES)
    end = event_timestamp - timedelta(minutes=BASELINE_GAP_MINUTES)

    sess = _session(token_path, verify_tls)
    baseline = {}
    for checker in checkers:
        for metric_def in _metric_defs(checker):
            query = _interpolate(metric_def["query"], namespace=namespace)
            label = metric_def["label"]
            series = _query_range(metrics_endpoint, query, start, end, session=sess)
            if series:
                baseline[label] = _summarise(series)
                log.info(
                    "Baseline %s: mean=%.3f stddev=%.3f slope=%.4f/min",
                    label,
                    baseline[label]["mean"],
                    baseline[label]["stddev"],
                    baseline[label]["slope_per_minute"],
                )
            else:
                log.warning("No baseline data for %s (query: %s)", label, query)
                baseline[label] = _empty_summary()

    return baseline


# ── Prometheus HTTP API ───────────────────────────────────────────────────────

def query_instant(
    metrics_endpoint: str,
    query: str,
    at: datetime,
    token_path: Optional[str] = None,
    verify_tls: bool = False,
) -> Optional[float]:
    """Single instant query — returns scalar value or None."""
    sess = _session(token_path, verify_tls)
    try:
        resp = sess.get(
            f"{metrics_endpoint}/api/v1/query",
            params={"query": query, "time": at.timestamp()},
            timeout=10,
        )
        resp.raise_for_status()
        result = resp.json()["data"]["result"]
        if result:
            return float(result[0]["value"][1])
    except Exception as e:
        log.warning("Instant query failed (%s): %s", query, e)
    return None


def query_range(
    metrics_endpoint: str,
    query: str,
    start: datetime,
    end: datetime,
    step: int = BASELINE_STEP_SECONDS,
    token_path: Optional[str] = None,
    verify_tls: bool = False,
    session: Optional[requests.Session] = None,
) -> list[tuple[float, float]]:
    """Range query — returns list of (unix_timestamp, value) pairs."""
    sess = session or _session(token_path, verify_tls)
    return _query_range(metrics_endpoint, query, start, end, step, session=sess)


# ── Internal helpers ──────────────────────────────────────────────────────────

def _query_range(
    endpoint: str,
    query: str,
    start: datetime,
    end: datetime,
    step: int = BASELINE_STEP_SECONDS,
    session: Optional[requests.Session] = None,
) -> list[tuple[float, float]]:
    s = session or requests.Session()
    try:
        resp = s.get(
            f"{endpoint}/api/v1/query_range",
            params={
                "query": query,
                "start": start.timestamp(),
                "end": end.timestamp(),
                "step": step,
            },
            timeout=15,
        )
        resp.raise_for_status()
        results = resp.json()["data"]["result"]
        if not results:
            return []
        # Take first result (assumes single time series per query)
        return [(float(ts), float(val)) for ts, val in results[0]["values"]]
    except Exception as e:
        log.warning("Range query failed (%s): %s", query, e)
        return []


def _summarise(series: list[tuple[float, float]]) -> dict:
    """Compute mean, stddev, and slope (per minute) from a time series."""
    values = [v for _, v in series]
    times_sec = [t for t, _ in series]

    mean = statistics.mean(values) if values else 0.0
    stddev = statistics.stdev(values) if len(values) > 1 else 0.0
    slope_per_sec = _slope(times_sec, values)

    return {
        "mean": mean,
        "stddev": stddev,
        "slope_per_minute": slope_per_sec * 60,
        "min": min(values) if values else 0.0,
        "max": max(values) if values else 0.0,
        "sample_count": len(values),
    }


def _empty_summary() -> dict:
    return {
        "mean": 0.0,
        "stddev": 0.0,
        "slope_per_minute": 0.0,
        "min": 0.0,
        "max": 0.0,
        "sample_count": 0,
    }


def _slope(times: list[float], values: list[float]) -> float:
    """Least-squares slope (value units per second)."""
    n = len(times)
    if n < 2:
        return 0.0
    t_mean = sum(times) / n
    v_mean = sum(values) / n
    num = sum((t - t_mean) * (v - v_mean) for t, v in zip(times, values))
    den = sum((t - t_mean) ** 2 for t in times)
    return num / den if den else 0.0


def _metric_defs(checker: dict) -> list[dict]:
    """Normalise checker config into a list of {query, label} dicts."""
    if "metrics" in checker:
        return checker["metrics"]
    if "query" in checker:
        return [{"query": checker["query"], "label": checker["label"]}]
    return []


def _interpolate(query: str, **kwargs) -> str:
    """Replace {{key}} placeholders in a PromQL query string."""
    for k, v in kwargs.items():
        query = query.replace(f"{{{{{k}}}}}", v)
    return query
