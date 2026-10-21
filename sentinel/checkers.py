"""
Anomaly checkers — deterministic analysis of observed metrics vs baseline.

Two detection methods:
  Slope  — for accumulation / drift (ResourceAccumulationChecker, MemoryDrift)
  Z-score — for spikes (APIWriteRateSpike, RestartRateSpike, ImagePullBackOff)

Each checker returns a CheckerResult. The CorrelationAgent receives all results
and decides whether the combination constitutes a real anomaly.
"""

import logging
import math
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import requests

from baseline import _interpolate, _metric_defs, _slope, _session, query_range

log = logging.getLogger(__name__)

BASELINE_FLATNESS_THRESHOLD = 0.05  # slope/min below this is considered "flat"


@dataclass
class CheckerResult:
    checker_type: str
    label: str
    fired: bool
    observed_value: float
    baseline_mean: float
    baseline_stddev: float
    baseline_slope_per_minute: float
    observed_slope_per_minute: Optional[float] = None
    zscore: Optional[float] = None
    message: str = ""


def run_all(
    checkers_config: list[dict],
    namespace: str,
    baseline: dict,
    window_start: datetime,
    metrics_endpoint: str,
    token_path: Optional[str] = None,
    verify_tls: bool = False,
) -> list[CheckerResult]:
    """Run all configured checkers and return results (fired and not fired)."""
    now = datetime.now(timezone.utc)
    sess = _session(token_path, verify_tls)
    results = []

    for checker_cfg in checkers_config:
        checker_type = checker_cfg["type"]
        try:
            result = _dispatch(
                checker_type, checker_cfg, namespace, baseline, window_start, now, metrics_endpoint, sess
            )
            if result:
                results.extend(result if isinstance(result, list) else [result])
        except Exception as e:
            log.exception("Checker %s failed: %s", checker_type, e)

    return results


# ── Dispatcher ────────────────────────────────────────────────────────────────

def _dispatch(checker_type, cfg, namespace, baseline, window_start, now, endpoint, sess):
    if checker_type == "ResourceAccumulationChecker":
        return _resource_accumulation(cfg, namespace, baseline, window_start, now, endpoint, sess)
    if checker_type == "APIWriteRateSpike":
        return [_zscore_checker(cfg, namespace, baseline, window_start, now, endpoint, sess)]
    if checker_type == "MemoryDrift":
        return [_slope_checker(cfg, namespace, baseline, window_start, now, endpoint, sess)]
    if checker_type == "RestartRateSpike":
        return [_zscore_checker(cfg, namespace, baseline, window_start, now, endpoint, sess)]
    if checker_type == "NodeNotReady":
        return [_threshold_checker(cfg, namespace, baseline, now, endpoint, sess)]
    if checker_type == "MetricsGap":
        return [_absence_checker(cfg, namespace, baseline, now, endpoint, sess)]
    if checker_type == "ScrapeTargetLoss":
        return [_pct_drop_checker(cfg, namespace, baseline, now, endpoint, sess)]
    if checker_type == "ImagePullBackOff":
        return [_zscore_checker(cfg, namespace, baseline, window_start, now, endpoint, sess)]
    log.warning("Unknown checker type: %s", checker_type)
    return []


# ── Checker implementations ───────────────────────────────────────────────────

def _resource_accumulation(cfg, namespace, baseline, window_start, now, endpoint, sess):
    """Slope-based check for each metric in the metrics list."""
    results = []
    threshold = cfg.get("threshold_slope_per_minute", 0.3)

    for metric_def in cfg.get("metrics", []):
        query = _interpolate(metric_def["query"], namespace=namespace)
        label = metric_def["label"]
        bsl = baseline.get(label, {})

        series = query_range(endpoint, query, window_start, now, step=120, session=sess)
        if not series:
            log.warning("No data for %s during watch window", label)
            continue

        values = [v for _, v in series]
        times = [t for t, _ in series]
        obs_slope = _slope(times, values) * 60  # per minute
        obs_value = values[-1] if values else 0.0
        bsl_slope = bsl.get("slope_per_minute", 0.0)
        fired = obs_slope > threshold and bsl_slope < BASELINE_FLATNESS_THRESHOLD

        results.append(CheckerResult(
            checker_type="ResourceAccumulationChecker",
            label=label,
            fired=fired,
            observed_value=obs_value,
            baseline_mean=bsl.get("mean", 0.0),
            baseline_stddev=bsl.get("stddev", 0.0),
            baseline_slope_per_minute=bsl_slope,
            observed_slope_per_minute=obs_slope,
            message=(
                f"{label} growing at {obs_slope:.2f}/min "
                f"(baseline slope {bsl_slope:.3f}/min, threshold {threshold}/min)"
            ) if fired else f"{label} slope {obs_slope:.3f}/min — within normal range",
        ))

    return results


def _zscore_checker(cfg, namespace, baseline, window_start, now, endpoint, sess):
    query = _interpolate(cfg["query"], namespace=namespace)
    label = cfg["label"]
    threshold_z = cfg.get("threshold_zscore", 3.0)
    bsl = baseline.get(label, {})

    value = _instant(endpoint, query, now, sess)
    if value is None:
        return _no_data(cfg["type"], label, bsl)

    z = _zscore(value, bsl.get("mean", 0.0), bsl.get("stddev", 0.0))
    fired = abs(z) > threshold_z

    return CheckerResult(
        checker_type=cfg["type"],
        label=label,
        fired=fired,
        observed_value=value,
        baseline_mean=bsl.get("mean", 0.0),
        baseline_stddev=bsl.get("stddev", 0.0),
        baseline_slope_per_minute=bsl.get("slope_per_minute", 0.0),
        zscore=z,
        message=(
            f"{label}={value:.2f} z={z:.1f} (threshold ±{threshold_z})"
        ),
    )


def _slope_checker(cfg, namespace, baseline, window_start, now, endpoint, sess):
    query = _interpolate(cfg["query"], namespace=namespace)
    label = cfg["label"]
    threshold = cfg.get("threshold_slope_per_minute", 524288)
    bsl = baseline.get(label, {})

    series = query_range(endpoint, query, window_start, now, step=120, session=sess)
    if not series:
        return _no_data(cfg["type"], label, bsl)

    times = [t for t, _ in series]
    values = [v for _, v in series]
    obs_slope = _slope(times, values) * 60
    bsl_slope = bsl.get("slope_per_minute", 0.0)
    fired = obs_slope > threshold and bsl_slope < threshold / 10

    return CheckerResult(
        checker_type=cfg["type"],
        label=label,
        fired=fired,
        observed_value=values[-1] if values else 0.0,
        baseline_mean=bsl.get("mean", 0.0),
        baseline_stddev=bsl.get("stddev", 0.0),
        baseline_slope_per_minute=bsl_slope,
        observed_slope_per_minute=obs_slope,
        message=f"{label} slope {obs_slope:.0f}/min (threshold {threshold}/min)",
    )


def _threshold_checker(cfg, namespace, baseline, now, endpoint, sess):
    query = _interpolate(cfg["query"], namespace=namespace)
    label = cfg["label"]
    threshold_count = cfg.get("threshold_count", 1)
    bsl = baseline.get(label, {})

    value = _instant(endpoint, query, now, sess)
    if value is None:
        return _no_data(cfg["type"], label, bsl)

    fired = value >= threshold_count
    return CheckerResult(
        checker_type=cfg["type"],
        label=label,
        fired=fired,
        observed_value=value,
        baseline_mean=bsl.get("mean", 0.0),
        baseline_stddev=bsl.get("stddev", 0.0),
        baseline_slope_per_minute=bsl.get("slope_per_minute", 0.0),
        message=f"{label}={value:.0f} (threshold {threshold_count})",
    )


def _absence_checker(cfg, namespace, baseline, now, endpoint, sess):
    """Fires if the metric has been zero/absent for longer than threshold_absent_seconds."""
    query = _interpolate(cfg["query"], namespace=namespace)
    label = cfg["label"]
    threshold_sec = cfg.get("threshold_absent_seconds", 120)
    bsl = baseline.get(label, {})

    value = _instant(endpoint, query, now, sess)
    fired = value is None or value == 0.0

    return CheckerResult(
        checker_type=cfg["type"],
        label=label,
        fired=fired,
        observed_value=value or 0.0,
        baseline_mean=bsl.get("mean", 0.0),
        baseline_stddev=bsl.get("stddev", 0.0),
        baseline_slope_per_minute=bsl.get("slope_per_minute", 0.0),
        message=f"{label} absent (threshold {threshold_sec}s)",
    )


def _pct_drop_checker(cfg, namespace, baseline, now, endpoint, sess):
    query = _interpolate(cfg["query"], namespace=namespace)
    label = cfg["label"]
    threshold_pct = cfg.get("threshold_drop_pct", 50)
    bsl = baseline.get(label, {})

    value = _instant(endpoint, query, now, sess)
    if value is None:
        return _no_data(cfg["type"], label, bsl)

    bsl_mean = bsl.get("mean", 0.0)
    drop_pct = ((bsl_mean - value) / bsl_mean * 100) if bsl_mean > 0 else 0
    fired = drop_pct >= threshold_pct

    return CheckerResult(
        checker_type=cfg["type"],
        label=label,
        fired=fired,
        observed_value=value,
        baseline_mean=bsl_mean,
        baseline_stddev=bsl.get("stddev", 0.0),
        baseline_slope_per_minute=bsl.get("slope_per_minute", 0.0),
        message=f"{label} dropped {drop_pct:.0f}% from baseline (threshold {threshold_pct}%)",
    )


# ── Math helpers ──────────────────────────────────────────────────────────────

def _zscore(value: float, mean: float, stddev: float) -> float:
    if stddev == 0:
        return 0.0
    return (value - mean) / stddev


def _instant(endpoint: str, query: str, at: datetime, sess: Optional[requests.Session] = None) -> Optional[float]:
    try:
        s = sess or requests.Session()
        resp = s.get(
            f"{endpoint}/api/v1/query",
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


def _no_data(checker_type: str, label: str, bsl: dict) -> CheckerResult:
    return CheckerResult(
        checker_type=checker_type,
        label=label,
        fired=False,
        observed_value=0.0,
        baseline_mean=bsl.get("mean", 0.0),
        baseline_stddev=bsl.get("stddev", 0.0),
        baseline_slope_per_minute=bsl.get("slope_per_minute", 0.0),
        message=f"{label}: no data returned from Prometheus",
    )
