"""
Scanner — runs every N seconds (default 300 / 5 min).

Pulls unprocessed ChangeEvents, debounces rapid updates to the same CR,
creates one WatchSession per logical change, then spawns a session thread.
"""

import json
import logging
import threading
import uuid
from datetime import datetime, timedelta, timezone
from itertools import groupby

import db
import session as session_mod
from config import Config

log = logging.getLogger(__name__)

# Two events on the same CR within this window → collapse to one WatchSession.
# The scanner's own interval (5 min) provides natural debouncing; this is a
# belt-and-suspenders guard for events ingested in the same run.
DEBOUNCE_SECONDS = 120


def start(cfg: Config, stop_event: threading.Event) -> threading.Thread:
    """Start the scanner loop in a daemon thread."""
    t = threading.Thread(
        target=_loop,
        args=(cfg, stop_event),
        daemon=True,
        name="scanner",
    )
    t.start()
    log.info("Scanner started (interval=%ds)", cfg.scanner_interval_seconds)
    return t


def _loop(cfg: Config, stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        try:
            _scan(cfg)
        except Exception as e:
            log.exception("Scanner error: %s", e)
        stop_event.wait(cfg.scanner_interval_seconds)


def _scan(cfg: Config) -> None:
    events = db.fetch_unprocessed_events()
    if not events:
        log.debug("Scanner: no unprocessed events")
        return

    log.info("Scanner: %d unprocessed event(s)", len(events))
    groups = _group_events(events)

    for (kind, name, ns), group_events in groups.items():
        _create_session(group_events, cfg)


# ── Grouping + debounce ───────────────────────────────────────────────────────

def _group_events(events: list[dict]) -> dict:
    """
    Group events by (resource_kind, resource_name, namespace).
    Multiple events for the same CR within DEBOUNCE_SECONDS → one group.
    """
    groups: dict[tuple, list[dict]] = {}
    for event in events:
        key = (event["resource_kind"], event["resource_name"], event["namespace"])
        groups.setdefault(key, []).append(event)
    return groups


# ── Session creation ──────────────────────────────────────────────────────────

def _create_session(events: list[dict], cfg: Config) -> None:
    # Use earliest event_timestamp as the anchor (baseline + monitoring window).
    timestamps = [datetime.fromisoformat(e["event_timestamp"]) for e in events]
    window_opens_at = min(timestamps)

    first_event = min(events, key=lambda e: e["event_timestamp"])
    resource_kind = first_event["resource_kind"]
    resource_name = first_event["resource_name"]
    namespace = first_event["namespace"]
    cluster = first_event["cluster"]

    plan = _load_plan(resource_kind, namespace, events, cfg)
    window_minutes = plan["window_minutes"]
    window_closes_at = window_opens_at + timedelta(minutes=window_minutes)

    session_id = str(uuid.uuid4())
    session = {
        "id": session_id,
        "trigger_event_id": first_event["id"],
        "resource_kind": resource_kind,
        "resource_name": resource_name,
        "namespace": namespace,
        "clusters_affected": [cluster],
        "window_opens_at": window_opens_at.isoformat(),
        "window_closes_at": window_closes_at.isoformat(),
    }

    db.insert_watch_session(session)
    db.update_session_plan(session_id, plan)
    for event in events:
        db.assign_event_to_session(event["id"], session_id)

    log.info(
        "WatchSession %s created: %s/%s window=%dmin plan=%s",
        session_id[:8], resource_kind, resource_name, window_minutes,
        "OperatorCR" if resource_kind not in cfg.monitoring_plans else resource_kind,
    )

    # Spawn session thread — runs the full lifecycle independently.
    t = threading.Thread(
        target=session_mod.run,
        args=(session_id, cfg),
        daemon=True,
        name=f"session/{session_id[:8]}",
    )
    t.start()


def _load_plan(resource_kind: str, namespace: str, events: list[dict], cfg: Config) -> dict:
    """
    Look up the monitoring plan by resource kind.
    Falls back to OperatorCR if no named plan exists.
    """
    plan_key = resource_kind if resource_kind in cfg.monitoring_plans else "OperatorCR"
    plan = cfg.monitoring_plans[plan_key].copy()

    # Resolve {{namespace}} template in checker queries.
    plan["checkers"] = [
        _resolve_checker(c, namespace, events)
        for c in plan.get("checkers", [])
    ]
    return plan


def _resolve_checker(checker: dict, namespace: str, events: list[dict]) -> dict:
    """Replace {{namespace}} and other placeholders in checker config."""
    checker = checker.copy()

    def interpolate(s: str) -> str:
        return s.replace("{{namespace}}", namespace)

    if "query" in checker:
        checker["query"] = interpolate(checker["query"])
    if "metrics" in checker:
        checker["metrics"] = [
            {**m, "query": interpolate(m["query"])}
            for m in checker["metrics"]
        ]
    return checker
