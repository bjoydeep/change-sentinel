"""
WatchSession lifecycle orchestrator.

Runs the full pipeline for a single WatchSession in its own thread:
  PENDING → BASELINING → WATCHING → CORRELATING → CLEAR | ANOMALOUS

Each state transition is persisted to the DB immediately so the health signal
endpoint always reflects current reality.
"""

import json
import logging
import time
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

import baseline as baseline_mod
import checkers as checkers_mod
import correlator
import db
import notifier
from config import Config

log = logging.getLogger(__name__)


def run(session_id: str, cfg: Config) -> None:
    """Entry point — called in a dedicated thread per session."""
    session = db.get_session(session_id)
    if not session:
        log.error("Session %s not found", session_id)
        return

    plan = json.loads(session["monitoring_plan"])
    namespace = session["namespace"]
    window_opens_at = datetime.fromisoformat(session["window_opens_at"])
    window_closes_at = datetime.fromisoformat(session["window_closes_at"])

    log.info(
        "Session %s starting: %s/%s window=%s→%s",
        session_id[:8],
        session["resource_kind"],
        session["resource_name"],
        window_opens_at.strftime("%H:%M"),
        window_closes_at.strftime("%H:%M"),
    )

    # ── BASELINING ────────────────────────────────────────────────────────────

    db.update_session_status(session_id, "BASELINING")
    snapshot = baseline_mod.capture(
        checkers=plan["checkers"],
        namespace=namespace,
        event_timestamp=window_opens_at,
        metrics_endpoint=cfg.metrics.endpoint,
        token_path=cfg.metrics.token_path,
        verify_tls=cfg.metrics.verify_tls,
    )
    db.update_session_baseline(session_id, snapshot)

    # ── WATCHING ──────────────────────────────────────────────────────────────

    db.update_session_status(session_id, "WATCHING")
    poll_seconds = plan.get("poll_interval_seconds", 120)
    all_checker_results = []

    while datetime.now(timezone.utc) < window_closes_at:
        results = checkers_mod.run_all(
            checkers_config=plan["checkers"],
            namespace=namespace,
            baseline=snapshot,
            window_start=window_opens_at,
            metrics_endpoint=cfg.metrics.endpoint,
            token_path=cfg.metrics.token_path,
            verify_tls=cfg.metrics.verify_tls,
        )
        all_checker_results = results  # keep latest run
        fired = [r for r in results if r.fired]

        if fired:
            log.warning(
                "Session %s: %d checker(s) fired — %s",
                session_id[:8],
                len(fired),
                [r.label for r in fired],
            )

        db.update_session_anomalies(
            session_id,
            [asdict(r) for r in results],
        )
        time.sleep(poll_seconds)

    # ── CORRELATING ───────────────────────────────────────────────────────────

    db.update_session_status(session_id, "CORRELATING")
    session = db.get_session(session_id)  # re-fetch for latest anomalies

    verdict = correlator.run(
        session=session,
        baseline=snapshot,
        checker_results=all_checker_results,
        llm_config=cfg.llm,
    )

    # ── CLEAR or ANOMALOUS ────────────────────────────────────────────────────

    final_status = _determine_status(verdict)
    db.complete_session(session_id, final_status, verdict)

    clusters = json.loads(session["clusters_affected"])
    description = f"{session['resource_kind']}/{session['resource_name']} changed at {window_opens_at.isoformat()}"
    for cluster_id in clusters:
        db.upsert_cluster_health(cluster_id, session_id, final_status, description)

    log.info("Session %s completed: %s (confidence %.2f)", session_id[:8], final_status, verdict.get("confidence", 0))

    if final_status == "ANOMALOUS":
        fired_checkers = [asdict(r) for r in all_checker_results if r.fired]
        notifier.send_anomaly(
            session=session,
            verdict=verdict,
            fired_checkers=fired_checkers,
            webhook_url=cfg.slack_webhook_url,
        )
        db.mark_notification_sent(session_id)


def _determine_status(verdict: dict) -> str:
    """
    ANOMALOUS if the LLM says it's a real anomaly caused by the change
    with meaningful confidence. CLEAR otherwise.
    """
    if (
        verdict.get("is_anomaly")
        and verdict.get("caused_by_change")
        and verdict.get("confidence", 0) >= 0.6
        and not verdict.get("expected_transient")
    ):
        return "ANOMALOUS"
    return "CLEAR"
