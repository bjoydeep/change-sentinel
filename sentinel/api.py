"""
Health signal endpoint — read-only FastAPI server.

change-first (or any pipeline) queries:
  GET /api/v1/health?clusters=C1,C2,C3

Returns CLEAR, DEGRADED, or ANOMALOUS with session details.
change-sentinel describes what it observed; the consumer decides how to respond.
"""

import json
import logging
from typing import Optional

from fastapi import FastAPI, Query
from pydantic import BaseModel

import db

log = logging.getLogger(__name__)

app = FastAPI(title="change-sentinel", version="0.1.0")


# ── Response models ───────────────────────────────────────────────────────────

class SessionSummary(BaseModel):
    session_id: str
    status: str
    resource_kind: str
    resource_name: str
    namespace: str
    change_description: Optional[str]
    window_opens_at: Optional[str]
    window_closes_at: Optional[str]


class HealthResponse(BaseModel):
    health: str                     # CLEAR | DEGRADED | ANOMALOUS
    clusters: list[str]
    sessions: list[SessionSummary]
    message: str


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/api/v1/health", response_model=HealthResponse)
def get_health(
    clusters: list[str] = Query(description="Cluster IDs to check"),
) -> HealthResponse:
    """
    Return the current health signal for the given clusters.

    CLEAR    — no active sessions, or all sessions completed without anomalies
    DEGRADED — sessions are WATCHING (monitoring in progress, outcome unknown)
    ANOMALOUS — one or more sessions ended with a confirmed anomaly
    """
    if not clusters:
        return HealthResponse(
            health="CLEAR",
            clusters=[],
            sessions=[],
            message="No clusters specified.",
        )

    cluster_health_rows = db.get_cluster_health(clusters)
    active_sessions = db.get_active_sessions_for_clusters(clusters)

    # Derive overall health from cluster_health table + any in-progress sessions
    health = _derive_health(cluster_health_rows, active_sessions)

    session_summaries = [
        SessionSummary(
            session_id=s["id"],
            status=s["status"],
            resource_kind=s["resource_kind"],
            resource_name=s["resource_name"],
            namespace=s["namespace"],
            change_description=_change_description(s),
            window_opens_at=s.get("window_opens_at"),
            window_closes_at=s.get("window_closes_at"),
        )
        for s in active_sessions
    ]

    return HealthResponse(
        health=health,
        clusters=clusters,
        sessions=session_summaries,
        message=_message(health, session_summaries),
    )


@app.get("/api/v1/sessions/{session_id}")
def get_session(session_id: str) -> dict:
    """Full session detail — verdict, anomalies, baseline snapshot."""
    session = db.get_session(session_id)
    if not session:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Session not found")
    # Parse JSON fields for readability
    for field in ("monitoring_plan", "baseline_snapshot", "anomalies", "verdict", "clusters_affected"):
        if session.get(field):
            try:
                session[field] = json.loads(session[field])
            except Exception:
                pass
    return session


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _derive_health(cluster_health_rows: list[dict], active_sessions: list[dict]) -> str:
    if any(s["status"] not in ("CLEAR", "ANOMALOUS") for s in active_sessions):
        return "DEGRADED"
    health_values = {row["health"] for row in cluster_health_rows}
    if "ANOMALOUS" in health_values:
        return "ANOMALOUS"
    return "CLEAR"


def _change_description(session: dict) -> str:
    return (
        f"{session['resource_kind']}/{session['resource_name']} "
        f"changed at {session.get('window_opens_at', 'unknown')} "
        f"(ns: {session['namespace']})"
    )


def _message(health: str, sessions: list[SessionSummary]) -> str:
    if health == "CLEAR":
        return "Fleet looks healthy — no active anomalies."
    if health == "DEGRADED":
        watching = [s for s in sessions if s.status not in ("CLEAR", "ANOMALOUS")]
        return (
            f"{len(watching)} session(s) in progress — monitoring outcome unknown. "
            "Consider waiting before making further changes."
        )
    anomalous = [s for s in sessions if s.status == "ANOMALOUS"]
    return (
        f"{len(anomalous)} confirmed anomaly(ies) on requested clusters. "
        "Review before proceeding."
    )
