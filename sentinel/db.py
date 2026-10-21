"""
SQLite persistence layer.

Three tables matching the starter data model:
  change_events   — append-only event ledger
  watch_sessions  — one per logical change, owns the full lifecycle
  cluster_health  — latest health signal per cluster, queryable by change-first
"""

import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

DB_PATH = Path(os.environ.get("DB_PATH", "sentinel.db"))


def init_db(path: Path = DB_PATH) -> None:
    with _conn(path) as c:
        c.executescript("""
            CREATE TABLE IF NOT EXISTS change_events (
                id                TEXT PRIMARY KEY,
                source            TEXT NOT NULL,
                resource_kind     TEXT NOT NULL,
                resource_name     TEXT NOT NULL,
                namespace         TEXT NOT NULL,
                cluster           TEXT NOT NULL,
                event_timestamp   TEXT NOT NULL,
                ingested_at       TEXT NOT NULL,
                raw_payload       TEXT,            -- JSON
                watch_session_id  TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_ce_unprocessed
                ON change_events (watch_session_id)
                WHERE watch_session_id IS NULL;

            CREATE TABLE IF NOT EXISTS watch_sessions (
                id                 TEXT PRIMARY KEY,
                trigger_event_id   TEXT NOT NULL,
                resource_kind      TEXT NOT NULL,
                resource_name      TEXT NOT NULL,
                namespace          TEXT NOT NULL,
                clusters_affected  TEXT NOT NULL,  -- JSON array
                status             TEXT NOT NULL,  -- PENDING|BASELINING|WATCHING|CORRELATING|CLEAR|ANOMALOUS
                monitoring_plan    TEXT,            -- JSON
                baseline_snapshot  TEXT,            -- JSON
                anomalies          TEXT,            -- JSON array
                verdict            TEXT,            -- JSON
                window_opens_at    TEXT,
                window_closes_at   TEXT,
                completed_at       TEXT,
                notification_sent  INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS cluster_health (
                cluster_id             TEXT PRIMARY KEY,
                last_watch_session_id  TEXT,
                health                 TEXT NOT NULL DEFAULT 'UNKNOWN',
                updated_at             TEXT NOT NULL,
                change_description     TEXT
            );
        """)


@contextmanager
def _conn(path: Path = DB_PATH):
    con = sqlite3.connect(path, isolation_level=None)
    con.row_factory = sqlite3.Row
    try:
        yield con
    finally:
        con.close()


# ── change_events ─────────────────────────────────────────────────────────────

def insert_change_event(event: dict) -> None:
    with _conn() as c:
        c.execute(
            """
            INSERT OR IGNORE INTO change_events
                (id, source, resource_kind, resource_name, namespace, cluster,
                 event_timestamp, ingested_at, raw_payload)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event["id"],
                event["source"],
                event["resource_kind"],
                event["resource_name"],
                event["namespace"],
                event["cluster"],
                event["event_timestamp"],
                _now(),
                json.dumps(event.get("raw_payload", {})),
            ),
        )


def fetch_unprocessed_events() -> list[dict]:
    with _conn() as c:
        rows = c.execute(
            "SELECT * FROM change_events WHERE watch_session_id IS NULL ORDER BY event_timestamp"
        ).fetchall()
    return [dict(r) for r in rows]


def assign_event_to_session(event_id: str, session_id: str) -> None:
    with _conn() as c:
        c.execute(
            "UPDATE change_events SET watch_session_id = ? WHERE id = ?",
            (session_id, event_id),
        )


# ── watch_sessions ────────────────────────────────────────────────────────────

def insert_watch_session(session: dict) -> None:
    with _conn() as c:
        c.execute(
            """
            INSERT INTO watch_sessions
                (id, trigger_event_id, resource_kind, resource_name, namespace,
                 clusters_affected, status, window_opens_at, window_closes_at)
            VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?, ?)
            """,
            (
                session["id"],
                session["trigger_event_id"],
                session["resource_kind"],
                session["resource_name"],
                session["namespace"],
                json.dumps(session["clusters_affected"]),
                session["window_opens_at"],
                session["window_closes_at"],
            ),
        )


def update_session_status(session_id: str, status: str) -> None:
    with _conn() as c:
        c.execute(
            "UPDATE watch_sessions SET status = ? WHERE id = ?",
            (status, session_id),
        )


def update_session_plan(session_id: str, plan: dict) -> None:
    with _conn() as c:
        c.execute(
            "UPDATE watch_sessions SET monitoring_plan = ? WHERE id = ?",
            (json.dumps(plan), session_id),
        )


def update_session_baseline(session_id: str, baseline: dict) -> None:
    with _conn() as c:
        c.execute(
            "UPDATE watch_sessions SET baseline_snapshot = ? WHERE id = ?",
            (json.dumps(baseline), session_id),
        )


def update_session_anomalies(session_id: str, anomalies: list) -> None:
    with _conn() as c:
        c.execute(
            "UPDATE watch_sessions SET anomalies = ? WHERE id = ?",
            (json.dumps(anomalies), session_id),
        )


def complete_session(session_id: str, status: str, verdict: dict) -> None:
    with _conn() as c:
        c.execute(
            """
            UPDATE watch_sessions
               SET status = ?, verdict = ?, completed_at = ?
             WHERE id = ?
            """,
            (status, json.dumps(verdict), _now(), session_id),
        )


def mark_notification_sent(session_id: str) -> None:
    with _conn() as c:
        c.execute(
            "UPDATE watch_sessions SET notification_sent = 1 WHERE id = ?",
            (session_id,),
        )


def get_session(session_id: str) -> Optional[dict]:
    with _conn() as c:
        row = c.execute(
            "SELECT * FROM watch_sessions WHERE id = ?", (session_id,)
        ).fetchone()
    return dict(row) if row else None


def get_active_sessions_for_clusters(cluster_ids: list[str]) -> list[dict]:
    placeholders = ",".join("?" * len(cluster_ids))
    with _conn() as c:
        rows = c.execute(
            f"""
            SELECT * FROM watch_sessions
             WHERE status NOT IN ('CLEAR', 'ANOMALOUS')
               AND clusters_affected LIKE '%' || ? || '%'
            """,
            (cluster_ids[0],),
        ).fetchall()
    return [dict(r) for r in rows]


# ── cluster_health ────────────────────────────────────────────────────────────

def upsert_cluster_health(cluster_id: str, session_id: str, health: str, description: str) -> None:
    with _conn() as c:
        c.execute(
            """
            INSERT INTO cluster_health (cluster_id, last_watch_session_id, health, updated_at, change_description)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(cluster_id) DO UPDATE SET
                last_watch_session_id = excluded.last_watch_session_id,
                health                = excluded.health,
                updated_at            = excluded.updated_at,
                change_description    = excluded.change_description
            """,
            (cluster_id, session_id, health, _now(), description),
        )


def get_cluster_health(cluster_ids: list[str]) -> list[dict]:
    placeholders = ",".join("?" * len(cluster_ids))
    with _conn() as c:
        rows = c.execute(
            f"SELECT * FROM cluster_health WHERE cluster_id IN ({placeholders})",
            cluster_ids,
        ).fetchall()
    return [dict(r) for r in rows]


# ── internal ──────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
