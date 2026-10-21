"""
Slack notifier — sends a notification when a WatchSession completes as ANOMALOUS.
"""

import json
import logging
from typing import Optional

import requests

log = logging.getLogger(__name__)


def send_anomaly(
    session: dict,
    verdict: dict,
    fired_checkers: list,
    webhook_url: Optional[str],
) -> None:
    if not webhook_url:
        log.info("No Slack webhook configured — printing notification to stdout")
        _print_notification(session, verdict, fired_checkers)
        return

    payload = _build_payload(session, verdict, fired_checkers)
    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
        resp.raise_for_status()
        log.info("Slack notification sent for session %s", session["id"])
    except Exception as e:
        log.error("Slack notification failed: %s", e)


def _build_payload(session: dict, verdict: dict, fired_checkers: list) -> dict:
    severity_emoji = {
        "info": ":information_source:",
        "warning": ":warning:",
        "critical": ":red_circle:",
    }.get(verdict.get("severity", "warning"), ":warning:")

    checker_lines = "\n".join(
        f"  • {c['checker_type']}: {c['message']}"
        for c in fired_checkers
    )

    return {
        "text": (
            f"{severity_emoji} *change-sentinel: ANOMALOUS* — "
            f"`{session['resource_kind']}/{session['resource_name']}` "
            f"(ns: `{session['namespace']}`)"
        ),
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"change-sentinel: ANOMALOUS — {session['resource_kind']}/{session['resource_name']}",
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Namespace:*\n`{session['namespace']}`"},
                    {"type": "mrkdwn", "text": f"*Clusters:*\n{session.get('clusters_affected', '[]')}"},
                    {"type": "mrkdwn", "text": f"*Change at:*\n{session.get('window_opens_at', 'unknown')}"},
                    {"type": "mrkdwn", "text": f"*Severity:*\n{verdict.get('severity', 'warning')}"},
                    {"type": "mrkdwn", "text": f"*Confidence:*\n{verdict.get('confidence', 0):.0%}"},
                ],
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Checkers fired:*\n{checker_lines or '(none)'}",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Assessment:*\n{verdict.get('reasoning', '')}",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Suggested action:*\n{verdict.get('recommended_action', '')}",
                },
            },
        ],
    }


def _print_notification(session: dict, verdict: dict, fired_checkers: list) -> None:
    print("\n" + "=" * 70)
    print(f"  change-sentinel: ANOMALOUS")
    print(f"  {session['resource_kind']}/{session['resource_name']} ({session['namespace']})")
    print("=" * 70)
    print(f"  Change at:   {session.get('window_opens_at')}")
    print(f"  Severity:    {verdict.get('severity')}")
    print(f"  Confidence:  {verdict.get('confidence', 0):.0%}")
    print(f"\n  Checkers fired:")
    for c in fired_checkers:
        print(f"    • {c['checker_type']}: {c['message']}")
    print(f"\n  Assessment:\n  {verdict.get('reasoning')}")
    print(f"\n  Suggested action:\n  {verdict.get('recommended_action')}")
    print("=" * 70 + "\n")
