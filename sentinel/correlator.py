"""
CorrelationAgent — the one LLM call per WatchSession.

Receives a summary of what was observed (which checkers fired, values, baseline)
and asks the model: is this a real anomaly caused by the change, how bad is it,
and what should the team do?

Uses OpenAI structured output so the pipeline never parses free text.
"""

import logging
from dataclasses import asdict

from openai import OpenAI
from pydantic import BaseModel

from checkers import CheckerResult
from config import LLMConfig

log = logging.getLogger(__name__)

SYSTEM_PROMPT = """\
You are change-sentinel, a post-deploy anomaly correlation agent.

You receive:
- A description of what changed (resource kind, name, namespace, cluster)
- A baseline snapshot (what metrics looked like BEFORE the change)
- Checker results (what metrics look like AFTER the change)

Your job:
1. Decide whether the observed deviations constitute a real anomaly
2. Decide whether the anomaly was caused by this specific change
3. Rate severity and whether it is an expected transient effect
4. Write a concise, actionable reasoning and recommended action for the on-call engineer

Be specific. Reference metric names, values, and slopes. If the baseline slope was already
non-zero before the change, factor that into your judgment.
"""


class AnomalyVerdict(BaseModel):
    is_anomaly: bool
    caused_by_change: bool
    confidence: float          # 0.0 – 1.0
    severity: str              # info | warning | critical
    expected_transient: bool
    reasoning: str
    recommended_action: str


def run(
    session: dict,
    baseline: dict,
    checker_results: list[CheckerResult],
    llm_config: LLMConfig,
) -> dict:
    """Run the correlation agent and return the AnomalyVerdict as a plain dict."""
    fired = [r for r in checker_results if r.fired]

    if not fired:
        return _clear_verdict("No checkers fired during the monitoring window.")

    prompt = _build_prompt(session, baseline, checker_results)

    log.debug("LLM prompt:\n%s", prompt)

    try:
        client = OpenAI(base_url=llm_config.base_url, api_key=llm_config.api_key)
        completion = client.beta.chat.completions.parse(
            model=llm_config.model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            response_format=AnomalyVerdict,
            temperature=0,
        )
        verdict = completion.choices[0].message.parsed
        log.info(
            "CorrelationAgent verdict: anomaly=%s caused_by_change=%s confidence=%.2f severity=%s",
            verdict.is_anomaly,
            verdict.caused_by_change,
            verdict.confidence,
            verdict.severity,
        )
        log.info("Reasoning: %s", verdict.reasoning)
        log.info("Recommended action: %s", verdict.recommended_action)
        return verdict.model_dump()

    except Exception as e:
        log.exception("CorrelationAgent LLM call failed: %s", e)
        return _error_verdict(str(e))


# ── Prompt builder ────────────────────────────────────────────────────────────

def _build_prompt(session: dict, baseline: dict, results: list[CheckerResult]) -> str:
    fired = [r for r in results if r.fired]
    not_fired = [r for r in results if not r.fired]

    lines = [
        f"## Change",
        f"- Kind: {session['resource_kind']}",
        f"- Name: {session['resource_name']}",
        f"- Namespace: {session['namespace']}",
        f"- Clusters: {session.get('clusters_affected', '[]')}",
        f"- Change time: {session.get('window_opens_at', 'unknown')}",
        "",
        "## Baseline (1h window before the change)",
    ]
    for label, stats in baseline.items():
        lines.append(
            f"- {label}: mean={stats['mean']:.3f} stddev={stats['stddev']:.3f} "
            f"slope={stats['slope_per_minute']:.4f}/min ({stats['sample_count']} samples)"
        )

    lines += ["", "## Checkers that FIRED (anomalies detected)"]
    if fired:
        for r in fired:
            lines.append(f"- [{r.checker_type}] {r.message}")
            lines.append(f"  observed={r.observed_value:.3f}  baseline_mean={r.baseline_mean:.3f}")
            if r.observed_slope_per_minute is not None:
                lines.append(f"  observed_slope={r.observed_slope_per_minute:.4f}/min  baseline_slope={r.baseline_slope_per_minute:.4f}/min")
            if r.zscore is not None:
                lines.append(f"  z-score={r.zscore:.2f}")
    else:
        lines.append("- None")

    lines += ["", "## Checkers that did NOT fire"]
    for r in not_fired:
        lines.append(f"- [{r.checker_type}] {r.label}: {r.message}")

    return "\n".join(lines)


# ── Fallback verdicts ─────────────────────────────────────────────────────────

def _clear_verdict(reason: str) -> dict:
    return {
        "is_anomaly": False,
        "caused_by_change": False,
        "confidence": 1.0,
        "severity": "info",
        "expected_transient": False,
        "reasoning": reason,
        "recommended_action": "No action required.",
    }


def _error_verdict(error: str) -> dict:
    return {
        "is_anomaly": True,
        "caused_by_change": None,
        "confidence": 0.0,
        "severity": "warning",
        "expected_transient": False,
        "reasoning": f"CorrelationAgent failed: {error}. Checkers fired — review manually.",
        "recommended_action": "Review checker results manually. LLM correlation unavailable.",
    }
