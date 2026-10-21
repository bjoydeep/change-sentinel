# change-sentinel: Starter Version

> Scoped-down v0.1. Proves the core loop works before adding generality.
> Full design at `design.md`. This doc captures what's cut, what's kept, and why.

---

## Goal

A working change health advisor anchored to two concrete, demonstrable scenarios:

1. **The leaky operator** — a CR spec change triggers a reconcile bug that slowly
   accumulates Secrets in the kube API server
2. **The runaway CronJob** — a Job execution takes 6x longer than its historical
   baseline, creating silent resource pressure

Both are caught by the same loop: CR or Job event → baseline captured → anomaly
detected → health signal updated → any pipeline can query it and decide how to respond.

---

## What's Cut vs the Full Design

| Full design | Starter | Reason |
|---|---|---|
| Multiple ChangeSource implementations | Two built-in watchers only | Covers the two primary scenarios; others are additive |
| Canonicalization tier (`canonical_changes`) | Removed | Single source per event type = no cross-source dupes |
| LLM-derived MonitoringPlan | Static YAML config per resource kind | Limits scope to known types; LLM added later |
| Pluggable anomaly algorithms | Z-score + slope only | Sufficient for both scenarios |
| EventSequenceAnalyzer | Removed | Metrics only for v0.1 |
| Storm detection | Removed | Assume sane event rate |
| Concurrent session confidence degradation | Removed | Assume one change at a time per cluster |
| Multiple NotifierSink implementations | Slack only | Ship something; others are additive |

**What is NOT cut:**
- Event ledger DB — the foundation everything else builds on
- 5-min scanner pattern — right architecture even for v0.1
- WatchSession lifecycle — same states, simplified internals
- Thanos baseline anchored to `event_timestamp` — the core insight; removing it kills the value
- Health signal endpoint — this is the point; without it this is just a monitor

---

## Watch Targets (v0.1)

The starter watches three categories. Trigger and observation surface differ by category.

| Watch target | Trigger | Observation surface |
|---|---|---|
| Operator CR (any opted-in CRD) | CR spec changed (generation bump) | kube API write rate + resource count in owning namespace |
| ACM Policy | Compliance status transition | Node restarts, metrics gaps, image pull failures on affected clusters |
| CronJob execution | Job created by opted-in CronJob | Duration vs historical baseline, failure count, Job object accumulation |

**Why CRs and not Deployments?**

A CR change is the *intent* — what the user asked the system to do. The kube API
server is the universal observation surface regardless of what triggered the watch:
what resources is the operator creating, modifying, or deleting at an abnormal rate?
Watching intent gives a cleaner correlation anchor than watching the mechanism.

A CR's `generation` field only increments when `spec` changes. Status updates
written back by the controller do NOT bump `generation`. So the controller's own
reconcile loop writing back status never produces a spurious WatchSession.

**CronJob as a special case:**

CronJob executions are not configuration changes — they are scheduled operations.
But they are a common source of silent production problems: jobs drifting longer
each week, accumulating Job objects (history limit not set), or failing silently.
Watching each execution against a historical baseline catches these.

---

## The Starter Spine

```
Event Sources (two built-in watchers)
  ├── CRInstanceWatcher   — watches opted-in CRDs for CR spec changes
  │     trigger: generation bump on any CR whose CRD has the opt-in label
  │     covers:  operator CRs, ACM Policies, ArgoCD Applications/AppSets
  └── CronJobWatcher      — watches opted-in CronJobs for Job creation events
    │
    │  emit ChangeEvent
    ▼
change_events  (DB: append-only ledger)
    ▼
Scanner  (every 5 min)
    │  pulls unprocessed events
    │  debounces: multiple events on same CR within 5-min window → one WatchSession
    │  creates one WatchSession per logical change
    ▼
WatchSession  (DB)
    │  PENDING → BASELINING → WATCHING → CORRELATING → CLEAR | ANOMALOUS
    │
    ├── MonitoringPlanLoader        (static YAML config lookup by resource_kind)
    ├── BaselineCapture             (Thanos: event_timestamp - 1h to event_timestamp - 5min)
    ├── MetricPoller                (runs checkers against live metrics at poll interval)
    │     ├── ResourceAccumulationChecker  (slope-of-count, for operator CR kind)
    │     ├── APIWriteRateSpike            (Z-score delta vs baseline)
    │     ├── MemoryDrift                  (slope, for operator kind)
    │     ├── JobDurationAnomaly           (Z-score vs historical runs, for CronJob)
    │     ├── JobAccumulation              (slope-of-count, for CronJob)
    │     ├── JobFailureRate               (threshold, for CronJob)
    │     ├── RestartRateSpike             (Z-score, for ACM Policy kind)
    │     ├── NodeNotReady                 (threshold, for ACM Policy kind)
    │     ├── MetricsGap                   (absence, for ACM Policy kind)
    │     └── ImagePullBackOff             (Z-score, for ACM Policy kind)
    └── CorrelationAgent            (LLM → AnomalyVerdict — only LLM call in the system)
            │
            ▼
    cluster_health  (DB)
            │
            ▼
    SlackNotifier  +  Health Signal Endpoint  (FastAPI, queryable by any pipeline)
```

---

## Data Model (Simplified)

Two tables. No canonicalization tier.

### `change_events`

```sql
change_events (
  id               UUID PRIMARY KEY,
  source           TEXT,        -- 'cr_instance' | 'acm_policy' | 'cronjob_execution'
  resource_kind    TEXT,        -- CR kind ('Widget', 'Policy', etc.) or 'CronJob'
  resource_name    TEXT,
  namespace        TEXT,
  cluster          TEXT,
  event_timestamp  TIMESTAMPTZ, -- when the change happened (baseline anchor)
  ingested_at      TIMESTAMPTZ,
  raw_payload      JSONB,       -- CR spec diff or Job metadata
  watch_session_id UUID         -- NULL until scanner assigns
)
```

### `watch_sessions`

```sql
watch_sessions (
  id                  UUID PRIMARY KEY,
  trigger_event_id    UUID,
  resource_kind       TEXT,
  resource_name       TEXT,
  namespace           TEXT,
  clusters_affected   TEXT[],
  status              TEXT,        -- PENDING|BASELINING|WATCHING|CORRELATING|CLEAR|ANOMALOUS
  monitoring_plan     JSONB,       -- loaded from static config (not LLM)
  baseline_snapshot   JSONB,
  anomalies           JSONB,
  verdict             JSONB,       -- LLM AnomalyVerdict
  window_opens_at     TIMESTAMPTZ,
  window_closes_at    TIMESTAMPTZ,
  completed_at        TIMESTAMPTZ,
  notification_sent   BOOL DEFAULT FALSE
)
```

### `cluster_health`

```sql
cluster_health (
  cluster_id            TEXT PRIMARY KEY,
  last_watch_session_id UUID,
  health                TEXT,        -- CLEAR|DEGRADED|ANOMALOUS|UNKNOWN
  updated_at            TIMESTAMPTZ,
  change_description    TEXT
)
```

---

## Spurious Trigger Filters

CR-based watching is inherently much less noisy than Deployment watching.
Three simple filters are all that's needed.

### Filter 1 — Opt-in label on the CRD (not individual CR instances)

```yaml
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: widgets.example.com
  labels:
    change-sentinel.io/watch-instances: "true"
  annotations:
    change-sentinel.io/observe-resources: "secrets,configmaps"
    change-sentinel.io/observe-namespace: "{{.metadata.namespace}}"
```

One label on the CRD covers every instance automatically. System CRDs
(`networking.k8s.io`, `apps`, etc.) never carry this label — no exclusion
lists needed.

### Filter 2 — Generation bump only

```python
def is_spec_change(old_cr, new_cr) -> bool:
    return new_cr.metadata.generation > old_cr.metadata.generation
```

Kubernetes guarantees `generation` only increments on spec changes. Status
write-backs, label edits, and annotation changes are ignored for free.
The controller's own reconcile loop writing status never fires a WatchSession.

### Filter 3 — Debounce (scanner window)

Multiple rapid spec changes to the same CR within a 5-min scanner window
→ one WatchSession. The earliest `event_timestamp` is used as the baseline
anchor. No extra logic needed — the scanner absorbs this naturally.

### CronJob: Opt-in label on the CronJob

```yaml
metadata:
  name: nightly-cleanup
  namespace: data-pipeline-ns
  labels:
    change-sentinel.io/watch-executions: "true"
```

Every Job created by this CronJob triggers a WatchSession. Jobs from
CronJobs without the label are ignored entirely.

---

## Static MonitoringPlan Config

A YAML file bundled with the deployment. `MonitoringPlanLoader` does a lookup
by `resource_kind`. `OperatorCR` is the fallback plan applied to any opted-in
CRD that does not have a named entry — it watches the kube API server for
accumulation regardless of what the operator specifically does.

```yaml
# Fallback plan for any opted-in CR instance
OperatorCR:
  window_minutes: 60
  extended_window_minutes: 180
  poll_interval_seconds: 120
  checkers:
    - type: ResourceAccumulationChecker
      metrics: [kube_secret_count, kube_configmap_count]
      label_filters:
        namespace: "{{observe_namespace}}"   # resolved from CRD annotation
      threshold_slope_per_minute: 0.3
    - type: APIWriteRateSpike
      metric: apiserver_request_total
      label_filters:
        verb: POST
        namespace: "{{observe_namespace}}"
      threshold_delta_over_baseline: 10
    - type: MemoryDrift
      metric: container_memory_working_set_bytes
      label_filters:
        namespace: "{{observe_namespace}}"
      threshold_slope_per_minute: 524288    # 512KB/min sustained growth

CronJob:
  window_minutes: 120                       # long enough to cover the full run
  extended_window_minutes: 240
  poll_interval_seconds: 60
  checkers:
    - type: JobDurationAnomaly
      metric: kube_job_status_completion_time
      baseline_source: historical_runs      # past 14 days of this CronJob's runs
      threshold_zscore: 2.5
    - type: JobFailureRate
      metric: kube_job_failed
      label_filters:
        namespace: "{{.metadata.namespace}}"
      threshold_count: 1
    - type: JobAccumulation
      metric: kube_job_count
      label_filters:
        namespace: "{{.metadata.namespace}}"
      threshold_slope_per_minute: 0.05     # jobs piling up = history limit not set

# Named plans take precedence over OperatorCR for exact kind matches
MachineConfig:
  window_minutes: 30
  extended_window_minutes: 120
  poll_interval_seconds: 60
  checkers:
    - type: RestartRateSpike
      metric: kube_pod_container_status_restarts_total
      label_filters:
        namespace: "openshift-*"
      threshold_delta_over_baseline: 5
    - type: NodeNotReady
      metric: kube_node_status_condition
      label_filters:
        condition: Ready
        status: "false"
      threshold_count: 1

cluster-monitoring-config:
  window_minutes: 15
  extended_window_minutes: 60
  poll_interval_seconds: 30
  checkers:
    - type: MetricsGap
      metric: up
      label_filters:
        job: "prometheus-k8s"
      threshold_absent_seconds: 120
    - type: ScrapeTargetLoss
      metric: scrape_samples_scraped
      threshold_drop_pct_over_baseline: 50

ImageDigestMirrorSet:
  window_minutes: 30
  extended_window_minutes: 120
  poll_interval_seconds: 60
  checkers:
    - type: ImagePullBackOff
      metric: kube_pod_container_status_waiting_reason
      label_filters:
        reason: ImagePullBackOff
      threshold_delta_over_baseline: 3
```

Adding a new resource kind = editing this file. No code change.
Unknown opted-in CRDs fall back to `OperatorCR` automatically.

---

## Known Anomaly Checkers

| Checker | Signal shape | Detection method | Fires when |
|---|---|---|---|
| `ResourceAccumulationChecker` | Monotonic count increase | Slope | Slope > threshold AND baseline slope < 0.05 |
| `APIWriteRateSpike` | Write rate delta | Z-score | Z-score > 3.0 vs baseline |
| `MemoryDrift` | Sustained memory growth | Slope | Slope > threshold AND baseline slope < threshold/10 |
| `JobDurationAnomaly` | Job completion time | Z-score vs 14-day history | Z-score > 2.5 |
| `JobAccumulation` | Job object count | Slope | Slope > 0.05 jobs/min |
| `JobFailureRate` | `kube_job_failed` count | Threshold | Count > 0 |
| `RestartRateSpike` | Restart count delta | Z-score | Z-score > 3.0 vs baseline |
| `NodeNotReady` | Node Ready=false count | Threshold | Count > 1 on affected clusters |
| `MetricsGap` | Prometheus `up` absent | Absence | `up = 0` for > threshold seconds |
| `ScrapeTargetLoss` | Scraped sample count drop | Percentage | Drop > threshold % vs baseline |
| `ImagePullBackOff` | Waiting reason count | Z-score | Z-score > 3.0 vs baseline |

**Z-score** (spike detection):
```
baseline_mean, baseline_stddev = Thanos[T-1h : T-5min]
z_score = (observed_value - baseline_mean) / baseline_stddev
anomaly if z_score > threshold  (default 3.0)
```

**Slope** (accumulation / drift detection):
```
samples       = metric values at each poll interval over monitoring window
slope         = linear regression gradient (units per minute)
baseline_slope = same regression over Thanos[T-1h : T-5min]
anomaly if slope > threshold AND baseline_slope < 0.05
```

The baseline slope check is important: a metric already growing before the
change is not attributed to the change.

**Historical runs** (for JobDurationAnomaly):
```
historical_durations = completion_time - start_time
                       for all Jobs from this CronJob over past 14 days in Thanos
baseline_mean, baseline_stddev = from historical_durations
z_score = (current_duration - baseline_mean) / baseline_stddev
```

---

## LLM Role (Narrow)

The LLM runs exactly once per WatchSession. The entry point is `correlator.run()`
in `correlator.py`, called from `session.py` after the monitoring window closes:

```python
verdict = correlator.run(
    session=session,                  # resource kind, name, namespace, clusters
    baseline=snapshot,                # mean, stddev, slope per metric (pre-change)
    checker_results=all_checker_results,  # list[CheckerResult] — fired and not fired
    llm_config=cfg.llm,
)
```

That is the only LLM call in the system. It is not asked to decide what to watch
(that is the static config's job) — only to interpret what was observed.

Input:
- Resource kind, name, namespace
- Clusters affected
- Baseline snapshot summary (mean, stddev, slope per metric)
- All checker results — both fired and not fired, so the LLM sees the full picture

Output (`AnomalyVerdict`, structured):

```json
{
  "is_anomaly": true,
  "caused_by_change": true,
  "confidence": 0.91,
  "severity": "warning",
  "expected_transient": false,
  "reasoning": "...",
  "recommended_action": "..."
}
```

---

## Baseline: The Critical Piece

All windows anchor to `event_timestamp` from the raw event — not to scan time.

```
CR spec changed at      T=0  → stored with event_timestamp = T
Scanner runs at         T+5  → creates WatchSession
Baseline query:  Thanos[ T-60min : T-5min ]  ← anchored to T
Monitoring window: T → T+60min               ← anchored to T
```

A pre-existing growing metric before T appears in the baseline slope and is
not falsely attributed to the change.

For CronJob executions, the baseline is a distribution of historical run
durations (past 14 days), not a time-window average.

---

## Worked Example 1: The Leaky Operator

### Setup

A team updates a Widget CR spec. The operator reconciles and has a bug: it
creates a new Secret on every reconcile instead of checking if one exists.

The CRD is opted in:
```yaml
metadata:
  name: widgets.example.com
  labels:
    change-sentinel.io/watch-instances: "true"
  annotations:
    change-sentinel.io/observe-resources: "secrets,configmaps"
    change-sentinel.io/observe-namespace: "{{.metadata.namespace}}"
```

### Timeline

```
14:30  Widget CR spec updated (syncMode: aggressive added)
         → generation 3 → 4
         → CRInstanceWatcher fires (generation bumped, CRD opted-in)
         → ChangeEvent: resource_kind=Widget, event_timestamp=14:30

14:35  Scanner runs. WatchSession created (plan: OperatorCR).
         → Baseline: kube_secret_count mean=12, stddev=0.4, slope≈0

14:35–15:30  MetricPoller every 2 min.
         → 14:37: secret_count=14  slope=+1.0/min  ← ResourceAccumulationChecker fires
         → 14:47: secret_count=24  slope=+1.0/min
         → API POST to secrets: 8x baseline         ← APIWriteRateSpike fires

15:30  Window closes. CorrelationAgent runs.
15:31  WatchSession → ANOMALOUS. Slack sent. cluster_health updated.
```

### Notification

> **change-sentinel: ANOMALOUS — Widget/my-widget (demo-ns)**
>
> Widget CR `my-widget` spec changed at 14:30 (generation 3→4).
>
> **Observed:**
> `kube_secret_count` growing +1.0/min (baseline slope ≈ 0). API server POST
> rate to secrets 8x baseline. 72 secrets in demo-ns; 60 created after 14:30.
>
> **Assessment (confidence 0.91):** Consistent with a reconcile loop creating
> Secrets on every pass without an idempotency check. Not pre-existing.
>
> **Suggested action:** Revert Widget CR or patch operator. Clean up accumulated
> secrets before proceeding.

---

## Worked Example 2: The Runaway CronJob

### Setup

A nightly data-cleanup CronJob completes in ~8 minutes historically. A new
script version has an N+1 query bug. It now takes 45+ minutes.

```yaml
metadata:
  name: nightly-cleanup
  namespace: data-pipeline-ns
  labels:
    change-sentinel.io/watch-executions: "true"
```

### Timeline

```
02:00  CronJob fires. Job nightly-cleanup-28765432 created.
         → CronJobWatcher fires (CronJob opted-in)
         → ChangeEvent: resource_kind=CronJob, event_timestamp=02:00

02:05  Scanner runs. WatchSession created (plan: CronJob).
         → Baseline: historical runs mean=8.2min, stddev=1.1min (p95=10.5min)

02:05–04:00  MetricPoller every 60s.
         → 02:25: job still running (25 min)
                  z_score = (25 - 8.2) / 1.1 = 15.3  ← JobDurationAnomaly fires
         → 02:30: kube_job_count = 18 (was 7)         ← JobAccumulation fires
                  (history limit not configured)
         → 02:51: job completes (51 min total)

04:00  Window closes. CorrelationAgent runs.
04:01  WatchSession → ANOMALOUS. Slack sent. cluster_health updated.
```

### Notification

> **change-sentinel: ANOMALOUS — CronJob/nightly-cleanup (data-pipeline-ns)**
>
> CronJob `nightly-cleanup` executed at 02:00.
>
> **Observed:**
> Job ran 51 min (historical p95: 10.5 min, z-score: 15.3). Job count in
> data-pipeline-ns grew from 7 to 18 (history limit not set — separate issue).
>
> **Assessment (confidence 0.88):** Duration 6.2x historical p95. Pattern
> consistent with a query regression or data volume increase. Job accumulation
> is unrelated to this run but worth fixing independently.
>
> **Suggested action:** Investigate query performance in cleanup script.
> Set `successfulJobsHistoryLimit: 3` and `failedJobsHistoryLimit: 1`.

---

## What This Proves

A successful starter demonstrates:

1. The event → session → health signal loop works end to end
2. CR `generation` filter means controller status write-backs never fire spurious sessions
3. Slope-based detection catches a slow resource leak that threshold alerting misses
4. Historical baseline catches a CronJob regression that Z-score on a fixed window misses
   (the job "succeeds" — it just takes 6x longer than usual)
5. The LLM verdict produces an actionable narrative, not just "metric exceeded threshold"
6. The health signal endpoint returns ANOMALOUS before a human would have noticed

If all six hold, the full design's generality is worth building.

---

## Integration with change-first (Starter Scope)

Full integration design is in `design.md → Integration with change-first`.
For the starter, implement only the first two points:

### Now: health signal query

Change-first calls the health signal endpoint before running its pre-merge vetting:

```
GET /api/v1/health?clusters=C1,C2,C3
```

If ANOMALOUS, change-first adds a PR comment naming the affected clusters and
the last known anomaly. The team decides whether to treat this as advisory or
a soft block — change-sentinel does not enforce either policy.

### Soon: PR annotation stamping

When a PR merges and the resource lands, change-first adds:

```yaml
annotations:
  change-first.io/pr: "456"
  change-first.io/pr-author: "jbanerje"
  change-first.io/risk-assessment: "medium"
  change-first.io/merged-at: "2026-10-20T14:28:00Z"
```

Change-sentinel reads these from the raw event payload and includes the PR
reference in its notification. Transforms "something looks wrong" into "PR #456
caused this." High value, low cost — implement as soon as the starter is stable.

### Later

- Historical risk feedback from change-sentinel → change-first risk scoring
- Pre-armed WatchSessions for high-risk PRs before they land

---

## Known Limitations

### Static MonitoringPlan is overfit to known scenarios

The `OperatorCR` plan in `config.yaml` hardcodes specific resource types to watch:

```yaml
checkers:
  - type: ResourceAccumulationChecker
    metrics:
      - label: kube_secret_count
      - label: kube_configmap_count
```

This catches a Secrets leak. It misses entirely if the operator leaks:
- `ServiceAccount`, `RoleBinding`, `ClusterRoleBinding`
- `Job`, `Pod`, `PVC`
- Any custom resource the operator owns

To cover all cases statically you would need to enumerate every kube-state-metrics
resource type — which is exactly the maintenance trap the full design avoids.

**The root cause:** the static config encodes domain knowledge (what this specific
operator might leak) in YAML. Add a new operator with different behaviour and the
config needs updating before the sentinel can detect its failures.

**The fix (Step 1 in the Upgrade Path):** replace the static config with the
LLM-derived `MonitoringPlan`. Given the CR spec and kind, the LLM reasons:

> *"This operator reconciles Widget CRs. I should watch for accumulation of
> secrets, configmaps, serviceaccounts, and rolebindings in its namespace,
> and monitor the operator pod's memory. Here are the PromQL queries."*

No enumeration. No YAML edits. New operator type → LLM derives the right
metric set automatically.

**Interim mitigation:** replace the specific resource type queries with a
more generic "total tracked objects in namespace" signal:

```promql
count({__name__=~"kube_.*_info", namespace="demo-operator-ns"})
```

This catches any resource type leak without knowing in advance what the operator
will create. It is still imprecise (doesn't distinguish object types) but is far
less overfit than enumerating specific resource types.

### Fixed monitoring window ignores signal strength

The monitoring window is fixed at 60 minutes regardless of how obvious the
anomaly already is. Consider what `kube_secret_count` looks like in Thanos
within 10 minutes of the Widget CR patch:

```
Before patch:  ─────────────────────  (flat, ~3 secrets)
After patch:   ──────────────────╱╱╱  (steep linear growth)
                                 ↑
                              change point
```

That inflection point is unmistakable in the first 3-4 data points — roughly
6-8 minutes after the change. The slope is 20x the threshold, consistent across
every poll, starting exactly at the event_timestamp. The evidence is overwhelming
within the first 10 minutes. The system waits another 50 minutes anyway,
collects 25 more data points that say the same thing, then calls the LLM.

The 60-minute window exists for *ambiguous* signals — subtle memory drift, a
transient spike that might self-resolve, a slow leak that needs time to become
statistically clear. For those, the full window is correct. For a signal this
clear, it is wasteful and dangerously slow — a real fleet would accumulate
hundreds of leaked objects while the sentinel waits.

**The fix: early termination when confidence is already high**

After each poll, evaluate whether evidence is already sufficient:

```
if consecutive_polls_fired  >= 5
   AND slope > 5 × threshold on every poll
   AND baseline slope was flat:
       → enough evidence, call LLM now
```

Or a simpler two-pass approach:
- **T+5min quick check** — is the signal already overwhelming?
  (slope > 5× threshold, all polls fired, z-score > 10). Alert immediately.
- **If ambiguous** — continue the full monitoring window as today.

The full design's `MonitoringPlan` has `extended_window_minutes` for signals
that need longer observation. The inverse — **early termination when confidence
is already high** — is missing entirely and should be a first-class concept.

### "No data" is indistinguishable from "no anomaly"

When a checker query returns no data (broken PromQL, missing metric, wrong label),
the checker returns `fired=False` — identical to a working query that found no
anomaly. The current code logs a warning at baseline time but is silent during
the watch window.

A wrong query like `apiserver_request_total{namespace="demo-operator-ns"}` (the
`namespace` label does not exist on that metric) returns empty, silently passes
every poll, and contributes nothing to the verdict. The system looks like it
checked something it did not.

**The fix:** add a `DATA_MISSING` state to `CheckerResult`, distinct from
`fired=False`. The LLM prompt should include `DATA_MISSING` checkers explicitly
so the verdict can say: *"I could not verify api_post_rate — that signal is
unavailable."* A session with multiple `DATA_MISSING` checkers should carry
reduced confidence in the verdict.

---

## Upgrade Path to Full Design

Each step is additive — nothing in the starter gets ripped out:

| Step | What it adds | Unlocks |
|---|---|---|
| 1 | LLM-derived `MonitoringPlan` | Arbitrary resource kinds without YAML edits |
| 2 | `canonical_changes` tier + Canonicalizer | Multi-source ingestion without dupes |
| 3 | Additional `ChangeSource` implementations | ArgoCD sync events, ManifestWork, Tekton |
| 4 | `EventSequenceAnalyzer` | Absence detection, out-of-order operator events |
| 5 | Pluggable anomaly algorithms (CUSUM, Isolation Forest) | Subtle drift, multi-metric correlation |
| 6 | Storm detection + STORM-type WatchSessions | Meta-analysis of event floods |

---

## Open Questions (Starter-Specific)

- [ ] **CRD label propagation** — when a CRD is opted in, do existing CR instances
      get watched from day one, or only new changes going forward? Suggest: only
      new changes; ignore CRs with `generation = 1` older than 1 hour on startup.
- [ ] **`observe-namespace` templating** — the CRD annotation `{{.metadata.namespace}}`
      must be resolved at event time by the watcher. Needs a clear contract between
      watcher and MonitoringPlanLoader.
- [ ] **Thanos access from hub** — confirm cluster-proxy route and RBAC for
      per-cluster metric queries. Verify `namespace` label cardinality is usable
      for filtering without excessive time-series fan-out.
- [ ] **CronJob historical baseline** — 14-day history assumes the CronJob is not
      new. Fallback for first 7 runs: use time-window baseline and flag low confidence.
- [ ] **Threshold tuning** — starting values (z-score 3.0, slope 0.3, job z-score 2.5)
      need calibration against real workload data to avoid false positives on
      naturally noisy or batch-heavy namespaces.
- [ ] **Slack notification format** — propose: change identity (kind/name/namespace),
      checkers fired with values, LLM summary, link to session detail.
- [ ] **Session cleanup** — suggest 7-day retention for audit, then archive or delete.
- [ ] **Multi-namespace leaks** — the CRD annotation specifies one observe namespace.
      If the operator leaks into multiple namespaces, support a comma-separated list?
