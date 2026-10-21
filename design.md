# change-sentinel: Change-Aware Anomaly Detection

> Working doc. Part of the Change-Aware Fleet Management platform.
> Companion to change-first (pre-merge vetting).

---

## What This System Does

A post-deploy autonomous correlation agent that watches for anomalies caused by
platform configuration changes on ACM-managed clusters — and surfaces a clear
health signal explaining what looks wrong and why.

**The sequence:**
1. A change lands on the fleet via ACM (Policy enforcement, GitOps sync, etc.)
2. change-sentinel detects and stores the raw event
3. A canonicalizer deduplicates events across sources into one logical change
4. Every 5 minutes, a scanner groups canonical changes into WatchSessions
5. Each WatchSession infers blast radius, captures a pre-change baseline, and
   monitors affected clusters for anomalies over a defined window
6. At the end of the window: a health signal is produced — CLEAR or ANOMALOUS —
   with a human-readable explanation of what was observed and why
7. Any pipeline or team can query the health signal and decide how to respond

**The goal:** Eliminate expensive post-change war-room triage. Surface the
"this metric spike started exactly when that policy landed" correlation
automatically, with a plain-language explanation of what was observed and why.
Once the signal has earned trust, teams can wire it into their deployment
pipelines as a precheck — on their terms, at their pace.

---

## Relationship to change-first

```
Change-Aware Fleet Management (platform)
├── change-first    — pre-merge vetting     (stop bad changes before they land)
└── change-sentinel — post-deploy watch     (surface health signal after each change)
                           ↑
                    change-first (or any pipeline) can query
                    the health signal and act on it
```

Phase 1 (pre-merge vetting): `~/code/change-first` — design at `~/code/change-first/design.md`

Together they form a **change health signal loop**:

```
Change lands
    → change-sentinel watches → CLEAR      → fleet looks healthy
                              → DEGRADED   → something looks off, watch it
                              → ANOMALOUS  → anomaly detected, here's why
                                    ↓
                            any pipeline or team queries the signal
                            and decides how to respond
```

Both systems share the same core concepts:
- Blast radius from ACM Placement resolution
- LLM knowledge about what platform changes DO
- ACM hub as the control plane (cluster-proxy, k8s API, Search)

They are separate repos because their lifecycles differ fundamentally:
- change-first: short-lived, request-response, triggered by GitHub webhook
- change-sentinel: long-running, stateful, event-driven, continuous

---

## Core Architectural Principle

> **Be deterministic where possible. Use LLM knowledge where encoding the
> same knowledge in code would mean constant maintenance or thousands of
> if/else branches.**

**Deterministic layer** — facts queried from systems or derived by rule:
- Event ingestion and storage
- Cross-source deduplication into canonical changes
- Storm detection and event grouping
- Blast radius (Placement → ManagedClusters, same as change-first)
- Metric analysis (querying pre-collected metrics from Thanos/Prometheus)
- Baseline capture anchored to event timestamp
- Anomaly detection — pluggable algorithms (see Anomaly Detection section)
- Event sequence conformance checking (expected vs. observed event ordering)

**LLM layer** — knowledge that would rot if encoded as rules:
- "Given this change type, what metrics should I watch and for how long?"
  The model already knows MachineConfig → node restarts → pod disruption metrics,
  cluster-monitoring-config → Prometheus restart → metrics gap, etc.
- "Given this change type, what event sequence should I expect and in what order?"
- "Is this metric deviation or sequence violation consistent with this change type?"
- "Is this anomaly worth alerting on, or within normal post-change variance?"
- Generating human-readable incident summaries

If a new resource type appears in ACM, the LLM handles it without code changes.

---

## The Central Spine

```
Event Sources
  ├── Built-in watchers (always on)
  │     ├── KubeDeploymentWatcher     — watches Deployment rollouts on managed clusters
  │     ├── KubePodEventWatcher       — watches pod lifecycle events
  │     └── ACMPolicyWatcher          — watches Policy compliance transitions
  └── Pluggable ChangeSource interface
        ├── ACMManifestWorkSource
        ├── ArgoCDSyncSource
        └── KubeEventSource (hub-side)
    │
    │  emits raw events
    ▼
change_events (DB: append-only event ledger)
    │
    ▼
Canonicalizer (runs continuously)
    │  deduplicates cross-source events → one canonical_change per logical change
    │  e.g., ArgoCD sync + Deployment rollout + kube event = one canonical change
    ▼
canonical_changes (DB)
    │
    ▼
Scanner (runs every 5 min)
    │  groups canonical_changes → WatchSessions
    │  detects storms → STORM-type WatchSessions
    ▼
WatchSession (DB: watch_sessions)
    │  one per logical change group, owns full lifecycle
    │  PENDING → BASELINING → WATCHING → CORRELATING → CLEAR | ANOMALOUS
    │
    ├── BlastRadiusResolver     (deterministic: Placement → ManagedClusters)
    ├── MonitoringPlanAgent     (LLM → structured MonitoringPlan)
    │                            produces: metrics to watch + expected event sequence
    ├── BaselineCapture         (Thanos/metric backend: T-1h to T-5min)
    ├── MetricPoller            (queries MetricBackend at interval)
    ├── MetricAnomalyDetector   (pluggable algorithms: Z-score, CUSUM, etc.)
    ├── EventSequenceAnalyzer   (absence detection, out-of-order, unexpected events)
    └── CorrelationAgent        (LLM → AnomalyVerdict)
            │
            ▼
    cluster_health (DB: queryable by any pipeline)
            │
            ▼
    NotifierSink (pluggable: Slack, PagerDuty, GitHub comment)
```

---

## Data Model

Three tiers, each with a clear role:

```
change_events       raw, append-only, all sources, may contain cross-source dupes
        ↓  canonicalize
canonical_changes   one per logical change, cross-source dupes resolved
        ↓  group
watch_sessions      one per canonical change (or logical group of related changes)
```

### `change_events` — Event Ledger (append-only)

```sql
change_events (
  id                UUID PRIMARY KEY,
  source            TEXT,        -- 'manifestwork', 'acm_policy', 'argocd', 'kube_event'
  resource_kind     TEXT,        -- 'MachineConfig', 'Policy', 'Application', etc.
  resource_name     TEXT,
  namespace         TEXT,
  cluster           TEXT,        -- managed cluster name
  hub_cluster       TEXT,
  event_timestamp   TIMESTAMPTZ, -- WHEN THE CHANGE HAPPENED (anchor for all queries)
  ingested_at       TIMESTAMPTZ,
  raw_payload       JSONB,
  canonical_id      UUID         -- NULL until canonicalized
)
```

`event_timestamp` is the critical field. All baseline windows and monitoring
windows are anchored to this timestamp, not to when the scanner ran.

### `canonical_changes` — Deduplicated Logical Changes

```sql
canonical_changes (
  id                UUID PRIMARY KEY,
  source_event_ids  UUID[],      -- all raw events that map to this logical change
  resource_kind     TEXT,
  resource_name     TEXT,
  clusters          TEXT[],      -- union of clusters from source events
  event_timestamp   TIMESTAMPTZ, -- earliest event_timestamp across source events
  ingested_at       TIMESTAMPTZ,
  watch_session_id  UUID         -- NULL until assigned by scanner
)
```

The dedup key is `(resource_kind, resource_name, cluster, ~5-min window)` normalized
across sources. An ArgoCD sync event and the resulting Deployment rollout referencing
the same resource on the same cluster within a 2-minute window → one canonical change.

### `watch_sessions` — One Per Logical Change Group

```sql
watch_sessions (
  id                    UUID PRIMARY KEY,
  type                  TEXT,        -- 'CHANGE' | 'STORM'
  trigger_canonical_ids UUID[],      -- canonical changes that caused this session
  change_summary        TEXT,        -- LLM-generated description
  clusters_affected     TEXT[],      -- resolved blast radius
  status                TEXT,        -- PENDING|BASELINING|WATCHING|CORRELATING|CLEAR|ANOMALOUS
  monitoring_plan       JSONB,       -- LLM output (metrics + event sequence, see schema below)
  baseline_snapshot     JSONB,       -- metric backend query results
  metric_anomalies      JSONB,       -- deviations detected by MetricAnomalyDetector
  sequence_anomalies    JSONB,       -- violations detected by EventSequenceAnalyzer
  verdict               JSONB,       -- LLM correlation output (see schema below)
  confidence            FLOAT,       -- reduced when concurrent sessions overlap
  window_opens_at       TIMESTAMPTZ, -- = min(event_timestamp) of trigger canonical changes
  window_closes_at      TIMESTAMPTZ, -- = window_opens_at + monitoring_plan.window_minutes
  completed_at          TIMESTAMPTZ,
  notification_sent     BOOL DEFAULT FALSE
)
```

### `cluster_health` — Queryable by Any Pipeline

```sql
cluster_health (
  cluster_id            TEXT PRIMARY KEY,
  last_watch_session_id UUID,
  health                TEXT,        -- CLEAR|DEGRADED|ANOMALOUS|UNKNOWN
  updated_at            TIMESTAMPTZ,
  change_description    TEXT
)
```

This is the table any pipeline queries: *"What is the current health of the
clusters this change will touch?"*

---

## Canonicalization: Cross-Source Dedup

The same logical change arrives through multiple sources simultaneously:

```
ArgoCD sync  → "Application/fleet-config synced"
                    ↓ triggers
Kubernetes   → "Deployment/prometheus-operator updated"
                    ↓ also from
ManifestWork → "ManifestWork/monitoring-config applied"
```

All three are representations of one change. The Canonicalizer resolves them:

1. Compute dedup key: `hash(resource_kind + resource_name + cluster)` within a
   rolling 5-minute window
2. Group events with the same dedup key into one `canonical_change`
3. Use the earliest `event_timestamp` as the canonical timestamp
4. Link all source event IDs in `canonical_changes.source_event_ids`

This runs continuously (not on the 5-min scan schedule) so that by the time the
scanner runs, canonical changes are already resolved and ready to group.

---

## The Scanner: 5-Minute Window Close

The scanner is not a trigger — it is a **window close** operation. Every 5 minutes:

1. Pull all `canonical_changes` where `watch_session_id IS NULL`
2. Check for storm conditions first
3. Group remaining canonical changes into WatchSessions
4. Update `canonical_changes.watch_session_id` for assigned changes

### Storm Detection and Analysis

Storms are not dropped — a storm is itself a signal worth analyzing.

| Condition | Action |
|---|---|
| > 30 canonical changes in one 5-min window across the fleet | Create a STORM-type WatchSession for meta-analysis |
| > 5 canonical changes on the same cluster in one window | Collapse into one session, flagged high-noise, reduced confidence |
| Same resource updated > 3 times in one window | Dedup via canonicalization to latest only |

A **STORM-type WatchSession** runs the same pipeline as a CHANGE session but with
a different MonitoringPlan prompt: *"A large volume of changes landed simultaneously.
What does this pattern suggest — runaway controller, sync loop, cascading failure?
What should I watch for?"*

### Change Grouping Rules (deterministic)

Two canonical changes belong in the same WatchSession if:
- Same Placement-resolved cluster set AND same scan window AND related resource kinds
  (e.g., deployed together by the same ArgoCD Application)

When in doubt, one canonical change = one WatchSession. Grouping is an optimization
for related co-deployed changes, not a requirement.

---

## The Timing Problem (and Why It's Not a Problem)

A change at T=0 may not be processed until the scanner runs at T+5. This looks
like a 5-minute lag in detection. But the lag is irrelevant because all windows
are anchored to `event_timestamp` from the raw event, not to scan time:

```
Event lands at       T=0   → stored with event_timestamp = T
Canonicalized at     T+1   → canonical_change.event_timestamp = T
Scanner runs at      T+5   → creates WatchSession
Baseline query:      Thanos[ T-60min : T-5min ]   ← anchored to event_timestamp
Monitoring window:   T → T+30min                  ← also anchored to event_timestamp
```

Even though sentinel didn't start watching until T+5, its baseline and correlation
window are anchored to the actual change time. A metric spike at T+2 is fully
captured. A metric spike that predates T by 10 minutes shows up as pre-existing
in the baseline — not falsely attributed to the change.

The only unsolved case: an event you never detected at all. That is a detection
coverage problem, not a timing problem.

---

## Anomaly Detection

Anomaly detection runs on two independent tracks, both feeding the CorrelationAgent.

### Track 1: MetricAnomalyDetector

Pluggable algorithm framework — the pipeline selects an algorithm based on the
signal characteristics in the MonitoringPlan:

| Algorithm | Good for |
|---|---|
| Z-score against rolling baseline | Fast, simple spike detection |
| CUSUM (cumulative sum) | Sustained shifts that don't spike — a slow drift post-change |
| Seasonal decomposition | Metrics with diurnal patterns (high at 9am, low at night) |
| Isolation Forest | Multi-metric correlation — unusual combination of individually normal values |

For v1, Z-score + CUSUM covers most cases. The pluggable interface allows upgrading
without touching the pipeline.

### Track 2: EventSequenceAnalyzer

Not all anomalies are metric anomalies. A MachineConfig rollout creates an expected
event sequence with timing bounds:

```
Expected sequence for MachineConfig rollout:
  T+0m:  MachineConfig updated                         (trigger)
  T+5m:  Node drain started         ← expected event, deadline T+10m
  T+12m: Node drain complete        ← expected event, deadline T+20m
  T+15m: Node reboot                ← expected event, deadline T+25m
  T+20m: Node ready                 ← expected event, deadline T+35m
```

The EventSequenceAnalyzer detects three classes of violations:

- **Absence**: "Node ready" never arrived → node stuck in drain/reboot
- **Out-of-order**: Events arrive in wrong sequence → something wrong mid-rollout
- **Unexpected**: OOMKill or eviction warning appears mid-sequence → unintended side effect
- **Deadline breach**: Expected event arrived but too late → slowdown, not failure

The expected sequence and deadlines come from the MonitoringPlan (LLM-generated).
The conformance checking is deterministic.

---

## Concurrent Changes and Confidence Degradation

If two changes land simultaneously on overlapping clusters, attribution is
ambiguous. Rather than failing, the system degrades confidence:

```
WatchSession A: clusters [C1, C2, C3]
WatchSession B: clusters [C2, C3, C4]  (concurrent, started within 10 min)

Overlap: [C2, C3]
Confidence multiplier on overlapping clusters: 0.5x

AnomalyVerdict.confidence for anomalies on C2/C3 is halved.
Alert threshold raised: only notify if confidence > 0.7 after degradation.
```

Both sessions complete independently. Each references the other's ID in its verdict
so the notification message can say: *"Attribution uncertain — concurrent change Y
also affected these clusters."*

---

## Pluggable Interfaces

The core pipeline is generic. Only adapters know about ACM/OCP specifics.

```python
class ChangeSource(Protocol):
    def stream_events(self) -> Iterator[ChangeEvent]: ...

class MetricBackend(Protocol):
    def query_range(self, metric, labels, start, end) -> MetricSeries: ...
    def query_instant(self, metric, labels, at) -> float: ...

class AnomalyAlgorithm(Protocol):
    def detect(self, baseline: MetricSeries, observed: MetricSeries) -> list[Deviation]: ...

class NotifierSink(Protocol):
    def send(self, notification: Notification) -> None: ...
```

Built-in implementations (no config required):
- `KubeDeploymentWatcher`, `KubePodEventWatcher`, `ACMPolicyWatcher`
- `ThanosBackend`, `PrometheusBackend`
- `ZScoreAlgorithm`, `CUSUMAlgorithm`
- `SlackSink`, `PagerDutySink`, `GitHubCommentSink`

The system is testable with `FakeChangeSource`, `FakeMetricBackend`, and
`FakeNotifierSink` without a real cluster.

---

## LLM Contracts (Structured Output)

LLM calls use structured output — the pipeline never parses free text.

### MonitoringPlan (what to watch)

Input: change type, resource kind, resource name, affected cluster count

```json
{
  "metrics": [
    "kube_pod_container_status_restarts_total",
    "node_ready",
    "apiserver_request_duration_seconds_bucket"
  ],
  "label_filters": {
    "kube_pod_container_status_restarts_total": {"namespace": "openshift-*"}
  },
  "window_minutes": 30,
  "extended_window_minutes": 120,
  "poll_interval_seconds": 60,
  "thresholds": {
    "kube_pod_container_status_restarts_total": {"delta_over_baseline": 5}
  },
  "expected_event_sequence": [
    {"event": "node_drain_started",   "deadline_minutes": 10,  "absence_is_anomaly": true},
    {"event": "node_drain_complete",  "deadline_minutes": 20,  "absence_is_anomaly": true},
    {"event": "node_reboot",          "deadline_minutes": 25,  "absence_is_anomaly": false},
    {"event": "node_ready",           "deadline_minutes": 35,  "absence_is_anomaly": true}
  ],
  "unexpected_events": ["oom_kill", "eviction_warning"],
  "anomaly_algorithm_hint": "cusum",
  "reasoning": "MachineConfig triggers node drain and reboot cycle..."
}
```

### AnomalyVerdict (correlation result)

Input: change type, resource, baseline snapshot, metric deviations, sequence violations,
concurrent sessions

```json
{
  "is_anomaly": true,
  "caused_by_change": true,
  "confidence": 0.85,
  "severity": "warning",
  "expected_transient": false,
  "anomaly_sources": ["metric", "sequence"],
  "concurrent_session_ids": ["uuid-of-concurrent-session"],
  "reasoning": "Pod restarts on node-* align precisely with MachineConfig rollout window. 3x baseline rate. Node drain deadline breached on cluster C2.",
  "recommended_action": "Investigate node restart logs on C2 before proceeding with next change."
}
```

---

## WatchSession Lifecycle

```
PENDING       → session created by scanner, canonical changes assigned
BASELINING    → querying metric backend for pre-change baseline
WATCHING      → MonitoringPlanAgent complete, MetricPoller + EventSequenceAnalyzer running
CORRELATING   → monitoring window closed, CorrelationAgent running
CLEAR         → no anomalies, or anomalies attributed elsewhere with high confidence
ANOMALOUS     → anomaly confirmed, notification sent, health signal updated
```

From ANOMALOUS, the session stays visible until the next CLEAR session on the
same clusters, or a human explicitly acknowledges it.

---

## Health Signal Criteria

A WatchSession reaches CLEAR when:
- Monitoring window has elapsed, AND one of:
  - No metric crossed threshold AND no sequence violations detected, OR
  - Anomalies detected but `AnomalyVerdict.caused_by_change = false` with confidence > 0.8, OR
  - Anomalies detected but `AnomalyVerdict.expected_transient = true` and
    metrics returned to baseline before window close

CLEAR is a **positive assertion** — not just absence of alerts.

---

## Health Signal Endpoint

change-sentinel exposes a read-only health signal endpoint. Any pipeline
— change-first, Tekton, ArgoCD, a human dashboard — can query it and decide
how to respond. change-sentinel does not control pipelines; it describes what
it observed.

```
GET /api/v1/health?clusters=C1,C2,C3

Response:
{
  "health": "DEGRADED",
  "sessions": [
    {
      "session_id": "uuid",
      "status": "WATCHING",
      "change_description": "MachineConfig/worker-config enforced 8 min ago — monitoring in progress",
      "clusters": ["C1", "C2"],
      "window_closes_at": "2026-10-20T14:32:00Z"
    }
  ]
}
```

Health values:
- `CLEAR` — no active sessions, or all sessions completed without anomalies
- `DEGRADED` — sessions are WATCHING (monitoring in progress, outcome unknown)
- `ANOMALOUS` — one or more sessions ended with a confirmed anomaly

What the consumer does with this signal is entirely up to them. A team that
has built confidence in the signal may wire it into their pipeline as a hard
precheck. A team that is still evaluating may use it as a Slack warning only.
change-sentinel does not enforce either policy.

---

## Monitoring Windows

LLM determines windows via MonitoringPlan. Reference defaults passed as prompt context:

| Change type | Initial window | Extended watch |
|---|---|---|
| MachineConfig | 30 min (node reboot time) | 2 hr |
| cluster-monitoring-config | 15 min (Prometheus restart) | 1 hr |
| ImageDigestMirrorSet | 30 min (image pull cycle) | 2 hr |
| ACM Policy (generic) | 10 min | 30 min |
| Placement scope change | 30 min | 1 hr |

These are inputs to the LLM prompt as guidance, not hardcoded branches.
The LLM can override based on context.

---

## Anomaly Categories

Two tracks, both feeding the CorrelationAgent:

**Metric anomalies** (MetricAnomalyDetector):
- **Availability**: pod restarts, crashloopbackoffs, node not ready
- **Latency**: API server response time, service latency spikes
- **Resource pressure**: CPU/memory spikes on affected nodes
- **Observability gaps**: Prometheus down, missing scrape targets, metric gaps
- **Image pull failures**: imagepullbackoff rate on affected clusters
- **Capacity**: PVC failures, quota exhaustion triggered by rollout

**Event sequence anomalies** (EventSequenceAnalyzer):
- **Absent events**: expected lifecycle event never arrived (node stuck draining)
- **Out-of-order events**: sequence violated (reboot before drain complete)
- **Unexpected events**: OOMKill, eviction warning mid-rollout
- **Deadline breaches**: event arrived too late (slow drain, degraded node)

---

## Technology Stack

- **Runtime**: Python, running on ACM hub
- **Database**: PostgreSQL (or CockroachDB for HA) — the event ledger and
  watch sessions are the system's persistent memory; SQLite acceptable for dev
- **Observability**: ACM Observability stack (Thanos on hub) for cross-cluster metrics
- **Hub APIs**: cluster-proxy, k8s API, ManifestWork, Policy compliance status
- **LLM**: OpenAI-compatible endpoint (same as change-first), structured output mode
- **Agent SDK**: OpenAI Agents SDK (same as change-first)
- **Scheduler**: APScheduler or Kubernetes CronJob for the 5-min scanner loop
- **Deployment**: OCP on hub as a long-running Deployment (canonicalizer + scanner + pollers)
  with a separate API server (FastAPI) for change-first integration

---

## Detection Sources and Fidelity

| Source | Signal | Fidelity | Use as |
|---|---|---|---|
| ManifestWork status transitions | Policy landed on cluster | High — confirms delivery | Primary |
| ACM Policy compliance status | Policy evaluated and enforced | High — confirms enforcement | Primary |
| ArgoCD Application sync events | GitOps change deployed | High — confirms sync | Primary |
| Kubernetes Deployment watcher | Rollout started/completed | High — confirms rollout | Built-in |
| Kubernetes Pod event watcher | Pod lifecycle events | High — per-pod signal | Built-in |
| Kubernetes events on hub | Resource created/updated | Medium | Supplementary |
| GitHub post-merge webhook | PR merged | Low — change not yet deployed | Not used |

Primary and built-in sources are consumed via Kubernetes watch (`informer` pattern).
The Canonicalizer reconciles across sources to resolve cross-source duplicates before
they reach the scanner.

---

## Integration with change-first

change-first and change-sentinel are two ends of the same safety loop:

```
PR opened
    → change-first vets it       "will this be safe?"        (pre-merge)
    → PR merges, change lands
    → change-sentinel watches it "was it safe?"              (post-deploy)
    → health signal written
    → next PR opened
    → change-first queries it    "is the fleet ready?"       (pre-merge again)
```

Four integration points, in order of priority:

---

### 1. change-first queries the health signal (implement now)

Before running its own pre-merge vetting, change-first calls:

```
GET /api/v1/health?clusters=C1,C2,C3

→ ANOMALOUS
→ change-first adds PR comment:
  "Fleet has an unresolved anomaly on C1, C2 — review before merging.
   Details: Widget/my-widget leaked secrets after last change (14:30 today)."
```

Change-first decides what to do with the signal — change-sentinel does not
control the gate. Teams configure the response per cluster tier:
- Evaluating: advisory comment only
- Trusting: warn and require manual override
- Fully committed: block merge until CLEAR

---

### 2. change-first stamps a PR reference on merged resources (implement soon)

When a PR merges and the resource lands on the cluster, change-first adds
annotations to the resource:

```yaml
metadata:
  annotations:
    change-first.io/pr: "456"
    change-first.io/pr-author: "jbanerje"
    change-first.io/risk-assessment: "medium"
    change-first.io/merged-at: "2026-10-20T14:28:00Z"
```

Change-sentinel reads these from the raw event payload. The WatchSession
and notification become dramatically richer:

> **Anomaly detected after PR #456** (merged by @jbanerje at 14:28,
> risk: medium). Secret count growing +1.0/min since merge.
> change-first had flagged node reboot disruption as expected — this is not
> that. Consistent with a reconcile loop idempotency bug.

Without this, change-sentinel can say "something looks wrong." With it, it
can say "this PR caused this problem" — the difference between an alert
humans investigate and one they act on immediately.

---

### 3. Historical risk feedback to change-first (implement later)

Change-sentinel accumulates outcomes over time: which resource types on which
cluster tiers have caused anomalies, and how often. This history should feed
back into change-first's risk scoring:

```
change-first risk assessment:
  baseline LLM reasoning:       "MachineConfig changes trigger node reboots — medium risk"
  + change-sentinel history:    "MachineConfig on production tier: 4/10 changes caused
                                 anomalies in the last 30 days — escalate to high risk"
  → final assessment:           high risk
```

This makes the pre-merge vetting smarter over time rather than each PR
assessment being independent of what actually happened on the fleet.

---

### 4. Pre-armed WatchSessions from change-first (implement later)

Change-first knows a high-risk PR is about to merge before it lands. It could
prime change-sentinel to start watching immediately on merge — reducing the
5-minute scanner lag to near-zero for changes that matter most:

```
change-first: "PR #456 merging now. MachineConfig change on C1, C2, C3.
               Watch for node restarts specifically."
→ change-sentinel: WatchSession created and BASELINING before the compliance
  transition even fires.
```

Useful for high-risk changes where 5 minutes of missed signal matters.

---

### Shared infrastructure

Both systems resolve blast radius the same way (Placement → ManagedClusters)
and call the same OpenAI-compatible endpoint. Extract to a shared
`change-aware-core` library rather than duplicating. The health signal endpoint
is the runtime contract; the shared library is the build-time contract.

---

## Open Questions

- [ ] **Event grouping edge cases** — what if the same MachineConfig is applied
      to 5 different MachineConfigPools with different timing? One session or five?
- [ ] **ANOMALOUS auto-resolve** — what is the automatic resolve policy for ANOMALOUS
      sessions? Time-based? Metric returns to baseline? Manual acknowledgement only?
- [ ] **Gate enforcement** — is the change-first integration a hard block in the
      webhook, or advisory only? Likely configurable per cluster tier.
- [ ] **Multi-hub** — does each hub run its own sentinel instance? How do
      health signals aggregate if a cluster is managed by multiple hubs?
- [ ] **Shared library with change-first** — blast radius resolver and LLM client
      are identical. Extract to a shared `change-aware-core` package?
- [ ] **Backpressure** — if the fleet is changing rapidly and sessions pile up,
      does the health signal become permanently DEGRADED? Need a session TTL and cleanup policy.
- [ ] **Bootstrap** — on first run, there's no baseline data for any cluster.
      Use Thanos historical data for the first N sessions, or skip baselining?
- [ ] **EventSequenceAnalyzer event source** — which Kubernetes events feed the
      sequence analyzer? Hub-side events only, or do we need cluster-proxy access
      to watch events on managed clusters directly?
- [ ] **Anomaly algorithm selection** — is the `anomaly_algorithm_hint` in
      MonitoringPlan sufficient, or should the system auto-select based on signal
      characteristics (variance, seasonality, sample rate)?
- [ ] **Early termination when confidence is already high** — the fixed monitoring
      window is designed for ambiguous signals but is wasteful for obvious ones.
      A resource leak that produces a 20x slope above baseline is unmistakable
      within 5-10 minutes; waiting 60 minutes adds no information and delays the
      health signal by nearly an hour. Need a progressive confidence model:
      if N consecutive polls all fire with high margin AND baseline was flat,
      call the CorrelationAgent early rather than waiting for window close.
      `MonitoringPlan` already has `extended_window_minutes` for signals that
      need more time — the inverse (`early_termination_confidence_threshold`)
      is missing and should be a first-class concept alongside it.
- [ ] **"No data" vs "no anomaly"** — a checker query that returns no data
      (broken PromQL, missing metric, wrong label cardinality) currently produces
      `fired=False`, identical to a working query that found no anomaly. Silent
      false confidence. Need a `DATA_MISSING` state on `CheckerResult` that
      propagates to the LLM prompt and reduces verdict confidence explicitly.
