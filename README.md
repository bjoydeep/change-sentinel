# change-sentinel

Post-deploy change health advisor for Kubernetes. Watches for anomalies after
a CR spec change and surfaces a health signal any pipeline can query.

Full design: [design.md](design.md) | Starter scope: [starter.md](starter.md)

---

## What this demo shows

1. A buggy operator (`demo-operator/`) leaks Kubernetes Secrets on every reconcile
2. change-sentinel (`sentinel/`) detects a CR spec change, captures a metric baseline,
   watches `kube_secret_count` accumulate, and surfaces an ANOMALOUS health signal
3. Any pipeline (change-first, Tekton, CI) can query the health endpoint before
   making further changes

---

## Prerequisites

| Requirement | Notes |
|---|---|
| `podman` | For building and pushing container images |
| OCP cluster | kubeconfig configured, `oc` and `kubectl` available |
| Thanos Querier | Already running in OCP (`openshift-monitoring/thanos-querier`) — no install needed |
| OpenAI API key | For the LLM correlation step |
| Slack webhook URL | Optional — notifications print to pod logs if not set |

---

## 1. Build and Deploy: change-sentinel

### Build and push

```bash
make build-push
```

Builds `quay.io/bjoydeep/change-sentinel:latest` and pushes it.

### Configure

`deploy/configmap.yaml` defaults are already correct for OCP — Thanos Querier
endpoint and in-cluster token path are pre-set. The only field to review:

```yaml
watch_label: hub   # routing key used in the health endpoint (?clusters=hub)
```

### Create the secret

```bash
make secret          # copies deploy/secret.template → deploy/secret.yaml
# edit deploy/secret.yaml — fill in OPENAI_API_KEY (SLACK_WEBHOOK_URL is optional)
kubectl apply -f deploy/secret.yaml
```

### Deploy

```bash
make deploy
```

Applies namespace → RBAC → ConfigMap → Deployment → Service and waits for rollout.

### Verify

```bash
make logs
```

Expected output:

```
INFO  main     change-sentinel starting (cluster=hub)
INFO  main     Database initialised
INFO  watcher  Started watcher for example.com/widgets in namespace demo-operator-ns
INFO  scanner  Scanner started (interval=300s)
INFO  main     Health signal endpoint: http://0.0.0.0:8000/api/v1/health
```

---

## 2. Build and Deploy: widget-operator (demo)

The buggy operator that leaks secrets — runs fully in the cluster, no local process needed.

### Build and push

```bash
make build-push-operator
```

Builds `quay.io/bjoydeep/widget-operator:latest` and pushes it.

### Deploy

```bash
make deploy-operator
```

Applies Widget CRD → RBAC → Deployment into `demo-operator-ns` and waits for rollout.

### Verify

```bash
kubectl logs -n demo-operator-ns -l app=widget-operator --tail=5
```

Expected output (no errors, kopf started cleanly):

```
kopf.activities.star  INFO   Activity 'configure' succeeded.
kopf._core.engines.a  INFO   Initial authentication has finished.
```

---

## 3. Run the Demo

Both pods must be running before starting. Confirm:

```bash
kubectl get pods -n change-sentinel -n demo-operator-ns
```

### Step 1 — Create the Widget CR (starts the leak)

```bash
kubectl apply -f demo-operator/widget-sample.yaml
```

The operator's 30-second reconcile timer starts immediately. A new Secret is
created on every tick. change-sentinel does **not** fire yet — it is waiting
for a spec change to anchor its baseline window.

Confirm the leak is happening:

```bash
watch kubectl get secrets -n demo-operator-ns --no-headers | wc -l
```

The count should climb by ~2 per minute. If it stays flat, the operator is not running —
check `kubectl logs -n demo-operator-ns -l app=widget-operator`.

### Step 2 — Watch the secrets accumulate

Let the count climb for a few minutes before triggering the sentinel. This gives
the Thanos baseline something to measure against (a flat pre-change baseline that
will contrast with the post-change slope).

```bash
# Optional: see the secrets being created in real time
kubectl get secrets -n demo-operator-ns -w
```

### Step 3 — Patch the Widget spec (triggers the WatchSession)

```bash
kubectl patch widget my-widget -n demo-operator-ns \
  --type=merge -p '{"spec":{"value":"hello-world-v2"}}'
```

This bumps `metadata.generation` (spec changed). change-sentinel detects it immediately.

---

## 4. What to Watch in the Sentinel Logs

```bash
make logs
```

Since Slack is not configured, all notifications print to the pod logs.
Here is the full sequence of what you will see:

**Immediately after the patch (watcher detects generation bump):**
```
INFO  watcher  ChangeEvent: demo-operator-ns/my-widget generation 1→2
```

**Within 5 minutes (scanner picks up the event and creates a WatchSession):**
```
INFO  scanner  WatchSession abc12345 created: Widget/my-widget window=60min plan=OperatorCR
INFO  session  Session abc12345 starting: Widget/my-widget window=HH:MM→HH:MM
INFO  session  Baseline kube_secret_count: mean=12.000 stddev=0.400 slope=0.0000/min
INFO  session  Baseline kube_configmap_count: mean=45.000 stddev=0.200 slope=0.0000/min
```

**Every 2 minutes (MetricPoller running, checker fires once slope exceeds threshold):**
```
WARNING  session  Session abc12345: 1 checker(s) fired — ['kube_secret_count']
```

**After the 60-minute window closes (CorrelationAgent calls the LLM):**
```
INFO  session  Session abc12345 completed: ANOMALOUS (confidence 0.91)
```

**The notification prints to stdout in the pod logs:**
```
======================================================================
  change-sentinel: ANOMALOUS
  Widget/my-widget (demo-operator-ns)
======================================================================
  Change at:   <timestamp of the patch>
  Severity:    warning
  Confidence:  91%

  Checkers fired:
    • ResourceAccumulationChecker: kube_secret_count growing at 2.01/min
      (baseline slope 0.000/min, threshold 0.3/min)

  Assessment:
  Secret count in demo-operator-ns growing at 2.0/min since Widget CR
  changed. Baseline was flat. API server POST rate to secrets 8x baseline.
  Consistent with a reconcile loop creating Secrets without an idempotency check.

  Suggested action:
  Roll back the Widget CR spec or patch the operator image. Clean up
  accumulated secrets before proceeding with further changes.
======================================================================
```

**Faster dev loop** — to see results in ~10 minutes instead of ~65, reduce the
window in `deploy/configmap.yaml` before deploying:

```yaml
scanner_interval_seconds: 30

monitoring_plans:
  OperatorCR:
    window_minutes: 5
    poll_interval_seconds: 30
```

Then re-apply: `kubectl apply -f deploy/configmap.yaml && kubectl rollout restart deployment/change-sentinel -n change-sentinel`

---

## 5. Query the Health Signal

From inside the cluster:

```bash
curl "http://change-sentinel.change-sentinel.svc.cluster.local:8000/api/v1/health?clusters=hub"
```

Or port-forward locally:

```bash
make health
```

**While the session is WATCHING** (monitoring in progress):

```json
{
  "health": "DEGRADED",
  "clusters": ["hub"],
  "sessions": [
    {
      "session_id": "abc12345-...",
      "status": "WATCHING",
      "resource_kind": "Widget",
      "resource_name": "my-widget",
      "namespace": "demo-operator-ns",
      "change_description": "Widget/my-widget changed at <timestamp>",
      "window_closes_at": "<timestamp>"
    }
  ],
  "message": "1 session(s) in progress — monitoring outcome unknown. Consider waiting before making further changes."
}
```

**After the session completes as ANOMALOUS:**

```json
{
  "health": "ANOMALOUS",
  "clusters": ["hub"],
  "sessions": [],
  "message": "1 confirmed anomaly(ies) on requested clusters. Review before proceeding."
}
```

Full session detail including verdict, anomalies, and baseline snapshot:

```bash
curl -s http://localhost:8000/api/v1/sessions/abc12345-... | python3 -m json.tool
```

---

## 6. Clean Up

```bash
# Remove accumulated secrets from the demo
kubectl delete secrets -n demo-operator-ns \
  $(kubectl get secrets -n demo-operator-ns -o name | grep widget-)

# Remove the Widget CR
kubectl delete widget my-widget -n demo-operator-ns

# Remove the operator and change-sentinel from the cluster
make undeploy-operator
make undeploy
```

---

## 7. Adding a New CRD to Watch

1. Add an entry to `deploy/configmap.yaml` under `watchers.cr_instances`
2. Add a rule to `deploy/rbac.yaml` under `rules` for the new API group
3. Apply and restart:

```bash
kubectl apply -f deploy/configmap.yaml
kubectl apply -f deploy/rbac.yaml
kubectl rollout restart deployment/change-sentinel -n change-sentinel
```

---

## Log Level

The default log level is `INFO`. At INFO you will see:

- Watcher: `ChangeEvent detected`
- Scanner: `WatchSession created`
- Session: `Baseline captured`, `checker(s) fired`, `completed: ANOMALOUS`
- Correlator: verdict fields, **LLM reasoning**, **recommended action**
- Notifier: full notification block

Set `DEBUG` to also log the full prompt sent to the LLM (baseline stats + all
checker results) — useful for understanding why the LLM reached its verdict.

**In the cluster** — edit `deploy/deployment.yaml`:

```yaml
env:
  - name: LOG_LEVEL
    value: DEBUG     # change from INFO
```

Then apply and restart:

```bash
kubectl apply -f deploy/deployment.yaml
kubectl rollout restart deployment/change-sentinel -n change-sentinel
```

**Locally** — set the env var before running:

```bash
export LOG_LEVEL=DEBUG
make run-local
```

---

## Makefile reference

| Target | What it does |
|---|---|
| `make build` | Build sentinel image with podman |
| `make push` | Push sentinel image to registry |
| `make build-push` | Build and push sentinel in one step |
| `make build-operator` | Build widget-operator image with podman |
| `make push-operator` | Push widget-operator image to registry |
| `make build-push-operator` | Build and push widget-operator in one step |
| `make deploy` | Deploy change-sentinel to cluster |
| `make deploy-operator` | Deploy widget-operator to cluster |
| `make undeploy` | Remove change-sentinel from cluster |
| `make undeploy-operator` | Remove widget-operator from cluster |
| `make logs` | Tail change-sentinel pod logs |
| `make health` | Port-forward and query `/api/v1/health` |
| `make secret` | Copy `secret.template` → `secret.yaml` |
| `make venv` | Create `.venv`, install all deps (local dev) |
| `make run-local` | Run sentinel locally from `.venv` |
| `make run-operator` | Run widget-operator locally from `.venv` |
| `make run-container` | Run sentinel image locally with `.env` + kubeconfig |

---

## Local Development

For iterating on sentinel code without a full cluster deploy.

### Setup

```bash
make venv
source .venv/bin/activate
```

Port-forward Thanos Querier and set your bearer token:

```bash
kubectl port-forward -n openshift-monitoring svc/thanos-querier 9091:9091 &
export METRICS_TOKEN=$(oc whoami -t)
export OPENAI_API_KEY=sk-...
```

### Run

```bash
# Terminal 1 — sentinel (reads sentinel/config.yaml)
make run-local

# Terminal 2 — operator (optional: or use the in-cluster operator)
make run-operator
```

### Faster dev loop

```yaml
# sentinel/config.yaml
scanner_interval_seconds: 30
monitoring_plans:
  OperatorCR:
    window_minutes: 5
    poll_interval_seconds: 30
```

---

## File Layout

```
change-sentinel/
├── Makefile
├── .gitignore
├── demo-operator/
│   ├── Dockerfile               container image for widget-operator
│   ├── operator.py              the buggy operator (kopf)
│   ├── widget-crd.yaml          CRD with change-sentinel opt-in labels
│   ├── widget-sample.yaml       sample Widget CR
│   ├── requirements.txt
│   └── deploy/
│       ├── rbac.yaml            ServiceAccount, ClusterRole, ClusterRoleBinding
│       └── deployment.yaml      widget-operator Deployment
├── sentinel/
│   ├── Dockerfile               container image for change-sentinel
│   ├── main.py                  entry point
│   ├── config.yaml              local dev configuration
│   ├── config.py                typed config loader
│   ├── db.py                    SQLite schema and queries
│   ├── watcher.py               CRInstanceWatcher (Kubernetes watch stream)
│   ├── scanner.py               5-min loop, creates WatchSessions
│   ├── session.py               WatchSession lifecycle orchestrator
│   ├── baseline.py              Thanos range queries and statistics
│   ├── checkers.py              ResourceAccumulationChecker and others
│   ├── correlator.py            LLM AnomalyVerdict (OpenAI structured output)
│   ├── notifier.py              Slack / stdout notification
│   ├── api.py                   FastAPI health signal endpoint
│   └── requirements.txt
└── deploy/
    ├── namespace.yaml            change-sentinel namespace
    ├── rbac.yaml                 ServiceAccount, ClusterRole, ClusterRoleBinding
    ├── configmap.yaml            in-cluster config.yaml
    ├── secret.template           copy → secret.yaml, fill in, apply (gitignored)
    ├── pvc.yaml                  1Gi PVC for SQLite (requires working EBS CSI)
    ├── deployment.yaml           single-replica Deployment
    └── service.yaml              ClusterIP Service for the health endpoint
```
