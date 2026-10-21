"""
Widget Operator — Demo operator for change-sentinel.

Watches Widget CRs and syncs their spec.value into a Kubernetes Secret.

THE BUG (intentional): On every reconcile, a new Secret is created with a
timestamp suffix instead of checking whether one already exists. A real operator
would use a create-or-update (server-side apply) pattern. This one leaks ~2
Secrets per minute per Widget CR while it is running.

Run locally (requires kubeconfig):
    pip install -r requirements.txt
    kubectl apply -f widget-crd.yaml
    kubectl create namespace demo-operator-ns
    kopf run operator.py --namespace demo-operator-ns

Then create a Widget to start the leak:
    kubectl apply -f widget-sample.yaml

Trigger a change-sentinel WatchSession by updating the spec:
    kubectl patch widget my-widget -n demo-operator-ns \
        --type=merge -p '{"spec":{"value":"hello-world-v2"}}'
"""

import time
import logging

import kopf
import kubernetes
import kubernetes.client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)


# ── Startup ───────────────────────────────────────────────────────────────────

@kopf.on.startup()
def configure(settings: kopf.OperatorSettings, **_):
    settings.persistence.progress_storage = kopf.AnnotationsProgressStorage()
    settings.posting.level = logging.WARNING


# ── Create / Update handler ───────────────────────────────────────────────────

@kopf.on.create("widgets.example.com")
@kopf.on.update("widgets.example.com")
def on_widget_change(spec, name, namespace, logger, **kwargs):
    """
    Fires when a Widget CR is created or its spec changes.
    This is the event that change-sentinel's CRInstanceWatcher detects
    (generation bump). The actual leak happens in reconcile() below.
    """
    logger.info(
        "Widget %s/%s changed (value=%r) — reconcile timer will sync",
        namespace, name, spec.get("value"),
    )


# ── Reconcile timer ───────────────────────────────────────────────────────────

@kopf.timer("widgets.example.com", interval=30.0, idle=5.0)
def reconcile(spec, name, namespace, logger, **kwargs):
    """
    Simulates a controller reconcile loop with RequeueAfter=30s.

    CORRECT behaviour (what this should do):
        secret_name = f"widget-{name}"
        try:
            existing = v1.read_namespaced_secret(secret_name, namespace)
            # Secret exists — patch it if value changed
            existing.string_data = {"value": spec.get("value", "")}
            v1.patch_namespaced_secret(secret_name, namespace, existing)
            logger.info("Patched existing secret %s", secret_name)
        except kubernetes.client.exceptions.ApiException as e:
            if e.status == 404:
                _create_secret(v1, name, namespace, spec)
            else:
                raise

    BUG: skips the existence check entirely. Appends a millisecond timestamp
    to the name so every reconcile creates a fresh Secret, leaking ~2/min.
    """
    v1 = kubernetes.client.CoreV1Api()
    _create_secret_buggy(v1, name, namespace, spec, logger)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _create_secret_buggy(v1, widget_name, namespace, spec, logger):
    """Always creates — the leak."""
    secret_name = f"widget-{widget_name}-{int(time.time() * 1000)}"
    secret = kubernetes.client.V1Secret(
        metadata=kubernetes.client.V1ObjectMeta(
            name=secret_name,
            namespace=namespace,
            labels={
                "managed-by": "widget-operator",
                "widget": widget_name,
            },
        ),
        string_data={
            "value": str(spec.get("value", "")),
            "widget": widget_name,
        },
    )
    try:
        v1.create_namespaced_secret(namespace=namespace, body=secret)
        logger.info("BUG: created secret %s (should have been idempotent)", secret_name)
    except kubernetes.client.exceptions.ApiException as e:
        if e.status == 409:
            pass  # 409 Conflict = already exists; can't happen with timestamp suffix
        else:
            raise
