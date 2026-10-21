"""
CRInstanceWatcher — detects CR spec changes via generation bumps.

For each configured CRD, runs a Kubernetes watch stream. When a CR's
metadata.generation increments, a ChangeEvent is written to change_events.

Runs in daemon threads — one per CRD watch target.
"""

import logging
import threading
import uuid
from datetime import datetime, timezone

import kubernetes
import kubernetes.client
import kubernetes.watch

import db
from config import Config, CRWatchTarget

log = logging.getLogger(__name__)


def start(cfg: Config, stop_event: threading.Event) -> list[threading.Thread]:
    """Start one watcher thread per CRD. Returns the threads (daemon, don't join)."""
    try:
        kubernetes.config.load_incluster_config()
    except kubernetes.config.ConfigException:
        kubernetes.config.load_kube_config()  # local dev fallback
    threads = []
    for target in cfg.watchers.cr_instances:
        t = threading.Thread(
            target=_watch_loop,
            args=(target, cfg.watch_label, stop_event),
            daemon=True,
            name=f"watcher/{target.group}/{target.plural}",
        )
        t.start()
        threads.append(t)
        log.info("Started watcher for %s/%s in namespace %s", target.group, target.plural, target.namespace)
    return threads


# ── Watch loop ────────────────────────────────────────────────────────────────

def _watch_loop(target: CRWatchTarget, cluster: str, stop_event: threading.Event) -> None:
    """
    Continuously watches for CR changes. Restarts the stream on error
    (network blip, API server restart) with a short back-off.
    """
    # Track last-seen generation per CR to detect spec changes.
    # Status-only updates do NOT bump generation — no false triggers.
    last_generation: dict[str, int] = {}
    api = kubernetes.client.CustomObjectsApi()

    while not stop_event.is_set():
        try:
            _stream(api, target, cluster, last_generation, stop_event)
        except Exception as e:
            if stop_event.is_set():
                break
            log.warning("Watcher %s/%s error: %s — restarting in 5s", target.group, target.plural, e)
            stop_event.wait(5)


def _stream(
    api: kubernetes.client.CustomObjectsApi,
    target: CRWatchTarget,
    cluster: str,
    last_generation: dict[str, int],
    stop_event: threading.Event,
) -> None:
    w = kubernetes.watch.Watch()
    log.info("Watching %s.%s/%s in ns=%s", target.plural, target.group, target.version, target.namespace)

    for event in w.stream(
        api.list_namespaced_custom_object,
        group=target.group,
        version=target.version,
        namespace=target.namespace,
        plural=target.plural,
        timeout_seconds=300,  # reconnect every 5 min to get fresh resource_version
    ):
        if stop_event.is_set():
            w.stop()
            break

        event_type = event["type"]        # ADDED | MODIFIED | DELETED
        obj = event["object"]
        name = obj["metadata"]["name"]
        namespace = obj["metadata"]["namespace"]
        generation = obj["metadata"].get("generation", 0)
        key = f"{namespace}/{name}"

        if event_type == "DELETED":
            last_generation.pop(key, None)
            continue

        prev_generation = last_generation.get(key, -1)

        # First time we see the CR: record generation but don't fire an event.
        # We only want to watch changes, not initial state.
        if prev_generation == -1:
            last_generation[key] = generation
            log.debug("Tracking new CR %s (generation=%d)", key, generation)
            continue

        # Generation bumped → spec changed → emit ChangeEvent
        if generation > prev_generation:
            last_generation[key] = generation
            _emit_change_event(obj, target, cluster, prev_generation, generation)


def _emit_change_event(
    obj: dict,
    target: CRWatchTarget,
    cluster: str,
    prev_gen: int,
    new_gen: int,
) -> None:
    name = obj["metadata"]["name"]
    namespace = obj["metadata"]["namespace"]

    # Use the object's last transition time if available, otherwise now.
    # This becomes the anchor for all baseline and monitoring windows.
    event_timestamp = _parse_timestamp(
        obj.get("metadata", {}).get("managedFields", [{}])[-1].get("time")
    ) or datetime.now(timezone.utc)

    event = {
        "id": str(uuid.uuid4()),
        "source": "cr_instance",
        "resource_kind": target.kind,
        "resource_name": name,
        "namespace": namespace,
        "cluster": cluster,
        "event_timestamp": event_timestamp.isoformat(),
        "raw_payload": {
            "group": target.group,
            "version": target.version,
            "plural": target.plural,
            "generation_before": prev_gen,
            "generation_after": new_gen,
            "spec": obj.get("spec", {}),
            "annotations": obj.get("metadata", {}).get("annotations", {}),
        },
    }

    db.insert_change_event(event)
    log.info(
        "ChangeEvent: %s/%s generation %d→%d (event_timestamp=%s)",
        namespace, name, prev_gen, new_gen, event_timestamp.strftime("%H:%M:%S"),
    )


def _parse_timestamp(ts_str: str | None) -> datetime | None:
    if not ts_str:
        return None
    try:
        return datetime.fromisoformat(ts_str.rstrip("Z") + "+00:00")
    except Exception:
        return None
