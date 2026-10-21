"""
Loads and validates config.yaml. Provides a typed Config object used by all modules.
Secrets (API keys, webhook URLs) are read from environment variables.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

CONFIG_PATH = Path(__file__).parent / "config.yaml"


@dataclass
class CRWatchTarget:
    group: str
    version: str
    plural: str
    kind: str
    namespace: str


@dataclass
class WatchersConfig:
    cr_instances: list[CRWatchTarget] = field(default_factory=list)


@dataclass
class MetricsConfig:
    endpoint: str = "https://thanos-querier.openshift-monitoring.svc.cluster.local:9091"
    # Path to a bearer token file — used for OCP Thanos Querier auth.
    # In-cluster: automatically set to the service account token path.
    # Local dev: set to a file containing `oc whoami -t` output, or leave blank
    #            and set METRICS_TOKEN env var instead.
    token_path: Optional[str] = None
    verify_tls: bool = False   # OCP internal certs are self-signed; set True with a CA bundle


@dataclass
class LLMConfig:
    base_url: str = "https://api.openai.com/v1"
    model: str = "gpt-4o"
    api_key: str = ""


@dataclass
class Config:
    # watch_label: a short identifier used as the key in cluster_health and
    # in the health endpoint query (?clusters=<watch_label>).
    # This is NOT the hub cluster's name — it is just a routing label for the
    # demo. In the full design, clusters_affected is populated from ACM
    # Placement resolution (actual managed cluster names).
    watch_label: str
    watchers: WatchersConfig
    metrics: MetricsConfig
    llm: LLMConfig
    monitoring_plans: dict
    scanner_interval_seconds: int = 300
    slack_webhook_url: Optional[str] = None


def load(path: Path = CONFIG_PATH) -> Config:
    with open(path) as f:
        raw = yaml.safe_load(f)

    watchers = WatchersConfig(
        cr_instances=[
            CRWatchTarget(**t)
            for t in raw.get("watchers", {}).get("cr_instances", [])
        ]
    )

    metrics_raw = raw.get("metrics", {})
    metrics = MetricsConfig(
        endpoint=metrics_raw.get(
            "endpoint",
            "https://thanos-querier.openshift-monitoring.svc.cluster.local:9091",
        ),
        token_path=metrics_raw.get("token_path"),
        verify_tls=metrics_raw.get("verify_tls", False),
    )

    llm_raw = raw.get("llm", {})
    llm = LLMConfig(
        base_url=llm_raw.get("base_url", "https://api.openai.com/v1"),
        model=llm_raw.get("model", "gpt-4o"),
        api_key=os.environ.get("OPENAI_API_KEY", llm_raw.get("api_key", "")),
    )

    return Config(
        watch_label=raw.get("watch_label", "hub"),
        watchers=watchers,
        metrics=metrics,
        llm=llm,
        monitoring_plans=raw.get("monitoring_plans", {}),
        scanner_interval_seconds=raw.get("scanner_interval_seconds", 300),
        slack_webhook_url=os.environ.get(
            "SLACK_WEBHOOK_URL",
            raw.get("slack", {}).get("webhook_url"),
        ),
    )
