"""
change-sentinel — starter entry point.

Wires together:
  - CRInstanceWatcher  (daemon thread per CRD)
  - Scanner            (daemon thread, runs every 5 min)
  - FastAPI server     (health signal endpoint)

Usage:
  cd sentinel/
  pip install -r requirements.txt
  python main.py

Environment variables:
  OPENAI_API_KEY      — required for CorrelationAgent LLM calls
  SLACK_WEBHOOK_URL   — optional; notifications printed to stdout if not set
  SENTINEL_CONFIG     — path to config.yaml (default: ./config.yaml)
"""

import logging
import os
import threading
from pathlib import Path

import uvicorn

import config as config_mod
import db
import scanner
import watcher
from api import app

logging.basicConfig(
    level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s  %(levelname)-8s  %(name)-20s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("main")


def main() -> None:
    config_path = Path(os.environ.get("SENTINEL_CONFIG", Path(__file__).parent / "config.yaml"))
    cfg = config_mod.load(config_path)

    log.info("change-sentinel starting (cluster=%s)", cfg.watch_label)

    db.init_db()
    log.info("Database initialised")

    stop_event = threading.Event()

    # Start watchers — one thread per CRD
    watcher_threads = watcher.start(cfg, stop_event)
    log.info("Started %d watcher(s)", len(watcher_threads))

    # Start scanner — creates WatchSessions from unprocessed events
    scanner_thread = scanner.start(cfg, stop_event)

    # Start API server — blocking, runs in main thread
    log.info("Health signal endpoint: http://0.0.0.0:8000/api/v1/health")
    try:
        uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
    except KeyboardInterrupt:
        log.info("Shutting down...")
        stop_event.set()


if __name__ == "__main__":
    main()
