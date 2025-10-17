"""
main.py – runs Reddit + Truthbrush feeds, prints JSONL, sends to Discord, de-dupes,
supports RUN_ONCE=1, and graceful shutdown.
"""
from __future__ import annotations

import os
import time
import signal
from typing import Iterable

import orjson
import schedule
from loguru import logger

# Feeds
from feeds.reddit import fetch_new as reddit_fetch
from feeds.truthbrush_feed import fetch_truthbrush_statuses as truth_fetch

# Notifications
from notify.discord import send_text
from notify.render import event_to_discord_text

# ---- Config ----
SUBS: str = os.getenv("SUBREDDITS", "wallstreetbets")
LIMIT: int = int(os.getenv("PULL_LIMIT", "25"))
INTERVAL: int = int(os.getenv("INTERVAL_MIN", "10"))

TRUTH_HANDLES = [
    h.strip().lstrip("@")
    for h in os.getenv("TRUTH_HANDLES", os.getenv("TRUTH_USERNAME", "realDonaldTrump")).split(",")
    if h.strip()
]
TRUTH_LIMIT: int = int(os.getenv("TRUTH_LIMIT", "5"))
TRUTH_INTERVAL: int = int(os.getenv("TRUTH_INTERVAL_MIN", "30"))

MIN_SCORE: float = float(os.getenv("MIN_SCORE", "0.5"))
RUN_ONCE: bool = os.getenv("RUN_ONCE", "0") == "1"
SEEN_MAX: int = int(os.getenv("SEEN_MAX", "5000"))

# ---- Dedupe ----
SEEN: set[str] = set()

# ---- Helpers ----
def _print_event(e: dict) -> None:
    print(orjson.dumps(e).decode("utf-8"), flush=True)

def _maybe_send_discord(e: dict) -> None:
    if e.get("score", 0.0) < MIN_SCORE:
        return
    try:
        send_text(event_to_discord_text(e))
    except Exception as ex:
        logger.exception(f"Discord send failed: {ex}")

def _dedupe_and_emit(events: Iterable[dict]) -> None:
    global SEEN
    for e in events:
        fp = e.get("fingerprint")
        if fp and fp in SEEN:
            continue
        if fp:
            SEEN.add(fp)
            if len(SEEN) > SEEN_MAX:
                SEEN = set(list(SEEN)[-SEEN_MAX // 2 :])
        _print_event(e)
        _maybe_send_discord(e)

# ---- Jobs ----
def reddit_job() -> None:
    subs = [s.strip() for s in SUBS.split(",") if s.strip()]
    for sub in subs:
        try:
            events = reddit_fetch(subreddit=sub, limit=LIMIT)
            _dedupe_and_emit(events)
        except Exception as ex:
            logger.exception(f"Reddit fetch error for r/{sub}: {ex}")

def truth_job() -> None:
    for handle in TRUTH_HANDLES:
        try:
            events = truth_fetch(handle=handle, limit=TRUTH_LIMIT)
            _dedupe_and_emit(events)
        except Exception as ex:
            logger.exception(f"Truth fetch error for @{handle}: {ex}")

# ---- Signals ----
_stop = False

def _request_stop(*_) -> None:
    global _stop
    _stop = True
    logger.info("Shutdown signal received; finishing current cycle…")

# ---- Main ----
def main() -> None:
    logger.add("app.log", rotation="1 day", retention="7 days")
    logger.info(
        "Starting scheduler:\n"
        f"  Reddit: subs={SUBS} every {INTERVAL} min; limit={LIMIT}\n"
        f"  Truth:  handles={TRUTH_HANDLES} every {TRUTH_INTERVAL} min; limit={TRUTH_LIMIT}\n"
        f"  MIN_SCORE={MIN_SCORE} RUN_ONCE={RUN_ONCE}"
    )

    # Run immediately
    reddit_job()
    truth_job()

    if RUN_ONCE:
        logger.info("RUN_ONCE=1; exiting after first run.")
        return

    try:
        signal.signal(signal.SIGINT, _request_stop)
        signal.signal(signal.SIGTERM, _request_stop)
    except Exception:
        pass

    if INTERVAL > 0:
        schedule.every(INTERVAL).minutes.do(reddit_job)
    if TRUTH_INTERVAL > 0:
        schedule.every(TRUTH_INTERVAL).minutes.do(truth_job)

    while not _stop:
        schedule.run_pending()
        time.sleep(1)

    logger.info("Stopped.")

if __name__ == "__main__":
    main()

