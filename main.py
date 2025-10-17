"""
main.py — Reddit + Hyperliquid tracker (Truth removed)
- Fetches Reddit posts, computes scores, sends to Discord
- Runs Hyperliquid whale-short detector (background WS + REST), sends alerts
- Prints JSONL for each event
- Supports RUN_ONCE=1 and graceful shutdown
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
from feeds.hyperliquid import ensure_started as hl_start, collect_events as hl_collect

# Notifications
from notify.discord import send_text
from notify.render import event_to_discord_text

# ---- Config (env) ----
SUBS: str = os.getenv("SUBREDDITS", "wallstreetbets")              # e.g., "wallstreetbets,stocks"
LIMIT: int = int(os.getenv("PULL_LIMIT", "25"))                    # posts per subreddit per run
INTERVAL_MIN: int = int(os.getenv("INTERVAL_MIN", "10"))           # Reddit cadence (minutes)

MIN_SCORE: float = float(os.getenv("MIN_SCORE", "0.5"))            # only send if score >= MIN_SCORE
RUN_ONCE: bool = os.getenv("RUN_ONCE", "0") == "1"                 # 1 = run once then exit
SEEN_MAX: int = int(os.getenv("SEEN_MAX", "5000"))                 # dedupe memory cap

# Hyperliquid cadence (seconds)
HL_INTERVAL_SEC: int = int(os.getenv("HL_INTERVAL_SEC", "60"))

# ---- Dedupe ----
SEEN: set[str] = set()

# ---- Helpers ----
def _print_event(e: dict) -> None:
    """Print one normalized event as JSONL (useful for logs / piping)."""
    print(orjson.dumps(e).decode("utf-8"), flush=True)

def _maybe_send_discord(e: dict) -> None:
    """Send to Discord if event score clears threshold."""
    if e.get("score", 0.0) < MIN_SCORE:
        return
    try:
        send_text(event_to_discord_text(e))
    except Exception as ex:
        logger.exception(f"Discord send failed: {ex}")

def _dedupe_and_emit(events: Iterable[dict]) -> None:
    """Dedupe by fingerprint, then print + notify."""
    global SEEN
    for e in events:
        fp = e.get("fingerprint")
        if fp and fp in SEEN:
            continue
        if fp:
            SEEN.add(fp)
            if len(SEEN) > SEEN_MAX:
                # trim (keep back half)
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
            logger.exception(f"[Reddit] fetch error for r/{sub}: {ex}")

def hyperliquid_job() -> None:
    try:
        events = hl_collect()  # internal cap controlled by HL_MAX_EVENTS_PER_PULSE
        _dedupe_and_emit(events)
    except Exception as ex:
        logger.exception(f"[Hyperliquid] job error: {ex}")

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
        f"  Reddit: subs={SUBS} every {INTERVAL_MIN} min; limit={LIMIT}\n"
        f"  Hyperliquid: interval={HL_INTERVAL_SEC}s\n"
        f"  MIN_SCORE={MIN_SCORE} RUN_ONCE={RUN_ONCE}"
    )

    # Start HL background services (WS + REST) once
    hl_start()

    # Run immediately on startup
    reddit_job()
    hyperliquid_job()

    if RUN_ONCE:
        logger.info("RUN_ONCE=1; exiting after first run.")
        return

    # Catch Ctrl+C / container stop
    try:
        signal.signal(signal.SIGINT, _request_stop)
        signal.signal(signal.SIGTERM, _request_stop)
    except Exception:
        pass

    # Schedule periodic jobs
    if INTERVAL_MIN > 0:
        schedule.every(INTERVAL_MIN).minutes.do(reddit_job)
    if HL_INTERVAL_SEC > 0:
        schedule.every(HL_INTERVAL_SEC).seconds.do(hyperliquid_job)

    # Main loop
    while not _stop:
        schedule.run_pending()
        time.sleep(1)

    logger.info("Stopped.")

if __name__ == "__main__":
    main()
