"""
main.py
-------
Single entrypoint that:
- Reads config from environment (.env when using `--env-file` with Docker).
- Schedules the Reddit job to run every N minutes.
- Emits normalized events as compact JSON lines to STDOUT (easy to pipe/collect).
- Sends high-score events to Discord (webhook or bot) if credentials are set.
- De-duplicates events during the process lifetime via a fingerprint set.
- Supports RUN_ONCE=1 to do a single fetch and exit (useful for quick tests).

To extend with more feeds:
1) Create `feeds/<name>.py` exposing `fetch_new(...) -> list[dict]`.
2) Import it here and call inside `job()`.
3) Keep the same normalized event shape so notify/render continues to work.
"""

from __future__ import annotations

import os
import time
import signal
import orjson
import schedule
from typing import Iterable

from loguru import logger

# Feeds
from feeds.reddit import fetch_new as reddit_fetch

# Notifications (both are safe no-ops if env isn’t set)
from notify.discord import send_text
from notify.render import event_to_discord_text

# -----------------------
# Configuration (env)
# -----------------------
# Subreddits to poll (comma-separated). Flair filtering happens inside feeds/reddit.py via FLAIRS env.
SUBS: str = os.getenv("SUBREDDITS", "wallstreetbets")

# How many posts to request per subreddit per run
LIMIT: int = int(os.getenv("PULL_LIMIT", "25"))

# Minutes between scheduled runs
INTERVAL: int = int(os.getenv("INTERVAL_MIN", "10"))

# Only send to Discord if event score >= MIN_SCORE
MIN_SCORE: float = float(os.getenv("MIN_SCORE", "0.5"))

# If set to "1", run one cycle and exit (testing / CI)
RUN_ONCE: bool = os.getenv("RUN_ONCE", "0") == "1"

# Max number of fingerprints to retain in memory for dedupe (avoid unbounded growth)
SEEN_MAX: int = int(os.getenv("SEEN_MAX", "5000"))


# -----------------------
# In-memory dedupe store
# -----------------------
SEEN: set[str] = set()


# -----------------------
# Output helpers
# -----------------------
def _print_event(e: dict) -> None:
    """
    Single place to emit events. Currently prints JSONL to STDOUT.
    Replace/extend this to write to file/DB, Kafka, etc.
    """
    # `orjson.dumps` returns bytes; decode to str for stdout.
    print(orjson.dumps(e).decode("utf-8"), flush=True)


def _maybe_send_discord(e: dict) -> None:
    """
    Push to Discord if the event passes the score threshold.
    Uses webhook or bot token based on available env vars.
    """
    if e.get("score", 0.0) < MIN_SCORE:
        return
    try:
        msg = event_to_discord_text(e)
        send_text(msg)  # Safe no-op if creds aren’t configured
    except Exception as ex:
        logger.exception(f"Discord send failed: {ex}")


def _dedupe_and_emit(events: Iterable[dict]) -> None:
    """
    De-duplicate by event fingerprint for the lifetime of this process.
    If you need persistence across restarts, store fingerprints in SQLite.
    """
    global SEEN
    for e in events:
        fp = e.get("fingerprint")
        if fp and fp in SEEN:
            # Already emitted in this process; skip
            continue

        # Track fingerprint (cap size)
        if fp:
            SEEN.add(fp)
            if len(SEEN) > SEEN_MAX:
                # Simple compaction: keep the most recent half
                SEEN = set(list(SEEN)[-SEEN_MAX // 2 :])

        # Emit & optionally alert
        _print_event(e)
        _maybe_send_discord(e)


# -----------------------
# The scheduled job
# -----------------------
def job() -> None:
    """
    One tick:
    - Iterate subreddits
    - Fetch Reddit events (flair filtering inside feeds/reddit.py)
    - Dedupe & emit output/alerts
    """
    subs = [s.strip() for s in SUBS.split(",") if s.strip()]
    for sub in subs:
        try:
            events = reddit_fetch(subreddit=sub, limit=LIMIT)
            _dedupe_and_emit(events)
        except Exception as ex:
            # Never let an exception kill the scheduler loop
            logger.exception(f"Reddit fetch error for r/{sub}: {ex}")


# -----------------------
# Graceful shutdown
# -----------------------
_stop_requested = False


def _request_stop(*_args) -> None:
    global _stop_requested
    _stop_requested = True
    logger.info("Shutdown signal received; finishing current cycle...")


# -----------------------
# Main
# -----------------------
def main() -> None:
    # Log to a rotating file inside the container (mount a volume if you want it on the host)
    logger.add("app.log", rotation="1 day", retention="7 days")
    logger.info(
        f"Starting scheduler: subs={SUBS} every {INTERVAL} min; "
        f"limit={LIMIT}; min_score={MIN_SCORE}; run_once={RUN_ONCE}"
    )

    # Run immediately on start so you see output right away
    job()

    if RUN_ONCE:
        logger.info("RUN_ONCE=1 set; exiting after first run.")
        return

    # Set up signals for graceful shutdown (works in Docker)
    try:
        signal.signal(signal.SIGINT, _request_stop)
        signal.signal(signal.SIGTERM, _request_stop)
    except Exception:
        # Some environments (e.g., Windows) may limit signal usage; ignore if not available
        pass

    # Schedule repeating runs
    schedule.every(INTERVAL).minutes.do(job)

    # Lightweight scheduler loop
    while not _stop_requested:
        schedule.run_pending()
        time.sleep(1)

    logger.info("Stopped.")


if __name__ == "__main__":
    main()
