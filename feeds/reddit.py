"""
Reddit feed module
------------------
Fetches recent posts from a subreddit using PRAW (official Reddit API),
normalizes them into a common "event" dict, and (optionally) filters by flair.

How to extend:
- Add more fields to the event payload if needed.
- Adjust `_score()` to change how "importance" is calculated.
- Modify `_matches_flair()` to filter by different flairs or keyword rules.
"""

import os
import re
import time
import hashlib
from typing import List, Dict, Any

import backoff            # for automatic retries
import orjson             # we don't use it here directly, but nice to have for dumps upstream
from loguru import logger
import praw               # Reddit API client

# ---------- Configuration (from environment) ----------
# Example: FLAIRS="DD,YOLO"
ALLOWED_FLAIRS = {
    s.strip().upper() for s in os.getenv("FLAIRS", "").split(",") if s.strip()
}
# If FLAIRS is empty, we won't filter by flair (we'll accept all posts).


# ---------- Client lifecycle ----------
_client = None

def _init_client():
    """
    Lazily initialize and memoize a PRAW client.
    Pulls credentials from environment variables set in .env.
    """
    global _client
    if _client:
        return _client

    cid  = os.getenv("REDDIT_CLIENT_ID")
    csec = os.getenv("REDDIT_CLIENT_SECRET")
    ua   = os.getenv("REDDIT_USER_AGENT", "my-scraper/1.0")

    if not (cid and csec):
        raise RuntimeError(
            "Missing Reddit creds: set REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET."
        )

    # NOTE: PRAW does not need username/password for public read-only.
    _client = praw.Reddit(client_id=cid, client_secret=csec, user_agent=ua)
    logger.info("Initialized Reddit client.")
    return _client


# ---------- Small helpers ----------
def _age_minutes(created_utc: float) -> float:
    """Return post age in minutes."""
    return max(0.0, (time.time() - (created_utc or time.time())) / 60.0)

def _score(ups: int, comments: int, age_min: float) -> float:
    """
    Convert engagement-per-minute to a 0–1 score using a simple squashing function.
    Tweak K to make the curve more/less aggressive.
    """
    x = (int(ups or 0) + 0.5 * int(comments or 0)) / max(1.0, age_min)
    K = 10.0
    return max(0.0, min(1.0, x / (x + K)))

def _fp(*parts: str) -> str:
    """Stable short fingerprint (for dedupe/idempotency)."""
    h = hashlib.sha256()
    for p in parts:
        h.update((p or "").encode("utf-8"))
    return h.hexdigest()[:16]

def _matches_flair(p) -> bool:
    """
    Return True if post matches desired flairs.
    - Checks the official "link flair" text first.
    - Also falls back to title patterns like "[DD]" or "YOLO:" just in case.
    If ALLOWED_FLAIRS is empty, accept everything.
    """
    if not ALLOWED_FLAIRS:
        return True

    flair = (getattr(p, "link_flair_text", "") or "").strip().upper()
    if flair in ALLOWED_FLAIRS:
        return True

    title = (getattr(p, "title", "") or "").upper()
    for tag in ALLOWED_FLAIRS:
        # Matches: "[DD]" or "DD:" or whole-word DD/YOLO
        if re.search(rf"(\[{tag}\]|^{tag}\b|\b{tag}\b:)", title):
            return True
    return False


# ---------- Public API ----------
@backoff.on_exception(backoff.expo, Exception, max_time=60)
def fetch_new(subreddit: str, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Pull the newest posts from `subreddit`, filter by flair, and
    return a list of normalized event dicts.

    Each event has this shape:
    {
      "source": "reddit",
      "kind": "post",
      "symbol": None,
      "detected_at": "...UTC ISO-ish...",
      "title": "...",
      "score": 0.0..1.0,
      "link": "https://reddit.com/...",
      "payload": {...raw fields...},
      "fingerprint": "abcd1234...",
      "version": "1.0"
    }
    """
    r = _init_client()
    sub = r.subreddit(subreddit)

    events: List[Dict[str, Any]] = []
    kept = 0

    for p in sub.new(limit=limit):
        # Flair / title rule
        if not _matches_flair(p):
            continue

        # Compute basics
        age = _age_minutes(getattr(p, "created_utc", None))
        link = f"https://reddit.com{getattr(p, 'permalink', '')}"

        # Build a normalized event
        event: Dict[str, Any] = {
            "source": "reddit",
            "kind": "post",
            "symbol": None,  # reserved for ticker if you parse it later
            "detected_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "title": (p.title or "")[:200],  # trim just in case
            "score": _score(p.ups or 0, p.num_comments or 0, age),
            "link": link,
            "payload": {
                "subreddit": str(p.subreddit),
                "flair": (getattr(p, "link_flair_text", "") or "").strip(),
                "ups": int(p.ups or 0),
                "comments": int(p.num_comments or 0),
                "author": str(getattr(p, "author", "")) if getattr(p, "author", None) else None,
                "created_utc": getattr(p, "created_utc", None),
                "id": getattr(p, "id", None),
            },
            "fingerprint": _fp("reddit", str(p.subreddit), getattr(p, "id", "")),
            "version": "1.0",
        }

        events.append(event)
        kept += 1

    logger.info(
        f"[Reddit] {subreddit}: kept {kept} posts "
        f"(flairs={sorted(ALLOWED_FLAIRS) if ALLOWED_FLAIRS else 'ALL'})"
    )
    return events
