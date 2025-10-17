"""
notify/discord.py
-----------------
Send messages to a Discord channel using a BOT TOKEN (no webhook).
Env:
  - DISCORD_TOKEN: your bot token (starts with 'MT...' etc.)
  - CHANNEL_ID:    target text channel ID (or thread ID)

Notes:
  - The bot must be invited to the server and have 'View Channel' + 'Send Messages'
    permissions in the target channel.
  - Content limit is 2000 chars; we chunk at 1900 just in case.
"""

import os
import time
import httpx
import backoff
from loguru import logger

# Support either your names or common alternates:
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN") or os.getenv("DISCORD_BOT_TOKEN")
CHANNEL_ID    = os.getenv("CHANNEL_ID") or os.getenv("DISCORD_CHANNEL_ID")

API_BASE = "https://discord.com/api/v10"

def _chunks(s: str, n: int = 1900):
    """Yield chunks <= n chars (Discord hard limit ~2000)."""
    for i in range(0, len(s), n):
        yield s[i:i+n]

def _on_backoff(details):
    logger.warning(f"Discord retrying: tries={details.get('tries')} wait={details.get('wait')}s")

@backoff.on_exception(backoff.expo, (httpx.HTTPError,), max_time=60, on_backoff=_on_backoff)
def _post_bot(content: str):
    if not DISCORD_TOKEN or not CHANNEL_ID:
        raise RuntimeError("Missing DISCORD_TOKEN or CHANNEL_ID env vars")

    url = f"{API_BASE}/channels/{CHANNEL_ID}/messages"
    headers = {
        "Authorization": f"Bot {DISCORD_TOKEN}",
        # optional but nice to have:
        "User-Agent": "moonla-bot/1.0 (+https://discord.com)"
    }

    with httpx.Client(timeout=20, headers=headers) as c:
        r = c.post(url, json={
            "content": content,
            "allowed_mentions": {"parse": []}  # avoid accidental @everyone
        })

        # Helpful diagnostics:
        if r.status_code in (401, 403):
            logger.error(f"Discord auth/perm error {r.status_code}: {r.text}")

        if r.status_code == 429:
            # Respect Discord rate limit
            try:
                retry = float(r.json().get("retry_after", 1.5))
            except Exception:
                retry = 1.5
            logger.warning(f"Discord rate limited, sleeping {retry}s")
            time.sleep(retry)
            # Raise so backoff retries
            raise httpx.HTTPError("429 rate limited")

        r.raise_for_status()

def send_text(content: str):
    """Public helper: send string (chunked) via bot."""
    if not content:
        return
    for chunk in _chunks(content):
        _post_bot(chunk)

