"""
notify/render.py
----------------
Format normalized events into readable Discord messages.
Customize this file to change look/fields, or to add embeds later.
"""

from datetime import datetime, timezone

def event_to_discord_text(e: dict) -> str:
    title = e.get("title", "Untitled")
    link  = e.get("link") or ""
    src   = e.get("source", "").upper()
    kind  = e.get("kind", "")
    when  = e.get("detected_at") or datetime.now(timezone.utc).isoformat()
    sym   = e.get("symbol") or "—"
    score = e.get("score", 0.0)

    payload = e.get("payload", {}) or {}
    flair   = payload.get("flair") or "—"
    ups     = payload.get("ups", 0)
    com     = payload.get("comments", 0)
    sub     = payload.get("subreddit", "r/?")

    line1 = f"**[{src}] {title}**"
    if link:
        line1 += f"\n{link}"

    meta = f"- **Type:** {kind}   **Sub:** r/{sub}   **Flair:** {flair}\n" \
           f"- **Score:** {score:.2f}   **Ups/Com:** {ups}/{com}   **When (UTC):** {when}"
    if sym and sym != "—":
        meta += f"   **Symbol:** {sym}"

    return f"{line1}\n{meta}"
