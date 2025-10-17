from datetime import datetime, timezone

def _fmt_usd(n):
    try:
        return f"${float(n):,.0f}"
    except Exception:
        return str(n)

def event_to_discord_text(e: dict) -> str:
    title = e.get("title", "Untitled")
    link  = e.get("link") or ""
    src   = e.get("source", "").upper()
    kind  = e.get("kind", "")
    when  = e.get("detected_at") or datetime.now(timezone.utc).isoformat()
    sym   = e.get("symbol") or "—"
    score = e.get("score", 0.0)

    payload = e.get("payload", {}) or {}

    # Header + link
    line1 = f"**[{src}] {title}**"
    if link:
        line1 += f"\n{link}"

    # Common meta
    meta = f"- **Type:** {kind}   **Score:** {score:.2f}   **When (UTC):** {when}"

    if src == "REDDIT":
        sub   = payload.get("subreddit", "?")
        flair = payload.get("flair") or "—"
        ups   = payload.get("ups", 0)
        com   = payload.get("comments", 0)
        meta += f"   **Sub:** r/{sub}   **Flair:** {flair}   **Ups/Com:** {ups}/{com}"

    elif src == "TRUTHSOCIAL":
        user = payload.get("username") or "—"
        meta += f"   **User:** @{user}"

    elif src == "HYPERLIQUID":
        # Pretty summary with top sellers
        coin = sym
        oi_z   = payload.get("oi_z", 0.0)
        prem_z = payload.get("premium_z", 0.0)
        skew   = payload.get("book_skew", 1.0)
        whale_raw = payload.get("whale_net_short_usd_5m_raw", 0.0)
        whale_wt  = payload.get("whale_net_short_usd_5m_weighted", 0.0)
        top = payload.get("top_sellers", []) or []

        meta += f"   **Coin:** {coin}"
        meta += f"\n- **OI z:** {oi_z:.2f}   **Prem z:** {prem_z:.2f}   **Skew (bid/ask):** {skew:.2f}"
        meta += f"\n- **Whale net short (5m):** raw={_fmt_usd(whale_raw)}  weighted={_fmt_usd(whale_wt)}"

        if top:
            lines = []
            for t in top:
                name = (t.get('user') or 'unknown')[:10] + '…'
                label = t.get('label')
                if label:
                    name += f" ({label})"
                lines.append(f"  • {name}  prints={t.get('prints',0)}  "
                             f"raw={_fmt_usd(t.get('raw_notional_usd',0))}  "
                             f"wt={_fmt_usd(t.get('weighted_notional_usd',0))}")
            meta += "\n- **Top sellers:**\n" + "\n".join(lines)

    if sym and sym != "—" and src != "HYPERLIQUID":
        meta += f"   **Symbol:** {sym}"

    return f"{line1}\n{meta}"

