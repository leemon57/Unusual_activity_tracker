# feeds/hyperliquid.py
# ------------------------------------------------------------
# Hyperliquid unusual-activity detector (perps) with:
# - WebSocket "trades" stream (buyer/seller attribution)
# - Periodic /info metaAndAssetCtxs for OI / mark / oracle premium
# - Rolling window metrics (default 5m)
# - Composite “unusual short” score
# - Watchlist: label addresses and apply weight multipliers to their flow
# - Debug flag logs internal metrics even when no alert fires
#
# Env (optional, with defaults):
#   HL_COINS=BTC,ETH
#   HL_WS_URL=wss://api.hyperliquid.xyz/ws
#   HL_INFO_URL=https://api.hyperliquid.xyz/info
#   HL_WINDOW_SEC=300
#   HL_MIN_WHALE_TRADE_USD=250000
#   HL_WHALE_UNIT_USD=5000000
#   HL_SCORE_TRIGGER=1.0
#   HL_BOOK_SKEW_TRIGGER=0.7
#   HL_INFO_POLL_SEC=30
#   HL_MAX_EVENTS_PER_PULSE=3
#   HL_DEBUG_METRICS=0          # set to 1 to log internals
#
#   # --- Watchlist ---
#   HL_WATCHLIST_PATH=watchlist.json   # or .csv with columns: address,label,weight
#   HL_WATCHLIST_RELOAD_SEC=60
#   HL_DEFAULT_WATCH_WEIGHT=1.0
#
# Requires: websockets, httpx, backoff, loguru
# ------------------------------------------------------------

from __future__ import annotations

import os
import csv
import json
import math
import time
import asyncio
import threading
import contextlib
import pathlib
from dataclasses import dataclass, field
from typing import Dict, List, Deque, Tuple, Optional
from collections import deque, defaultdict

import httpx
import backoff
from loguru import logger

# websockets is optional at import time so your app can still boot without it
try:
    import websockets  # type: ignore
except Exception:
    websockets = None
    logger.warning("websockets package not installed; Hyperliquid live trades disabled")


# ---------- Config ----------
def _env_clean(name: str, default: str = "") -> str:
    """Read env var and strip inline comments / whitespace."""
    v = os.getenv(name, default)
    return v.split("#", 1)[0].strip()


HL_WS_URL        = _env_clean("HL_WS_URL", "wss://api.hyperliquid.xyz/ws")
HL_INFO_URL      = _env_clean("HL_INFO_URL", "https://api.hyperliquid.xyz/info")
HL_COINS         = [c.strip().upper() for c in _env_clean("HL_COINS", "BTC,ETH").split(",") if c.strip()]
HL_WINDOW_SEC    = int(_env_clean("HL_WINDOW_SEC", "300"))
HL_MIN_WHALE_T   = float(_env_clean("HL_MIN_WHALE_TRADE_USD", "250000"))
HL_WHALE_UNIT    = float(_env_clean("HL_WHALE_UNIT_USD", "5000000"))
HL_SCORE_TRIG    = float(_env_clean("HL_SCORE_TRIGGER", "1.0"))
HL_SKEW_TRIG     = float(_env_clean("HL_BOOK_SKEW_TRIGGER", "0.7"))
HL_INFO_POLL_SEC = int(_env_clean("HL_INFO_POLL_SEC", "30"))
HL_MAX_EVENTS    = int(_env_clean("HL_MAX_EVENTS_PER_PULSE", "3"))
DEBUG_METRICS    = _env_clean("HL_DEBUG_METRICS", "0") == "1"

# Watchlist knobs
HL_WATCHLIST_PATH        = _env_clean("HL_WATCHLIST_PATH", "watchlist.json")
HL_WATCHLIST_RELOAD_SEC  = int(_env_clean("HL_WATCHLIST_RELOAD_SEC", "60"))
HL_DEFAULT_WATCH_WEIGHT  = float(_env_clean("HL_DEFAULT_WATCH_WEIGHT", "1.0"))


# ---------- Helpers ----------
def _now() -> float:
    return time.time()


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _zscore(series: List[float]) -> float:
    """z of LAST point vs previous points; 0 if insufficient variance/history."""
    if len(series) < 5:
        return 0.0
    x = series[-1]
    xs = series[:-1]
    mean = sum(xs) / len(xs)
    var = sum((v - mean) ** 2 for v in xs) / max(1, len(xs) - 1)
    std = math.sqrt(var) if var > 0 else 0.0
    if std == 0:
        return 0.0
    return (x - mean) / std


def _fp(parts: List[str]) -> str:
    import hashlib
    h = hashlib.sha256()
    for p in parts:
        h.update((p or "").encode("utf-8"))
    return h.hexdigest()[:16]


# ---------- Watchlist (optional) ----------
# In-memory: addr_lower -> {"label": str, "weight": float}
_watchlist: Dict[str, Dict[str, object]] = {}
_watchlist_path = pathlib.Path(HL_WATCHLIST_PATH)
_watchlist_mtime = 0.0
_last_watchlist_check = 0.0


def _load_watchlist_once() -> None:
    """Load JSON or CSV watchlist if present."""
    global _watchlist, _watchlist_mtime
    if not _watchlist_path.exists():
        return
    try:
        mtime = _watchlist_path.stat().st_mtime
        if mtime == _watchlist_mtime:
            return
        _watchlist_mtime = mtime

        wl: Dict[str, Dict[str, object]] = {}
        if _watchlist_path.suffix.lower() == ".json":
            data = json.loads(_watchlist_path.read_text(encoding="utf-8"))
            # JSON format: [{"address":"0xabc..", "label":"Whale A", "weight":1.5}, ...]
            for row in data:
                addr = str(row.get("address", "")).lower().strip()
                if not addr:
                    continue
                wl[addr] = {
                    "label": str(row.get("label", "")),
                    "weight": float(row.get("weight", HL_DEFAULT_WATCH_WEIGHT)),
                }
        else:
            # CSV format: address,label,weight
            with _watchlist_path.open("r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    addr = str(row.get("address", "")).lower().strip()
                    if not addr:
                        continue
                    wl[addr] = {
                        "label": str(row.get("label", "")),
                        "weight": float(row.get("weight", HL_DEFAULT_WATCH_WEIGHT)),
                    }
        _watchlist = wl
        logger.info(f"[HL] Loaded watchlist: {len(_watchlist)} entries from {_watchlist_path}")
    except Exception as e:
        logger.warning(f"[HL] Watchlist load failed: {e}")


def _maybe_reload_watchlist():
    """Hot-reload watchlist every HL_WATCHLIST_RELOAD_SEC seconds."""
    global _last_watchlist_check
    now = _now()
    if now - _last_watchlist_check >= HL_WATCHLIST_RELOAD_SEC:
        _last_watchlist_check = now
        _load_watchlist_once()


def _address_label(addr: Optional[str]) -> Tuple[str, float]:
    """Return (label_display, weight) from watchlist, defaults if missing."""
    if not addr:
        return ("", HL_DEFAULT_WATCH_WEIGHT)
    ent = _watchlist.get(addr.lower())
    if not ent:
        return ("", HL_DEFAULT_WATCH_WEIGHT)
    return (str(ent.get("label", "")), float(ent.get("weight", HL_DEFAULT_WATCH_WEIGHT)))


# ---------- State ----------
@dataclass
class TradeItem:
    ts: float
    coin: str
    side: str             # 'B' (taker buys) or 'S' (taker sells)
    px: float
    sz: float
    notional: float
    buyer: Optional[str]
    seller: Optional[str]
    hash: Optional[str] = None


@dataclass
class CoinState:
    trades: Deque[TradeItem] = field(default_factory=deque)
    best_bid_sz: float = 0.0
    best_ask_sz: float = 0.0
    oi_series: Deque[Tuple[float, float]] = field(default_factory=deque)    # (ts, OI USD)
    prem_series: Deque[Tuple[float, float]] = field(default_factory=deque)  # (ts, (mark-oracle)/oracle)
    last_alert_ts: float = 0.0


_STATE: Dict[str, CoinState] = defaultdict(CoinState)
_loop: Optional[asyncio.AbstractEventLoop] = None
_thread: Optional[threading.Thread] = None
_stop_event = threading.Event()
_lock = threading.Lock()


# ---------- WebSocket worker ----------
async def _subscribe(ws, coin: str, sub_type: str) -> None:
    """
    Subscribe to a ws stream.
    Official shape: {"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}
    """
    await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": sub_type, "coin": coin}}))


def _parse_trade_msg(msg: dict) -> List[TradeItem]:
    """
    Parse trades, accepting a few shapes:
      - {"data":[{...trade...}, ...]}
      - {"trades":[...]}
      - direct list/object
    Each trade ideally contains: coin/symbol, side, px, sz, and either users or side_info.
    """
    out: List[TradeItem] = []
    batch = msg.get("data", msg)
    if isinstance(batch, dict) and "trades" in batch:
        batch = batch["trades"]
    if not isinstance(batch, list):
        batch = [batch]

    now = _now()
    for t in batch:
        try:
            coin = (t.get("coin") or t.get("symbol") or "").upper()

            # --- side normalization here ---
            raw_side = str(t.get("side") or t.get("takerSide") or t.get("dir") or "").upper()
            if raw_side in {"B", "BUY", "BID"}:
                side = "B"
            elif raw_side in {"S", "SELL", "A", "ASK"}:
                side = "S"
            else:
                side = raw_side or "B"
            # -------------------------------

            px   = float(t.get("px") or t.get("price") or 0.0)
            sz   = float(t.get("sz") or t.get("size") or 0.0)
            notional = abs(px * sz)
            buyer = seller = None
            if "users" in t and isinstance(t["users"], list) and len(t["users"]) == 2:
                buyer, seller = t["users"][0], t["users"][1]
            elif "side_info" in t and isinstance(t["side_info"], list) and len(t["side_info"]) == 2:
                buyer = t["side_info"][0].get("user")
                seller = t["side_info"][1].get("user")

            out.append(TradeItem(
                ts=now, coin=coin, side=side, px=px, sz=sz, notional=notional,
                buyer=buyer, seller=seller, hash=t.get("hash")
            ))
        except Exception:
            continue
    return out


async def _ws_worker():
    """Connect, subscribe to trades + bbo for all coins, and feed state."""
    if websockets is None:
        logger.error("websockets package missing; skipping HL ws worker")
        return

    @backoff.on_exception(backoff.expo, Exception, max_time=None, jitter=None)
    async def _connect_and_run():
        async with websockets.connect(HL_WS_URL, max_queue=1000, ping_interval=20) as ws:
            # subscribe
            for coin in HL_COINS:
                with contextlib.suppress(Exception):
                    await _subscribe(ws, coin, "trades")
                with contextlib.suppress(Exception):
                    await _subscribe(ws, coin, "bbo")

            # loop
            while not _stop_event.is_set():
                raw = await asyncio.wait_for(ws.recv(), timeout=30)
                with contextlib.suppress(Exception):
                    msg = json.loads(raw)

                    # trades
                    if (isinstance(msg, dict) and
                        (msg.get("channel") == "trades"
                         or msg.get("subscription", {}).get("type") == "trades"
                         or isinstance(msg.get("data", None), list))):
                        trades = _parse_trade_msg(msg)
                        if trades:
                            with _lock:
                                for tr in trades:
                                    _STATE[tr.coin].trades.append(tr)
                            if DEBUG_METRICS:
                                # simple tick log: how many trades just arrived + side mix
                                from collections import Counter
                                c = trades[0].coin
                                sides = Counter(t.side for t in trades)
                                logger.info(f"[HL/WS] {c} got {len(trades)} trades (sides={dict(sides)})")

                    # bbo → store best sizes (for rough skew)
                    if (isinstance(msg, dict) and
                        (msg.get("channel") == "bbo"
                         or msg.get("subscription", {}).get("type") == "bbo")):
                        data = msg.get("data", msg)
                        coin = (data.get("coin") or data.get("symbol") or "").upper()
                        if coin:
                            with _lock:
                                st = _STATE[coin]
                                with contextlib.suppress(Exception):
                                    st.best_bid_sz = float(data.get("bestBidSz") or data.get("bidSz") or 0.0)
                                    st.best_ask_sz = float(data.get("bestAskSz") or data.get("askSz") or 0.0)

    # reconnect forever
    while not _stop_event.is_set():
        try:
            await _connect_and_run()
        except Exception as e:
            logger.warning(f"WS worker error: {e}; reconnecting in 2s")
            with contextlib.suppress(Exception):
                await asyncio.sleep(2)


# ---------- /info poller ----------
async def _info_poller():
    """
    Poll /info metaAndAssetCtxs to get OI / mark / oracle (premium) and update series.
    Handles 'universe' being a list of dicts, strings, or lists/tuples.
    """
    async with httpx.AsyncClient(timeout=10) as client:

        @backoff.on_exception(backoff.expo, Exception, max_time=None)
        async def _tick():
            r = await client.post(HL_INFO_URL, json={"type": "metaAndAssetCtxs"})
            r.raise_for_status()
            j = r.json()

            universe = j.get("universe") or []
            # Robust mapping: index -> coin name (uppercased)
            idx_to_name: Dict[int, str] = {}
            for idx, item in enumerate(universe):
                name = None
                if isinstance(item, dict):
                    name = item.get("name") or item.get("asset") or item.get("symbol")
                elif isinstance(item, (list, tuple)) and item:
                    name = item[0]
                else:
                    name = str(item)
                name = (name or "").strip()
                if name:
                    idx_to_name[idx] = name.upper()

            asset_ctxs = j.get("assetCtxs") or []
            ts = _now()

            with _lock:
                for ctx in asset_ctxs:
                    if not isinstance(ctx, dict):
                        continue

                    # Resolve coin symbol
                    coin = (ctx.get("name") or ctx.get("asset") or ctx.get("symbol") or "").upper()
                    if not coin and "index" in ctx:
                        coin = idx_to_name.get(ctx["index"], "")
                    if not coin or coin not in HL_COINS:
                        continue

                    st = _STATE[coin]
                    perp = ctx.get("perpStats") or {}

                    # OI (USD notionals)
                    oi_val = None
                    with contextlib.suppress(Exception):
                        oi_val = float(perp.get("openInterest") or ctx.get("openInterest") or 0.0)
                    if oi_val is not None:
                        st.oi_series.append((ts, oi_val))
                        # keep ~1h history at ~30s cadence
                        while st.oi_series and ts - st.oi_series[0][0] > 3600:
                            st.oi_series.popleft()

                    # premium = (mark - oracle)/oracle
                    with contextlib.suppress(Exception):
                        mark = float(perp.get("markPx") or ctx.get("markPx") or 0.0)
                        oracle = float(perp.get("oraclePx") or ctx.get("oraclePx") or 0.0)
                        prem = ((mark - oracle) / oracle) if oracle else 0.0
                        st.prem_series.append((ts, prem))
                        while st.prem_series and ts - st.prem_series[0][0] > 3600:
                            st.prem_series.popleft()

            if DEBUG_METRICS:
                # Log latest samples for your watch coins
                with _lock:
                    for coin in HL_COINS:
                        st = _STATE[coin]
                        oi  = st.oi_series[-1][1] if st.oi_series else None
                        prm = st.prem_series[-1][1] if st.prem_series else None
                        logger.info(f"[HL/INFO] {coin} oi={oi} prem={prm}")

        while not _stop_event.is_set():
            with contextlib.suppress(Exception):
                await _tick()
            with contextlib.suppress(Exception):
                await asyncio.sleep(HL_INFO_POLL_SEC)


# ---------- Orchestration ----------
def _start_background_once():
    """Start a daemon thread with an asyncio loop running ws worker + info poller."""
    global _loop, _thread
    if _thread and _thread.is_alive():
        return

    _stop_event.clear()
    _load_watchlist_once()  # initial load if file exists
    _loop = asyncio.new_event_loop()

    def runner():
        asyncio.set_event_loop(_loop)
        tasks = []
        if websockets is not None:
            tasks.append(_loop.create_task(_ws_worker()))
        tasks.append(_loop.create_task(_info_poller()))
        try:
            _loop.run_until_complete(asyncio.gather(*tasks))
        except Exception as e:
            logger.warning(f"[HL] loop stopped: {e}")

    _thread = threading.Thread(target=runner, name="HL-bg", daemon=True)
    _thread.start()
    logger.info(f"[HL] Background started, coins={HL_COINS}")


def ensure_started() -> None:
    """Call once from your app startup (e.g., first time the job runs)."""
    _start_background_once()


def shutdown():
    """Optional: call on app exit."""
    _stop_event.set()
    if _loop:
        _loop.call_soon_threadsafe(_loop.stop)
    if _thread:
        _thread.join(timeout=2)


# ---------- Metrics & Scoring ----------
def _book_skew(coin: str) -> float:
    with _lock:
        st = _STATE[coin]
        bid, ask = st.best_bid_sz, st.best_ask_sz
    if bid <= 0 or ask <= 0:
        return 1.0  # neutral when unknown
    return bid / max(1e-9, ask)


def _series_slice(series: Deque[Tuple[float, float]], window_sec: int) -> List[float]:
    ts_now = _now()
    return [v for (ts, v) in series if ts_now - ts <= window_sec]


def _oi_change_z(coin: str, window_sec: int) -> float:
    # z-score of OI change over window relative to earlier points
    s = _series_slice(_STATE[coin].oi_series, window_sec)
    if len(s) < 5:
        return 0.0
    diffs = []
    base = s[0]
    for v in s:
        diffs.append(v - base)
    return _zscore(diffs)


def _premium_z(coin: str, window_sec: int) -> float:
    s = _series_slice(_STATE[coin].prem_series, window_sec)
    if len(s) < 5:
        return 0.0
    return _zscore(s)


def _aggregate_whale_shorts(
    coin: str, window_sec: int
) -> Tuple[List[Dict], float, float]:
    """
    Return (top_sellers_list, total_net_short_usd_raw, total_net_short_usd_weighted)
    - Consider taker SELL prints within window, notional >= HL_MIN_WHALE_T
    - Attribute to seller address (when available)
    - Apply watchlist weights
    """
    ts_now = _now()
    per_user_raw: Dict[str, Tuple[float, int]] = defaultdict(lambda: (0.0, 0))
    per_user_wt: Dict[str, float] = defaultdict(float)
    labels: Dict[str, str] = {}

    with _lock:
        st = _STATE[coin]
        # prune old trades
        while st.trades and ts_now - st.trades[0].ts > window_sec:
            st.trades.popleft()

        for tr in st.trades:
            if tr.coin != coin or tr.side != "S":
                continue
            if tr.notional < HL_MIN_WHALE_T:
                continue
            who = tr.seller or "unknown"
            raw = tr.notional
            label, w = _address_label(who)
            per_user_raw[who] = (per_user_raw[who][0] + raw, per_user_raw[who][1] + 1)
            per_user_wt[who] += raw * (w if w > 0 else 1.0)
            if label:
                labels[who] = label

    # sort by weighted notional desc (fallback to raw)
    users = list(per_user_raw.keys())
    top = sorted(
        users,
        key=lambda u: (per_user_wt.get(u, 0.0), per_user_raw.get(u, (0.0, 0))[0]),
        reverse=True,
    )
    out_top: List[Dict] = []
    for u in top[:3]:
        raw_usd, prints = per_user_raw[u]
        wt_usd = per_user_wt.get(u, raw_usd)
        out_top.append({
            "user": u,
            "label": labels.get(u, ""),
            "prints": prints,
            "raw_notional_usd": raw_usd,
            "weighted_notional_usd": wt_usd,
            "weight": (wt_usd / max(1e-9, raw_usd)) if raw_usd > 0 else 1.0
        })

    total_raw = sum(v[0] for v in per_user_raw.values())
    total_wt  = sum(per_user_wt.values()) if per_user_wt else total_raw
    return out_top, total_raw, total_wt


def _score_unusual_short(coin: str) -> Tuple[float, dict]:
    """
    Composite score:
      0.35 * ΔOI_z
      0.35 * (WhaleNetShortUSD_weighted / HL_WHALE_UNIT)
      0.20 * max(0, -Premium_z)
      0.10 * reserved (e.g., cross-venue funding divergence)
    """
    _maybe_reload_watchlist()

    window = HL_WINDOW_SEC
    oi_z   = max(0.0, _oi_change_z(coin, window))
    prem_z = _premium_z(coin, window)   # negative => bearish; we use -prem_z
    top3, whale_raw, whale_wt = _aggregate_whale_shorts(coin, window)
    skew  = _book_skew(coin)

    score = (
        0.35 * oi_z +
        0.35 * (whale_wt / max(1.0, HL_WHALE_UNIT)) +
        0.20 * max(0.0, -prem_z) +
        0.10 * 0.0
    )

    details = {
        "window_sec": window,
        "oi_z": oi_z,
        "premium_z": prem_z,
        "book_skew": skew,
        "whale_net_short_usd_5m_raw": whale_raw,
        "whale_net_short_usd_5m_weighted": whale_wt,
        "top_sellers": top3,
        "watchlist_size": len(_watchlist),
        "params": {
            "HL_MIN_WHALE_TRADE_USD": HL_MIN_WHALE_T,
            "HL_WHALE_UNIT_USD": HL_WHALE_UNIT,
            "HL_SCORE_TRIGGER": HL_SCORE_TRIG,
            "HL_BOOK_SKEW_TRIGGER": HL_SKEW_TRIG,
        }
    }

    if DEBUG_METRICS:
        logger.info(
            f"[HL/SCORE] {coin} score={score:.3f} oi_z={oi_z:.2f} "
            f"prem_z={prem_z:.2f} skew={skew:.2f} "
            f"whale_raw={whale_raw:.0f} whale_wt={whale_wt:.0f}"
        )

    return float(score), details


# ---------- Public API for your scheduler ----------
def collect_events(max_events: Optional[int] = None) -> List[dict]:
    """
    Compute alerts *now* from rolling state and return normalized event dicts.
    Call this from a scheduled job (e.g., every 60s).
    """
    ensure_started()
    max_out = int(max_events or HL_MAX_EVENTS)
    out: List[dict] = []
    ts_now = _now()

    for coin in HL_COINS:
        try:
            score, details = _score_unusual_short(coin)
            skew = details["book_skew"]
            whale_wt = details["whale_net_short_usd_5m_weighted"]

            # Gate: require score + skew + non-trivial whale flow
            if score >= HL_SCORE_TRIG and skew < HL_SKEW_TRIG and whale_wt >= HL_MIN_WHALE_T:
                with _lock:
                    st = _STATE[coin]
                    # avoid spam: one alert per coin per 2 minutes
                    if ts_now - st.last_alert_ts < 120:
                        continue
                    st.last_alert_ts = ts_now

                # Title + summary
                top3 = details["top_sellers"]

                def _fmt_entry(t):
                    tag = f"{(t.get('user') or '')[:10]}…"
                    if t.get("label"):
                        tag += f" ({t['label']})"
                    amt = int(t.get("weighted_notional_usd", 0))
                    return f"{tag} ${amt:,}"

                top_line = ", ".join([_fmt_entry(t) for t in top3]) or "N/A"
                title = (f"Hyperliquid {coin}: unusual SHORT flow — {top_line} | "
                         f"OI z={details['oi_z']:.2f}, prem z={details['premium_z']:.2f}, "
                         f"skew(b/a)={details['book_skew']:.2f}")

                ev = {
                    "source": "hyperliquid",
                    "kind": "short_flow_alert",
                    "symbol": coin,
                    "detected_at": _now_iso(),
                    "title": title,
                    "score": float(round(score, 3)),
                    "link": None,  # TODO: add explorer/leaderboard link if you maintain mapping
                    "payload": details,
                    "fingerprint": _fp(["hl", coin, str(int(ts_now // 60)), str(int(whale_wt))]),
                    "version": "1.2",
                }
                out.append(ev)
                if len(out) >= max_out:
                    break

        except Exception as e:
            logger.exception(f"[HL] compute error for {coin}: {e}")

    return out

