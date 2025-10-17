"""
feeds/truth.py
--------------
Fetch latest posts from a Truth Social profile using Playwright (headless Chromium).
- Reuses a storage_state JSON so you don't have to log in every run.
- If login credentials are provided via env, it will log in once and save cookies/session.

Env:
  TRUTH_USERNAME           # the @username to scrape, e.g. "realDonaldTrump" (without @)
  TRUTH_LIMIT              # max posts to emit per run (default 5)
  TRUTH_STATE_PATH         # path to cookie/session state file (default: .truth_state.json)
  TRUTH_LOGIN_USER         # optional: email/username to log in
  TRUTH_LOGIN_PASS         # optional: password

Notes:
- Site markup can change. Selectors below try multiple fallbacks.
- Be gentle with cadence; respect site terms & rate limits.
"""

import os
import time
import hashlib
from typing import List, Dict, Any, Optional

import backoff
from loguru import logger
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError


def _fp(*parts: str) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update((p or "").encode("utf-8"))
    return h.hexdigest()[:16]


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


async def _login_if_needed(page, state_path: str) -> None:
    """
    If TRUTH_LOGIN_USER/PASS provided and we don't have a saved state, attempt login once.
    Saves storage_state to disk for reuse.
    """
    user = os.getenv("TRUTH_LOGIN_USER")
    pw   = os.getenv("TRUTH_LOGIN_PASS")
    if not (user and pw):
        return  # no creds, skip login

    # Heuristic: if already logged in (presence of avatar/menu), skip
    try:
        await page.goto("https://truthsocial.com/home", wait_until="domcontentloaded", timeout=30000)
        # Look for a "Log in" link; if not found, assume logged in
        login_links = await page.locator("a[href*='/login']").all()
        if not login_links:
            logger.info("Truth: seems already logged in; skipping login flow.")
            return
    except Exception:
        pass

    logger.info("Truth: attempting login…")
    await page.goto("https://truthsocial.com/login", wait_until="domcontentloaded", timeout=45000)

    # Try common selectors (sites change; we try multiple)
    # Email/username field
    candidates_user = [
        "input[type='email']",
        "input[name='email']",
        "input[placeholder*='Email']",
        "input[placeholder*='email']",
        "input[name='username']",
        "input[placeholder*='Username']",
    ]
    # Password field
    candidates_pw = [
        "input[type='password']",
        "input[name='password']",
        "input[placeholder*='Password']",
    ]
    # Submit button
    candidates_submit = [
        "button[type='submit']",
        "button:has-text('Log in')",
        "button:has-text('Sign in')",
        "button:has-text('Login')",
        "button:has-text('Log In')",
    ]

    # Fill user
    filled = False
    for sel in candidates_user:
        if await page.locator(sel).count():
            await page.fill(sel, user)
            filled = True
            break
    if not filled:
        logger.warning("Truth: could not find username/email field.")

    # Fill password
    filled = False
    for sel in candidates_pw:
        if await page.locator(sel).count():
            await page.fill(sel, pw)
            filled = True
            break
    if not filled:
        logger.warning("Truth: could not find password field.")

    # Click submit
    clicked = False
    for sel in candidates_submit:
        if await page.locator(sel).count():
            await page.click(sel)
            clicked = True
            break
    if not clicked:
        logger.warning("Truth: could not find login submit button; continuing unauthenticated.")
        return

    # Wait for navigation / logged-in UI
    try:
        await page.wait_for_load_state("networkidle", timeout=45000)
    except PWTimeoutError:
        pass

    # Save cookies/session
    try:
        await page.context.storage_state(path=state_path)
        logger.info(f"Truth: saved storage_state to {state_path}")
    except Exception as e:
        logger.warning(f"Truth: could not save storage_state: {e}")


def _extract_posts_html(html: str, username: str, limit: int) -> List[Dict[str, Any]]:
    """
    Parse the rendered HTML to extract posts.
    We look for <article> blocks and try to grab content, timestamp, and a permalink.
    """
    soup = BeautifulSoup(html, "lxml")
    results: List[Dict[str, Any]] = []

    # Very generic: Truth/Mastodon-like frontends often render posts in <article>
    articles = soup.select("article")
    # Fallback: sometimes posts are in divs with role/list semantics
    if not articles:
        articles = soup.select("div[role='article'], div.status")

    for a in articles[: limit or 5]:
        # Content: try common containers
        text = ""
        # Try content containers by common classes/tags
        content_candidates = [
            ".status__content", ".post__content", "div[class*='content']",
            "div[dir='auto']", "p"
        ]
        for sel in content_candidates:
            node = a.select_one(sel)
            if node and node.get_text(strip=True):
                text = node.get_text(" ", strip=True)
                break
        if not text:
            # Last resort: use article text
            text = a.get_text(" ", strip=True)[:280]

        # Timestamp
        ts = None
        t = a.select_one("time")
        if t and t.has_attr("datetime"):
            ts = t["datetime"]

        # Permalink: look for anchors pointing to "/@username/" style or "/posts/"
        link = None
        for href_node in a.select("a[href]"):
            href = href_node["href"]
            if "/posts/" in href or f"/@{username}/" in href:
                link = href
                break
        if link and link.startswith("/"):
            link = f"https://truthsocial.com{link}"

        # Build normalized event
        ev = {
            "source": "truthsocial",
            "kind": "post",
            "symbol": None,
            "detected_at": _now_iso(),
            "title": (text or "Truth post").strip()[:200],
            "score": 0.0,  # you can compute a score later (e.g., recency, length)
            "link": link,
            "payload": {
                "username": username,
                "text": text,
                "timestamp": ts,
            },
            "fingerprint": _fp("truth", username, (link or text)[:64]),
            "version": "1.0",
        }
        results.append(ev)

    return results


@backoff.on_exception(backoff.expo, Exception, max_time=120)
async def fetch_truth_posts(username: Optional[str] = None, limit: int = 5) -> List[Dict[str, Any]]:
    """
    Render the Truth Social profile and return normalized posts.
    """
    username = (username or os.getenv("TRUTH_USERNAME") or "realDonaldTrump").lstrip("@")
    limit    = int(os.getenv("TRUTH_LIMIT", str(limit or 5)))
    state_path = os.getenv("TRUTH_STATE_PATH", ".truth_state.json")

    url = f"https://truthsocial.com/@{username}"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = await browser.new_context()  # optionally pass storage_state=state_path if exists

        # Reuse cookies if state file exists
        if os.path.exists(state_path):
            try:
                await context.close()
                context = await browser.new_context(storage_state=state_path)
                logger.info(f"Truth: loaded storage_state from {state_path}")
            except Exception as e:
                logger.warning(f"Truth: failed to load storage_state: {e}")
                context = await browser.new_context()

        page = await context.new_page()

        # If we don't have a state yet and creds exist, try login
        if not os.path.exists(state_path):
            await _login_if_needed(page, state_path)

        # Go to profile
        logger.info(f"Truth: navigating to {url}")
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=45000)
            # Let dynamic content load
            await page.wait_for_load_state("networkidle", timeout=45000)
        except PWTimeoutError:
            logger.warning("Truth: timeout while loading profile; proceeding with whatever rendered.")

        # Optionally scroll to load more posts
        try:
            for _ in range(2):
                await page.mouse.wheel(0, 2000)
                await page.wait_for_timeout(1200)
        except Exception:
            pass

        # Get rendered HTML
        html = await page.content()

        # Clean up browser
        await context.close()
        await browser.close()

    events = _extract_posts_html(html, username=username, limit=limit)
    logger.info(f"Truth: extracted {len(events)} posts for @{username}")
    return events
