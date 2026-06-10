"""
Headless-browser stream resolver for DaddyLive channels.

Drives Chromium via Playwright to load dlhd.pk's player page for a channel,
then captures every .m3u8 request the player actually fires AND the headers
(Referer, Origin, User-Agent, etc.) it attaches. Real headers, real URLs.

Why: the static scraper only knows the donis.* backend (Player 1). When that
returns 500 the channel marks "down" - but dlhd.pk's site silently falls over
to Player 2/4/5/6 in a real browser. This script reproduces that fallback by
running a real browser and recording the m3u8 + headers it gets, per channel.

Usage:
  python scripts/headless_resolve.py 657                    # one channel, verbose
  python scripts/headless_resolve.py 657 51 302             # multiple
  python scripts/headless_resolve.py --all-down             # every channel with status=down
  python scripts/headless_resolve.py 657 --paths stream cast watch    # specific slots
  python scripts/headless_resolve.py 657 --headed          # show the browser window

Per channel it tries each player path until it captures at least one m3u8 that
returns 200 + manifest body. Records the working result into channels.json
under c["live_stream"] = {url, referer, origin, user_agent, captured_from}.
"""
import argparse
import json
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

ROOT = Path(__file__).parent.parent  # project root (one up from scripts/)
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
CHANNELS_JSON = DATA_DIR / "channels.json"

# dlhd.pk wrapper paths (we already mapped these in the static scraper)
ALL_PATHS = ["stream", "cast", "watch", "plus", "casting", "player"]

# How long to wait for the player to emit an m3u8 request per attempt
M3U8_WAIT_MS = 12_000


def resolve_one(page, cid: str, paths: list[str], verbose: bool) -> dict | None:
    """Try each player path; return the first capture whose m3u8 returns a 200 manifest."""
    captured: list[dict] = []

    def on_request(req):
        url = req.url
        if ".m3u8" not in url.lower():
            return
        h = req.headers
        captured.append({
            "url": url,
            "method": req.method,
            "referer": h.get("referer"),
            "origin": h.get("origin"),
            "user_agent": h.get("user-agent"),
            "resource_type": req.resource_type,
        })

    page.on("request", on_request)

    for path in paths:
        target = f"https://dlhd.pk/{path}/stream-{cid}.php"
        captured.clear()
        if verbose:
            print(f"  [{path:8s}] -> {target}")
        try:
            # 'domcontentloaded' is fast and the player's JS will keep running after.
            page.goto(target, wait_until="domcontentloaded", timeout=20_000)
        except PWTimeout:
            if verbose:
                print(f"  [{path:8s}] page nav timeout")
            continue
        # Give the player up to M3U8_WAIT_MS to fire an m3u8 request.
        end = time.monotonic() + M3U8_WAIT_MS / 1000
        while time.monotonic() < end and not captured:
            page.wait_for_timeout(250)
        if not captured:
            if verbose:
                print(f"  [{path:8s}] no m3u8 fired within {M3U8_WAIT_MS}ms")
            continue
        # Validate the first capture really plays.
        first = captured[0]
        try:
            r = page.request.get(
                first["url"],
                headers={
                    k: v for k, v in {
                        "Referer": first["referer"],
                        "Origin":  first["origin"],
                        "User-Agent": first["user_agent"],
                    }.items() if v
                },
                timeout=15_000,
            )
            body = r.text()
            if r.status == 200 and body.lstrip().startswith("#EXTM3U"):
                if verbose:
                    print(f"  [{path:8s}] PLAYABLE: {first['url']}")
                    print(f"             referer={first['referer']}  origin={first['origin']}")
                return {
                    "stream_url": first["url"],
                    "referer": first["referer"],
                    "origin": first["origin"],
                    "user_agent": first["user_agent"],
                    "captured_from": f"https://dlhd.pk/{path}/stream-{cid}.php",
                    "captured_via": "headless-chromium",
                }
            if verbose:
                print(f"  [{path:8s}] captured but HTTP {r.status} (not playable)")
        except Exception as e:
            if verbose:
                print(f"  [{path:8s}] validation error: {e}")
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("ids", nargs="*", help="channel ids to resolve")
    ap.add_argument("--all-down", action="store_true",
                    help="resolve every channel whose status is 'down' in channels.json")
    ap.add_argument("--paths", nargs="+", default=ALL_PATHS,
                    help="player paths to try, in order (default: all 6)")
    ap.add_argument("--headed", action="store_true",
                    help="show the browser window (default headless)")
    ap.add_argument("--save", action="store_true",
                    help="write captured live_stream into channels.json")
    ap.add_argument("--out", default="live_streams.json",
                    help="also dump captures to this file")
    args = ap.parse_args()

    ids: list[str] = list(args.ids)
    channels_data = []
    if args.all_down or args.save:
        if not CHANNELS_JSON.exists():
            print("channels.json missing - run scraper.py first", file=sys.stderr)
            return 2
        channels_data = json.loads(CHANNELS_JSON.read_text(encoding="utf-8"))
    if args.all_down:
        ids = [c["id"] for c in channels_data if c.get("status") == "down"]
        print(f"Resolving {len(ids)} 'down' channels...")
    if not ids:
        print("no channel ids given (use positional args or --all-down)", file=sys.stderr)
        return 2

    results: dict[str, dict | None] = {}
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=not args.headed)
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/131.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 720},
        )
        # block obvious popunder/ad scripts so the page doesn't redirect us away
        ctx.route("**/*", lambda route: (
            route.abort() if any(p in route.request.url for p in (
                "onclickprediction.com", "onclickalgo.com", "histats.com",
                "kefiricoxalate.com", "effectivecpmnetwork.com", "jnbhi.com",
                "fellfortunatepassive.com", "waust.at", "cloudflareinsights",
            )) else route.continue_()
        ))
        page = ctx.new_page()
        for cid in ids:
            print(f"\n[{cid}]")
            try:
                results[cid] = resolve_one(page, str(cid), args.paths, verbose=True)
            except Exception as e:
                print(f"  ERROR: {e}")
                results[cid] = None
        browser.close()

    (DATA_DIR / args.out).write_text(
        json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"\nWrote {args.out} ({sum(1 for v in results.values() if v)}/{len(results)} resolved)")

    if args.save and channels_data:
        by_id = {c["id"]: c for c in channels_data}
        n = 0
        for cid, cap in results.items():
            if cap and cid in by_id:
                by_id[cid]["live_stream"] = cap
                n += 1
        CHANNELS_JSON.write_text(
            json.dumps(channels_data, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        print(f"Wrote {n} live_stream entries back into channels.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
