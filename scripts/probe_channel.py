"""
Quick diagnostic: load one dlhd.pk player wrapper page in headless Chromium and
dump everything that happens - frames, stream-related responses, console errors,
plus a full-page screenshot.

Use to investigate why a specific channel's iframe doesn't play.

Usage:
  python scripts/probe_channel.py 206
  python scripts/probe_channel.py 206 --path cast        # cast=Player 2 (default: cast)
  python scripts/probe_channel.py 206 --wait 30          # seconds to watch network
  python scripts/probe_channel.py 206 --headed          # show the browser window
"""
import argparse
import json
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).parent.parent  # project root (one up from scripts/)
OUT_DIR = ROOT / "data"
OUT_DIR.mkdir(exist_ok=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("cid", help="channel id (numeric, as listed on dlhd.pk)")
    ap.add_argument("--path", default="cast",
                    choices=["stream", "cast", "watch", "plus", "casting", "player"],
                    help="which dlhd.pk player wrapper to probe (default: cast = Player 2)")
    ap.add_argument("--wait", type=int, default=20,
                    help="seconds to keep the page open while observing network")
    ap.add_argument("--headed", action="store_true", help="show the browser window")
    args = ap.parse_args()

    url = f"https://dlhd.pk/{args.path}/stream-{args.cid}.php"
    print(f"Loading {url}")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=not args.headed)
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 720},
        )
        requests_log: list[dict] = []
        responses_log: list[dict] = []
        console_log: list[str] = []
        errors_log: list[str] = []

        def on_request(req):
            requests_log.append({
                "url": req.url, "method": req.method,
                "type": req.resource_type,
                "referer": req.headers.get("referer"),
            })

        def on_response(resp):
            u = resp.url.lower()
            if any(k in u for k in (".m3u8", ".mpd", "/stream-", "/daddy", "manifest", "hls")):
                responses_log.append({"url": resp.url, "status": resp.status})

        def on_console(msg):
            if msg.type in ("error", "warning"):
                console_log.append(f"[{msg.type}] {msg.text}")

        def on_page_error(exc):
            errors_log.append(str(exc))

        page = ctx.new_page()
        page.on("request", on_request)
        page.on("response", on_response)
        page.on("console", on_console)
        page.on("pageerror", on_page_error)

        page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        print(f"Page loaded, waiting {args.wait}s for player...")
        page.wait_for_timeout(args.wait * 1000)

        print("\n=== FRAME TREE ===")
        def dump_frames(frame, depth=0):
            print("  " * depth + f"- {(frame.url or '(blank)')[:140]}")
            for child in frame.child_frames:
                dump_frames(child, depth + 1)
        dump_frames(page.main_frame)

        print("\n=== VIDEO ELEMENTS (any frame) ===")
        for frame in page.frames:
            try:
                vids = frame.evaluate("""() => [...document.querySelectorAll('video')].map(v => ({
                  src: v.src || v.currentSrc || '(no src)',
                  paused: v.paused, readyState: v.readyState,
                  videoWidth: v.videoWidth, videoHeight: v.videoHeight,
                  error: v.error ? v.error.code + ':' + v.error.message : null,
                }))""")
                for v in vids:
                    print(f"  [{frame.url[:60]}]  {json.dumps(v, ensure_ascii=False)}")
            except Exception:
                pass

        print("\n=== STREAM-RELATED RESPONSES ===")
        seen = set()
        for r in responses_log:
            if r["url"] in seen: continue
            seen.add(r["url"])
            print(f"  {r['status']}  {r['url'][:140]}")
        if not responses_log:
            print("  (none)")

        print("\n=== .m3u8 REQUESTS (with headers) ===")
        m3u8 = [r for r in requests_log if ".m3u8" in r["url"].lower()]
        for r in m3u8:
            print(f"  {r['method']} {r['url'][:150]}")
            if r.get("referer"):
                print(f"      referer: {r['referer']}")
        if not m3u8:
            print("  (none)")

        if console_log:
            print(f"\n=== CONSOLE warnings/errors ({len(console_log)}) ===")
            for line in console_log[-15:]:
                print(f"  {line[:200]}")

        if errors_log:
            print(f"\n=== PAGE ERRORS ({len(errors_log)}) ===")
            for e in errors_log[-5:]:
                print(f"  {e[:200]}")

        shot = OUT_DIR / f"_probe_{args.cid}_{args.path}.png"
        page.screenshot(path=str(shot), full_page=True)
        print(f"\nscreenshot: {shot}")
        browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
