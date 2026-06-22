"""
Deep per-channel health probe. Runs the full playback chain that the
device actually exercises:

    1. Resolve via donis — pick the first daddyN endpoint that returns
       a base64'd master URL.
    2. GET the master playlist.
    3. Follow the master's first variant into the inner playlist.
    4. GET the first listed segment; require the bytes start with 0x47
       (the TS sync byte), which proves the CDN is actually serving
       media, not an HTML error page or a 410 Gone wrapper.

The existing scraper's `probe_live` only checks steps 2–3 and skips the
segment fetch. Channels marked ok there frequently 410 at the segment
layer, which is what the user reported as "some channels fail to play
after some time".

Writes `data/health.json` with one entry per channel:
    {
      "id": "303",
      "name": "amc usa",
      "checked_at": 1781600000,
      "status": "ok" | "down" | "unreachable",
      "fail_reason": null | "resolve" | "master:500" | "inner:410" |
                     "segment:410" | "segment:bad-sync" | "exception:...",
      "daddy_endpoint": "daddy3.php",
      "host": "pontos.phantemlis.top",
      "first_segment_url": "..."
    }

Designed to run from GitHub Actions on a 15 min cron. Uses a ThreadPool
of [WORKERS] to fit ~750 channels into the 15 min budget; each channel
takes ~3-5 s when healthy, ~5-15 s on timeout.
"""

import argparse
import base64
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import requests

ROOT = Path(__file__).parent.parent
DATA = ROOT / "data"

CHROME_UA = (
    "Mozilla/5.0 (Linux; Android 12) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"
)

DADDY_BASE = "https://donis.jimpenopisonline.online/premiumtv/daddy{suf}.php?id={id}"
DADDY_SUFFIXES = ["", "2", "3", "4", "5"]

B64_RE = re.compile(r"window\.atob\(\s*['\"]([A-Za-z0-9+/=]+)['\"]\s*\)")

# Per-channel timeouts. Tight on purpose — we'd rather mark a channel
# "down" with a fast probe than have one slow donis hold up the whole
# sweep. The device has its own ensureCached 7 s ceiling anyway.
TIMEOUT_RESOLVE = 6
TIMEOUT_PLAYLIST = 5
TIMEOUT_SEGMENT = 7

WORKERS = 20


def session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = CHROME_UA
    return s


def _try_endpoint(
    s: requests.Session, cid: str, suf: str,
) -> tuple[Optional[str], Optional[requests.Response]]:
    """Returns (master_url, master_response) or (None, None).
    The master response is included so the caller can decide whether the
    master itself was usable — donis may return a base64 URL pointing at
    a sibling that's currently 500ing, and we need to keep iterating
    rather than declare the channel down on the first such miss.
    """
    try:
        r = s.get(
            DADDY_BASE.format(suf=suf, id=cid),
            headers={"Referer": f"https://dlhd.pk/stream/stream-{cid}.php"},
            timeout=TIMEOUT_RESOLVE,
        )
    except requests.RequestException:
        return None, None
    if r.status_code != 200:
        return None, None
    m = B64_RE.search(r.text)
    if not m:
        return None, None
    try:
        master = base64.b64decode(m.group(1)).decode("utf-8", errors="replace")
    except Exception:
        return None, None
    if ".m3u8" not in master:
        return None, None
    try:
        rm = s.get(master, timeout=TIMEOUT_PLAYLIST)
    except requests.RequestException:
        return master, None
    return master, rm


def resolve(
    s: requests.Session, cid: str, preferred_suffix: Optional[str] = None,
) -> tuple[Optional[str], Optional[str], Optional[requests.Response]]:
    """Returns (master_url, daddy_endpoint, master_response). Iterates
    daddy siblings until we find one whose master fetch comes back 200
    + #EXTM3U — donis often hands out a base64 URL pointing at a
    currently-500ing sibling, and we have to walk past those to find
    the one phantemlis is actually serving for this channel. Respects
    the catalog's last-known-good [preferred_suffix] by trying it
    first.
    """
    suffixes: list[str] = list(DADDY_SUFFIXES)
    if preferred_suffix:
        # Strip "daddy" prefix + ".php" suffix to get just the numeric
        # tail (e.g. "daddy3.php" -> "3", "daddy.php" -> "").
        norm = preferred_suffix.replace("daddy", "").replace(".php", "")
        if norm in suffixes:
            suffixes.remove(norm)
            suffixes.insert(0, norm)
    last_master: Optional[str] = None
    last_resp: Optional[requests.Response] = None
    last_suf: Optional[str] = None
    for suf in suffixes:
        master, rm = _try_endpoint(s, cid, suf)
        if not master:
            continue
        last_master, last_resp, last_suf = master, rm, suf
        if rm is not None and rm.ok and (rm.text or "").lstrip().startswith("#EXTM3U"):
            return master, f"daddy{suf}.php", rm
    # No working sibling. Return what we last tried so the caller can
    # report a precise failure reason.
    return last_master, (f"daddy{last_suf}.php" if last_suf else None), last_resp


def probe_channel(ch: dict) -> dict:
    """Full chain probe for one channel."""
    cid = str(ch.get("id", ""))
    name = ch.get("name", "")
    result = {
        "id": cid,
        "name": name,
        "checked_at": int(time.time()),
        "status": "down",
        "fail_reason": None,
        "daddy_endpoint": None,
        "host": None,
        "first_segment_url": None,
    }
    s = session()

    # 1. Resolve via donis. resolve() walks all 5 siblings until one
    # returns a master that fetches 200 + #EXTM3U. Catalog's known-good
    # daddy_endpoint is tried first to save iterations on healthy
    # channels.
    master_url, daddy, rm = resolve(s, cid, preferred_suffix=ch.get("daddy_endpoint"))
    if not master_url:
        result["fail_reason"] = "resolve"
        return result
    result["daddy_endpoint"] = daddy
    result["host"] = master_url.split("/")[2] if "//" in master_url else None
    if rm is None:
        result["fail_reason"] = "master:no-response"
        return result
    if not rm.ok:
        result["fail_reason"] = f"master:{rm.status_code}"
        return result
    body = rm.text or ""
    if not body.lstrip().startswith("#EXTM3U"):
        result["fail_reason"] = "master:not-m3u8"
        return result

    # 3. Inner playlist (first variant).
    inner_rel = next(
        (ln.strip() for ln in body.splitlines() if ln and not ln.startswith("#")),
        None,
    )
    if not inner_rel:
        # Master already contains segments (no variant indirection).
        inner_url = master_url
        inner_body = body
    else:
        inner_url = urljoin(master_url, inner_rel)
        try:
            ri = s.get(inner_url, timeout=TIMEOUT_PLAYLIST)
        except requests.RequestException as e:
            result["fail_reason"] = f"inner:{type(e).__name__}"
            return result
        if not ri.ok:
            result["fail_reason"] = f"inner:{ri.status_code}"
            return result
        inner_body = ri.text or ""
        if not inner_body.lstrip().startswith("#EXTM3U"):
            result["fail_reason"] = "inner:not-m3u8"
            return result

    # 4. First segment — partial fetch + check TS sync byte (0x47).
    seg_rel = next(
        (ln.strip() for ln in inner_body.splitlines() if ln and not ln.startswith("#")),
        None,
    )
    if not seg_rel:
        result["fail_reason"] = "inner:no-segments"
        return result
    seg_url = urljoin(inner_url, seg_rel)
    result["first_segment_url"] = seg_url
    try:
        # Range request — only need first 188 bytes (one TS packet) to
        # validate sync. Saves bandwidth across 750 channels per sweep.
        rs = s.get(
            seg_url,
            headers={"Range": "bytes=0-187"},
            timeout=TIMEOUT_SEGMENT,
            stream=True,
        )
    except requests.RequestException as e:
        result["fail_reason"] = f"segment:{type(e).__name__}"
        return result
    if not rs.ok:
        result["fail_reason"] = f"segment:{rs.status_code}"
        rs.close()
        return result
    first = next(rs.iter_content(chunk_size=4), b"")
    rs.close()
    if not first.startswith(b"\x47"):
        result["fail_reason"] = "segment:bad-sync"
        return result

    result["status"] = "ok"
    return result


def load_channels(path: Path) -> list[dict]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    chans = raw.get("channels", raw) if isinstance(raw, dict) else raw
    # Only probe channels the scraper already considers "ok" — there's
    # no point spending time on ones already marked down/unreachable
    # by the catalog. The catalog's broader pass catches those.
    return [c for c in chans if c.get("status") == "ok"]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--channels", default=str(DATA / "channels.json"))
    ap.add_argument("--out", default=str(DATA / "health.json"))
    ap.add_argument("--workers", type=int, default=WORKERS)
    ap.add_argument(
        "--limit",
        type=int,
        default=0,
        help="0 = sweep every ok channel; set for a faster smoke run",
    )
    args = ap.parse_args()

    channels = load_channels(Path(args.channels))
    if args.limit:
        channels = channels[: args.limit]
    print(f"sweep: probing {len(channels)} ok-marked channels", flush=True)

    started = time.time()
    results: list[dict] = []
    ok_count = 0
    fail_count = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(probe_channel, c): c for c in channels}
        for fut in as_completed(futures):
            r = fut.result()
            results.append(r)
            if r["status"] == "ok":
                ok_count += 1
            else:
                fail_count += 1
            done = ok_count + fail_count
            if done % 50 == 0:
                elapsed = int(time.time() - started)
                print(
                    f"  {done}/{len(channels)} done — ok={ok_count} "
                    f"fail={fail_count} ({elapsed} s elapsed)",
                    flush=True,
                )

    elapsed = int(time.time() - started)
    print(
        f"sweep finished in {elapsed} s — ok={ok_count} fail={fail_count}",
        flush=True,
    )

    # Sort by id for stable diffs in the committed health.json.
    results.sort(key=lambda r: int(r["id"]) if r["id"].isdigit() else 0)
    payload = {
        "swept_at": int(time.time()),
        "channels_swept": len(channels),
        "ok_count": ok_count,
        "fail_count": fail_count,
        "elapsed_seconds": elapsed,
        "results": results,
    }
    Path(args.out).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"wrote {args.out}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
