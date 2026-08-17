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
import random
import re
import sys
import threading
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

DADDY_HOSTS = [
    # Authoritative resolver — captured by scraping dlhd.pk on
    # 2026-06-23. Update by appending; the dynamic discovery in
    # discover_host() picks up replacements without a code change as
    # long as dlhd's iframe page keeps writing the URL in plaintext.
    "hamis.romponalis.st",
    # Previously authoritative — kept as a last-resort fallback. NXDOMAIN
    # since 2026-06 so it'll fail fast and the next host wins.
    "donis.jimpenopisonline.online",
]
DADDY_PATH = "/premiumtv/daddy{suf}.php?id={id}"
DADDY_SUFFIXES = ["", "2", "3", "4", "5"]
HOST_DISCOVERY_RE = re.compile(
    r"https?://([a-z0-9.-]+)/premiumtv/daddy\d*\.php", re.IGNORECASE,
)

B64_RE = re.compile(r"window\.atob\(\s*['\"]([A-Za-z0-9+/=]+)['\"]\s*\)")

# Per-channel timeouts. Tight on purpose — we'd rather mark a channel
# "down" with a fast probe than have one slow donis hold up the whole
# sweep. The device has its own ensureCached 7 s ceiling anyway.
TIMEOUT_RESOLVE = 6
TIMEOUT_PLAYLIST = 5
TIMEOUT_SEGMENT = 7

# 4, not 20. At 20 the CDN rate-limits us: a full sweep came back
# ok=70/752 with 562 of the failures being HTTP 429 — i.e. we throttled
# ourselves and then recorded healthy channels as down. Measured on 40
# channels: 4 workers = 37 ok / 0 throttles, 8 = 32 ok / 3 throttles,
# 20 = mass 429. Slower, but the output is TRUE.
WORKERS = 4


def session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = CHROME_UA
    return s


def cdn_origin(url: str) -> str:
    """Origin/Referer pair a DADDY LIVE CDN demands for a signed URL.

    Cloudflare binds the signed-URL token to the ORIGIN of the wrapper page
    that minted it, not to dlhd — so a request presenting no Origin, or
    dlhd's, gets 403 even though the URL is perfectly valid. That is why
    every channel came back down (`master:403`, ok=0) while the app played
    the same hosts fine: the app maps each CDN family to its wrapper page and
    this script never did.

    Mirrors LiveStreamProxy.originFor() in the Android app — keep them in
    sync when a CDN family moves.
    """
    host = url.split("//", 1)[-1].split("/", 1)[0].split(":", 1)[0].lower()
    if host.endswith("phantemlis.top"):
        return "https://hamis.romponalis.st"
    if host.endswith("inproviszon.st"):
        return "https://ritzembeds.pages.dev"
    return "https://dlhd.st"


def cdn_headers(url: str, extra: Optional[dict] = None) -> dict:
    origin = cdn_origin(url)
    h = {"Referer": origin + "/", "Origin": origin}
    if extra:
        h.update(extra)
    return h


def cdn_get(s: requests.Session, url: str, headers: dict, timeout: int, **kw):
    """GET that RETRIES a 429 instead of accepting it as a verdict.

    A 429 says the CDN is throttling us, not that the channel is broken —
    so treating it as an answer produces false "down" marks (a CI run
    recorded 155 of them in one sweep). Backs off and re-asks, honouring
    Retry-After when present. Returns the last response either way, so the
    caller still sees a 429 if the CDN really will not serve us.
    """
    delay = 1.5
    r = None
    for attempt in range(3):
        r = s.get(url, headers=headers, timeout=timeout, **kw)
        if r.status_code != 429:
            return r
        if attempt == 2:
            break
        wait = delay
        ra = r.headers.get("Retry-After")
        if ra:
            try:
                wait = min(float(ra), 8.0)
            except ValueError:
                pass
        # Jitter so 4 workers that get throttled together don't retry in
        # lockstep and throttle each other again.
        time.sleep(wait + random.uniform(0, 0.75))
        delay *= 2
    return r


def discover_host(s: requests.Session, cid: str) -> Optional[str]:
    """Scrape dlhd.pk/stream/stream-{cid}.php for the current resolver
    host. The page writes the daddyN URL in plaintext HTML (it's the
    page's own backend) so no JS decryption needed. Returns the
    discovered hostname, or None on scrape failure."""
    try:
        r = s.get(
            f"https://dlhd.st/stream/stream-{cid}.php",
            headers={"Referer": f"https://dlhd.st/watch.php?id={cid}"},
            timeout=TIMEOUT_RESOLVE,
        )
    except requests.RequestException:
        return None
    if r.status_code != 200:
        return None
    m = HOST_DISCOVERY_RE.search(r.text)
    return m.group(1) if m else None


# Process-wide cache: once a host works for one channel, prefer it for
# all subsequent resolves this run. The 757-channel sweep would
# otherwise re-scrape dlhd for every dead-host channel — wasteful.
_CACHED_HOST: Optional[str] = None
_CACHE_LOCK = threading.Lock()


def _hosts_to_try(s: requests.Session, cid: str) -> list[str]:
    """Resolver hosts in try-order: process cache, hardcoded list,
    then one-shot dynamic discovery as the safety net."""
    global _CACHED_HOST
    with _CACHE_LOCK:
        cached = _CACHED_HOST
    out: list[str] = []
    seen: set[str] = set()
    if cached and cached not in seen:
        out.append(cached); seen.add(cached)
    for h in DADDY_HOSTS:
        if h not in seen:
            out.append(h); seen.add(h)
    discovered = discover_host(s, cid)
    if discovered and discovered not in seen:
        out.append(discovered)
    return out


def _try_endpoint(
    s: requests.Session, host: str, cid: str, suf: str,
) -> tuple[Optional[str], Optional[requests.Response]]:
    """Returns (master_url, master_response) or (None, None).
    The master response is included so the caller can decide whether the
    master itself was usable — donis may return a base64 URL pointing at
    a sibling that's currently 500ing, and we need to keep iterating
    rather than declare the channel down on the first such miss.
    """
    url = "https://" + host + DADDY_PATH.format(suf=suf, id=cid)
    try:
        r = s.get(
            url,
            headers={"Referer": f"https://dlhd.st/stream/stream-{cid}.php"},
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
        rm = cdn_get(s, master, cdn_headers(master), TIMEOUT_PLAYLIST)
    except requests.RequestException:
        return master, None
    return master, rm


# dlhd.st exposes the SAME channel through several player wrappers, each
# routing to a different backend/CDN. Mirrors LiveResolver.PLAYER_PATHS in the
# Android app.
PLAYER_PATHS = ["stream", "cast", "watch", "plus", "casting", "player"]
IFRAME_RE = re.compile(r'<iframe[^>]+src="(https?://[^"]+)"', re.IGNORECASE)
M3U8_RE = re.compile(r"""https?://[^\s"']+\.m3u8[^\s"']*""", re.IGNORECASE)
ATOB_RE = re.compile(r"""atob\(["']([A-Za-z0-9+/=]+)["']\)""")


def try_player_path(
    s: requests.Session, cid: str, path: str,
) -> tuple[Optional[str], Optional[requests.Response]]:
    """dlhd.st/<path>/stream-<id>.php -> iframe -> backend page -> m3u8.

    The daddyN route below only reaches ONE CDN family. When that family
    rotates or goes down, every channel looks dead even though the app plays
    them fine off a different wrapper — verified 2026-08-17, when the sweep
    reported ok=268/752 while the app streamed ch 51 happily from
    volder.timst.cfd, a host the daddy route never returns.

    Mirrors LiveResolver.tryPlayerPath() in the Android app; the m3u8 is
    either atob-encoded (PLAYER 1/3) or sits literally in the page (PLAYER 4).
    """
    try:
        r = s.get(
            f"https://dlhd.st/{path}/stream-{cid}.php",
            headers={"Referer": "https://dlhd.st/"}, timeout=TIMEOUT_RESOLVE,
        )
        if not r.ok:
            return None, None
        m = IFRAME_RE.search(r.text)
        if not m:
            return None, None
        iframe = m.group(1)
        # An iframe pointing back at dlhd.st is a placeholder, not a backend.
        if "dlhd.st" in iframe:
            return None, None
        r2 = s.get(
            iframe, headers={"Referer": "https://dlhd.st/"},
            timeout=TIMEOUT_RESOLVE,
        )
        if not r2.ok:
            return None, None
        url = None
        a = ATOB_RE.search(r2.text)
        if a:
            try:
                cand = base64.b64decode(a.group(1)).decode("utf-8", "replace")
                if ".m3u8" in cand:
                    url = cand
            except Exception:  # noqa: BLE001
                url = None
        if url is None:
            m2 = M3U8_RE.search(r2.text)
            url = m2.group(0) if m2 else None
        if not url:
            return None, None
        rm = cdn_get(s, url, cdn_headers(url), TIMEOUT_PLAYLIST)
        return url, rm
    except requests.RequestException:
        return None, None


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
    for host in _hosts_to_try(s, cid):
        for suf in suffixes:
            master, rm = _try_endpoint(s, host, cid, suf)
            if not master:
                continue
            last_master, last_resp, last_suf = master, rm, suf
            if rm is not None and rm.ok and (rm.text or "").lstrip().startswith("#EXTM3U"):
                # Remember the working host so the rest of this sweep
                # doesn't pay the discovery cost again.
                global _CACHED_HOST
                with _CACHE_LOCK:
                    _CACHED_HOST = host
                return master, f"daddy{suf}.php", rm
    # Every daddy sibling failed. Before calling the channel down, try the
    # other dlhd.st player wrappers — they route to different CDNs entirely,
    # which is how the app keeps playing channels this route says are dead.
    for path in PLAYER_PATHS:
        master, rm = try_player_path(s, cid, path)
        if not master:
            continue
        last_master, last_resp, last_suf = master, rm, None
        if rm is not None and rm.ok and (rm.text or "").lstrip().startswith("#EXTM3U"):
            return master, f"player:{path}", rm
    # Nothing served this channel. Return what we last tried so the caller can
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
            ri = cdn_get(s, inner_url, cdn_headers(inner_url), TIMEOUT_PLAYLIST)
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
        rs = cdn_get(
            s, seg_url, cdn_headers(seg_url, {"Range": "bytes=0-187"}),
            TIMEOUT_SEGMENT, stream=True,
        )
        if not rs.ok:
            result["fail_reason"] = f"segment:{rs.status_code}"
            rs.close()
            return result
        # NOTE: the streaming read below can ALSO raise ReadTimeout /
        # ConnectionError — the timeout often fires while consuming the
        # body, not on connect. This must be inside the try, or one slow
        # segment CDN (romplovanis.shop has been flaky) crashes the
        # whole sweep via the ThreadPoolExecutor future.
        first = next(rs.iter_content(chunk_size=4), b"")
        rs.close()
    except requests.RequestException as e:
        result["fail_reason"] = f"segment:{type(e).__name__}"
        return result
    except Exception as e:  # noqa: BLE001 — never let a worker kill the sweep
        result["fail_reason"] = f"segment:err:{type(e).__name__}"
        return result
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
    throttled_count = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(probe_channel, c): c for c in channels}
        for fut in as_completed(futures):
            # Defensive: probe_channel should catch its own errors, but
            # if anything slips through, mark that ONE channel down
            # rather than letting fut.result() raise and abort the whole
            # sweep (which is what crashed run 28044638524 — a segment
            # read timeout propagated all the way up to sys.exit(main)).
            try:
                r = fut.result()
            except Exception as e:  # noqa: BLE001
                ch = futures[fut]
                r = {
                    "id": str(ch.get("id", "")),
                    "name": ch.get("name", ""),
                    "checked_at": int(time.time()),
                    "status": "down",
                    "fail_reason": f"worker:{type(e).__name__}",
                    "daddy_endpoint": None,
                    "host": None,
                    "first_segment_url": None,
                }
            # A 429 tells us nothing about the CHANNEL — it says the CDN
            # throttled US. Recording it as "down" is how a sweep publishes a
            # catalogue of false negatives (and how the app ends up badging
            # working channels as offline). Mark it unknown and drop it from
            # the payload so the previous verdict for that channel stands.
            # UNVERIFIABLE, not down.
            #
            # This script can only check ONE of the six player routes dlhd.st
            # exposes (the daddyN one). The other five hand back backends whose
            # URL is assembled in JavaScript, which the app resolves with a
            # headless WebView and a plain Python job cannot. So when our route
            # is unavailable — 429 (throttled), 5xx (that CDN family down) — it
            # says nothing about the channel: verified 2026-08-17, when this
            # route 503'd for ABC USA while the app streamed it happily from
            # volder.timst.cfd via a different wrapper.
            #
            # Mark those unknown and DROP them, so the previous verdict stands.
            # Only a definite negative from our own route (404, a served
            # playlist that is malformed, a dead segment) is recorded as down.
            reason = r.get("fail_reason") or ""
            unverifiable = (
                "429" in reason
                or ":503" in reason or ":502" in reason or ":504" in reason
            )
            if unverifiable:
                r["status"] = "unknown"
                throttled_count += 1
                continue
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
        f"sweep finished in {elapsed} s — ok={ok_count} fail={fail_count} "
        f"unverifiable={throttled_count}",
        flush=True,
    )

    # COLLAPSE GUARD. If this run says almost everything is down, the likely
    # cause is us (throttling, a resolver move, a network blip) rather than
    # the whole catalogue dying at once. Publishing that would badge working
    # channels as offline for every user. Refuse to overwrite a healthier
    # previous sweep and exit non-zero so the failure is visible.
    prev_ok = 0
    out_path = Path(args.out)
    if out_path.exists():
        try:
            prev = json.loads(out_path.read_text(encoding="utf-8"))
            prev_ok = int(prev.get("ok_count") or 0)
        except Exception:  # noqa: BLE001
            prev_ok = 0
    if throttled_count > len(channels) // 10:
        print(
            f"::warning::SKIPPING PUBLISH — {throttled_count} channels were "
            f"unverifiable (throttled, or our one CDN route 5xx'd). This run "
            f"measured our own reachability, not channel health. Keeping the "
            f"previous health.json.",
            flush=True,
        )
        # Exit 0, NOT 1. The job did exactly what it should: it detected that
        # it could not measure reliably and declined to overwrite good data.
        # Failing the build for that trains everyone to ignore a red sweep,
        # and hides a real breakage when one happens. The warning above is
        # visible on the run; staleness of health.json is the other signal.
        return 0
    if prev_ok >= 20 and ok_count < prev_ok // 2:
        print(
            f"::warning::SKIPPING PUBLISH — ok collapsed {prev_ok} -> "
            f"{ok_count}. Refusing to overwrite with a sweep that marks most "
            f"channels down; investigate before trusting this.",
            flush=True,
        )
        return 0

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
