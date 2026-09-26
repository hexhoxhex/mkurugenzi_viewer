"""
DaddyLive (dlhd.pk) channel + schedule scraper.

Walks the 3-hop chain for each channel:
  1. https://dlhd.pk/24-7-channels.php  -> (id, name) pairs
  2. (skipped - we know hop 3 directly)
  3. https://donis.jimpenopisonline.online/premiumtv/daddy3.php?id={id}
       -> contains  window.atob('<base64 m3u8 URL>')
Then GETs each resolved m3u8 once to mark status='ok'|'down'.

Also scrapes the live event schedule from https://dlhd.pk/ and matches
each event's listed channel ids back to our resolved catalog.

Emits:
  channels.json       - [{id, name, stream_url, status}]
  schedule.json       - [{category, time, title, channels:[{id,name}]}]
  playlist_new.m3u8   - standard M3U (live channels only by default)
  tester.html         - self-contained browser player (Channels + Schedule tabs)

Usage:
  python scraper.py                  # full run
  python scraper.py --limit 20       # quick smoke test
  python scraper.py --workers 30
  python scraper.py --schedule-only  # re-use cached channels.json, refresh schedule + tester
  python scraper.py --no-probe       # skip live/down probe (faster, no status badges)
  python scraper.py --include-down   # write down channels into the M3U too
"""
import argparse
import base64
import concurrent.futures
import datetime as _dt
import html as _html
import json
import re
import sys
import threading
import time
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
# How hard to try for the schedule before falling back to the stored one.
SCHEDULE_FETCH_ATTEMPTS = 3
SCHEDULE_RETRY_SLEEP_S = 3

HOME_URL = "https://dlhd.pk/"
CHANNELS_URL = "https://dlhd.pk/24-7-channels.php"
# The resolver host serves several daddyN endpoints, each routing to a
# different CDN node (daddy.php->pontos, daddy2->kolis, daddy3->vomos,
# daddy4->fomis, daddy5->zalis). dlhd rotates the RESOLVER HOST itself
# periodically (donis.jimpenopisonline.online went NXDOMAIN in 2026-06;
# the current one is hamis.romponalis.st). Rather than hard-pin a host
# that will die again, we keep a known-good list AND scrape the current
# host out of dlhd.pk's own stream page (which writes the daddyN URL in
# plaintext) — see discover_host(). Mirrors scripts/sweep_health.py.
DADDY_HOSTS = [
    "hamis.romponalis.st",            # current (captured 2026-06-23)
    "donis.jimpenopisonline.online",  # previous — NXDOMAIN, fails fast
]
DADDY_PATH = "/premiumtv/daddy{suf}.php?id={id}"
DADDY_SUFFIXES = ["", "2", "3", "4", "5"]
HOST_DISCOVERY_RE = re.compile(
    r"https?://([a-z0-9.-]+)/premiumtv/daddy\d*\.php", re.IGNORECASE,
)
# Once any host works, prefer it for the rest of the run (avoids
# re-scraping dlhd for every channel). Guarded for thread-safety since
# resolve_stream runs under a ThreadPoolExecutor.
_CACHED_HOST: str | None = None
_CACHE_LOCK = threading.Lock()

# Circuit breaker for the whole donis/daddy family.
#
# When hamis.romponalis.st started answering "403 - Access Denied" to us, every
# one of 899 channels still paid the full toll before giving up: 2 hosts x 5
# suffixes of dead requests, plus a dlhd host-discovery scrape, and only THEN
# the player wrappers that actually work. The run stopped being about resolving
# channels and became about waiting for a host that is never going to answer —
# it hit the 90-minute CI timeout and was cancelled, which is why the app had
# no channels to list.
#
# So: notice. After DONIS_FAIL_LIMIT channels in a row where no donis route
# produced anything, stop asking for the rest of the run and resolve through
# the player wrappers directly. Deliberately NOT a hardcoded "donis is dead":
# the counter resets the moment one succeeds, and every run starts with the
# breaker closed, so the day the host comes back it is used again with no
# code change.
DONIS_FAIL_LIMIT = 12
_DONIS_LOCK = threading.Lock()
_DONIS_FAILS = 0
_DONIS_DEAD = False


def _donis_open() -> bool:
    """False once the donis family has proved it is not answering."""
    with _DONIS_LOCK:
        return not _DONIS_DEAD


def _note_donis(ok: bool) -> None:
    global _DONIS_FAILS, _DONIS_DEAD
    with _DONIS_LOCK:
        if ok:
            if _DONIS_DEAD:
                print("      .. donis answered again — re-enabling it")
            _DONIS_FAILS = 0
            _DONIS_DEAD = False
            return
        _DONIS_FAILS += 1
        if not _DONIS_DEAD and _DONIS_FAILS >= DONIS_FAIL_LIMIT:
            _DONIS_DEAD = True
            print(
                f"      !! donis gave nothing for {_DONIS_FAILS} channels in a"
                " row — skipping it for the rest of this run and resolving"
                " through the player wrappers instead"
            )

ROOT = Path(__file__).parent
DATA_DIR = ROOT / "data"
SCRIPTS_DIR = ROOT / "scripts"
TESTER_TEMPLATE = SCRIPTS_DIR / "tester_template.html"
LOGOS_SOURCE = ROOT / "all_channels" / "playlist.m3u8"  # original repo's M3U; source of logos

DATA_DIR.mkdir(exist_ok=True)
SCRIPTS_DIR.mkdir(exist_ok=True)

SESSION = requests.Session()
# Cloudflare on dlhd.pk fingerprints UA-only requests as bots and 1020s them.
# Sending the full set of browser-like headers gets us through.
SESSION.headers.update({
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Upgrade-Insecure-Requests": "1",
})
# Retry transient network failures with exponential backoff. Without this,
# a single TCP timeout aborts the whole scrape.
_retry = Retry(
    total=3, connect=3, read=2,
    backoff_factor=1.5,
    status_forcelist=[502, 503, 504],
    allowed_methods=frozenset(["GET", "HEAD"]),
)
SESSION.mount("https://", HTTPAdapter(max_retries=_retry, pool_connections=50, pool_maxsize=50))
SESSION.mount("http://", HTTPAdapter(max_retries=_retry, pool_connections=50, pool_maxsize=50))


# ---------- channels ----------

def fetch_channel_list() -> list[dict]:
    r = SESSION.get(CHANNELS_URL, timeout=30)
    r.raise_for_status()
    pairs = re.findall(
        r'href="/watch\.php\?id=(\d+)"\s*data-title="([^"]+)"',
        r.text,
    )
    seen = set()
    out = []
    for cid, name in pairs:
        if cid in seen:
            continue
        seen.add(cid)
        out.append({"id": cid, "name": _html.unescape(name).strip()})
    return out


B64_RE = re.compile(r"window\.atob\(\s*['\"]([A-Za-z0-9+/=]+)['\"]\s*\)")


def discover_host(cid: str) -> str | None:
    """Scrape dlhd.pk/stream/stream-{cid}.php for the current resolver host.
    The page embeds the daddyN URL in plaintext HTML (it's the page's own
    backend) so no JS decoding is needed. Returns the hostname or None."""
    try:
        r = SESSION.get(
            f"https://dlhd.pk/stream/stream-{cid}.php",
            headers={"Referer": f"https://dlhd.pk/watch.php?id={cid}"},
            timeout=15,
        )
        if r.status_code != 200:
            return None
        m = HOST_DISCOVERY_RE.search(r.text)
        return m.group(1) if m else None
    except requests.RequestException:
        return None


def _hosts_to_try(cid: str) -> list[str]:
    """Known resolver hosts in order: process cache first, then the
    hardcoded list. Dynamic discovery is deliberately NOT done here —
    it's a last resort inside resolve_stream, called only when every
    known host fails. Scraping dlhd.pk for EVERY channel (the old
    behaviour) hammered the site from the CI runner's IP, which dlhd
    rate-limited — that's what made the later homepage schedule fetch
    time out and blank schedule.json."""
    with _CACHE_LOCK:
        cached = _CACHED_HOST
    out: list[str] = []
    seen: set[str] = set()
    for h in ([cached] if cached else []) + DADDY_HOSTS:
        if h and h not in seen:
            out.append(h)
            seen.add(h)
    return out


def _fetch_daddy_url(host: str, cid: str, suf: str) -> str | None:
    """Fetch one daddyN.php on [host] and extract the base64'd m3u8 URL."""
    try:
        r = SESSION.get(
            "https://" + host + DADDY_PATH.format(suf=suf, id=cid),
            headers={"Referer": f"https://dlhd.pk/stream/stream-{cid}.php"},
            timeout=15,
        )
        if r.status_code != 200:
            return None
        m = B64_RE.search(r.text)
        if not m:
            return None
        url = base64.b64decode(m.group(1)).decode("utf-8", errors="replace")
        return url if ".m3u8" in url else None
    except requests.RequestException:
        return None


def probe_live(url: str) -> str:
    """GET the m3u8 AND follow into its first variant. Returns 'ok' only if both
    the master and the inner chunk URL (typically tracks-v1a1/mono.m3u8) return
    200 with a real manifest body. Many channels in this catalog templating
    serve a valid master forever while the inner chunk URL has rolled over to
    HTTP 410 Gone — hls.js then bombs with levelLoadError. Probing the chunk
    catches that before the player gets the bad URL."""
    hdrs = _PLAYER_HEADERS.get(url)
    try:
        r = SESSION.get(url, headers=hdrs, timeout=15)
        body = r.text or ""
        if not (r.ok and body.lstrip().startswith("#EXTM3U")):
            return "down"
        # Find the first non-comment, non-blank line — that's the level URL.
        chunk_rel = next(
            (ln.strip() for ln in body.splitlines() if ln and not ln.startswith("#")),
            None,
        )
        if not chunk_rel:
            # Already a media playlist (no variants) — master itself was the
            # chunk list. Trust the 200.
            return "ok"
        if "#EXT-X-STREAM-INF" not in body:
            # A MEDIA playlist with segments listed directly. The line above is
            # a SEGMENT, not another playlist, so demanding #EXTM3U from it
            # marks every such channel "down" — which is most of the catalog
            # now that these routes serve media playlists. Check that the
            # segment SERVES rather than what it looks like: one route
            # disguises the container (measured: a segment whose first bytes
            # are "RIFF", not the TS sync byte), so sniffing would lie.
            from urllib.parse import urljoin as _urljoin
            try:
                rseg = SESSION.get(
                    _urljoin(url, chunk_rel), headers=hdrs, timeout=12, stream=True,
                )
                served = rseg.ok
                rseg.close()
            except requests.RequestException:
                served = False
            return "ok" if served else "down"
        from urllib.parse import urljoin
        chunk_url = urljoin(url, chunk_rel)
        r2 = SESSION.get(chunk_url, headers=hdrs, timeout=10)
        body2 = r2.text or ""
        if r2.ok and body2.lstrip().startswith("#EXTM3U"):
            return "ok"
        return "down"
    except requests.RequestException:
        return "down"


def _try_host(cid: str, host: str) -> tuple[tuple[str, str] | None, bool]:
    """Try all daddy suffixes on one host. Returns ((url, suf) or None,
    host_produced_any_base64)."""
    fallback = None
    host_worked = False
    for suf in DADDY_SUFFIXES:
        url = _fetch_daddy_url(host, cid, suf)
        if not url:
            continue
        host_worked = True
        if fallback is None:
            fallback = (url, suf)
        if probe_live(url) == "ok":
            return (url, suf), True
    return fallback, host_worked


# Wrapper pages, best-first. /plus was the only route that resolved in
# testing on 2026-09-20 (3 of 3 channels, epidd via the XOR shape); the rest
# are tried in turn because which one answers moves around.
PLAYER_PATHS_RESOLVE = ["plus", "watch", "casting", "stream", "hub", "cast"]

# dlhd.st redirects to whatever domain the site is on today, which is how
# the app reaches it too. HOME_URL still points at the old dlhd.pk name.
PLAYER_BASE = "https://dlhd.st"

_M3U8_DIRECT_RE = re.compile(
    r'https?://[a-z0-9.-]+/[^\s"\'<>\\]*\.m3u8[^\s"\'<>\\]*', re.IGNORECASE,
)
# epiembeds hides the URL in a number array: ((n ^ key) - salt + 256) & 255.
_XOR_RE = re.compile(r"=\s*\[([0-9,\s]{40,})\][^;]*?=\s*(\d+)\s*,\s*\w+\s*=\s*(\d+)")


# A stream resolved through a wrapper only serves with the embed page's
# Referer/Origin — bare requests get 403. probe_live would then call a
# perfectly good channel "down", which is how a working resolve still ends up
# as an empty catalog. Keyed by URL, bounded, written once at resolve time.
_PLAYER_HEADERS: dict[str, dict] = {}
_PLAYER_HEADERS_LOCK = threading.Lock()

# Longest one channel may spend walking wrappers. Measured 2026-09-20: a
# channel with no working route burned 210 s across six paths, which at 899
# channels is worse than the outage it fixes. A channel that has not answered
# in this long is not going to.
PLAYER_RESOLVE_BUDGET_S = 30.0


def _remember_headers(url: str, headers: dict) -> None:
    with _PLAYER_HEADERS_LOCK:
        if len(_PLAYER_HEADERS) > 4000:
            _PLAYER_HEADERS.clear()
        _PLAYER_HEADERS[url] = headers


def _player_get(url: str, referer: str, timeout: int = 10):
    try:
        r = SESSION.get(url, headers={"Referer": referer}, timeout=timeout)
        return r if r.ok else None
    except requests.RequestException:
        return None


def _decode_xor_array(body: str) -> str | None:
    m = _XOR_RE.search(body or "")
    if not m:
        return None
    try:
        nums = [int(x) for x in m.group(1).replace(" ", "").split(",") if x]
        key, salt = int(m.group(2)), int(m.group(3))
        out = "".join(chr(((n ^ key) - salt + 256) & 255) for n in nums)
        hit = _M3U8_DIRECT_RE.search(out.replace("\\/", "/"))
        return hit.group(0) if hit else None
    except Exception:
        return None


def _extract_stream(body: str) -> str | None:
    """The m3u8 out of an embed page, whichever way it is hidden.

    Three shapes seen in the wild: base64 inside atob(), a literal URL
    (sometimes JSON-escaped as https:\\/\\/), and an XOR-obfuscated array.
    """
    body = body or ""
    m = B64_RE.search(body)
    if m:
        try:
            url = base64.b64decode(
                m.group(1) + "=" * (-len(m.group(1)) % 4)
            ).decode("utf-8", "replace").replace("\\/", "/")
            if ".m3u8" in url:
                return url
        except Exception:
            pass
    hit = _M3U8_DIRECT_RE.search(body.replace("\\/", "/"))
    if hit:
        return hit.group(0)
    return _decode_xor_array(body)


def resolve_via_players(cid: str) -> str | None:
    """Resolve the way the APP does, through the player wrappers.

    The donis endpoints this scraper was built on now answer "403 - Access
    Denied" to us (plain nginx, no challenge page) and the secondary host no
    longer resolves at all, so a refresh resolved 0 of 899 channels and
    published a catalog with every status unset. The app stopped depending on
    donis long ago: it races these wrapper pages, and they still serve.

    Bounded on purpose. Each wrapper page is ~640 KB and the site starts
    refusing after sustained fetching, so this stops at the first route whose
    master playlist actually loads rather than collecting them all.
    """
    ref = f"{PLAYER_BASE}/watch.php?id={cid}"
    deadline = time.monotonic() + PLAYER_RESOLVE_BUDGET_S
    for path in PLAYER_PATHS_RESOLVE:
        if time.monotonic() > deadline:
            break
        page = _player_get(f"{PLAYER_BASE}/{path}/stream-{cid}.php", ref)
        if page is None:
            continue
        for frame in IFRAME_RE.findall(page.text)[:2]:
            if time.monotonic() > deadline:
                break
            inner = _player_get(frame, page.url)
            if inner is None:
                continue
            url = _extract_stream(inner.text)
            if not url:
                # One nested hop; some wrappers iframe another embed.
                for nested in IFRAME_RE.findall(inner.text)[:1]:
                    deeper = _player_get(nested, inner.url)
                    if deeper is not None:
                        url = _extract_stream(deeper.text)
                        if url:
                            break
            if not url:
                continue
            # Only claim it if the master really loads — an extracted URL
            # that 403s is worse than no URL, because it looks like success.
            try:
                origin = "https://" + frame.split("//", 1)[-1].split("/", 1)[0]
                hdrs = {"Referer": frame, "Origin": origin}
                mr = SESSION.get(url, headers=hdrs, timeout=12)
                if mr.ok and (mr.text or "").lstrip().startswith("#EXTM3U"):
                    _remember_headers(url, hdrs)
                    return url
            except requests.RequestException:
                pass
    return None


def resolve_stream(cid: str) -> tuple[str | None, str | None]:
    """Try each known host × daddy endpoint until one returns a playable
    m3u8. Only if EVERY known host fails do we fall back to scraping dlhd
    for the current host (discover_host) — keeping that off the hot path
    is what stops the CI runner from hammering dlhd.pk on every channel."""
    global _CACHED_HOST
    # Donis has already shown it is not answering — don't spend this
    # channel's budget proving it twice.
    if not _donis_open():
        via_player = resolve_via_players(cid)
        return (via_player, None) if via_player else (None, None)
    fallback = None
    for host in _hosts_to_try(cid):
        result, host_worked = _try_host(cid, host)
        if result and probe_live(result[0]) == "ok":
            with _CACHE_LOCK:
                _CACHED_HOST = host
            _note_donis(True)
            return result
        if result and fallback is None:
            fallback = result
        if host_worked:
            with _CACHE_LOCK:
                _CACHED_HOST = host
            break
    # Last resort: known hosts gave us nothing playable. Scrape dlhd ONCE
    # for the current resolver host (self-heals across host rotations).
    if fallback is None:
        discovered = discover_host(cid)
        if discovered:
            result, host_worked = _try_host(cid, discovered)
            if result:
                if host_worked:
                    with _CACHE_LOCK:
                        _CACHED_HOST = discovered
                _note_donis(True)
                return result
    # Every donis route is exhausted. Fall back to the player wrappers, which
    # is what the app has been using successfully all along. The suffix is
    # None because no daddyN endpoint served this one; the caller leaves
    # daddy_endpoint untouched rather than inventing a label for it.
    _note_donis(fallback is not None)
    if fallback is None:
        via_player = resolve_via_players(cid)
        if via_player:
            return via_player, None
    return fallback if fallback else (None, None)


def resolve_and_probe(cid: str, do_probe: bool) -> tuple[str | None, str | None, str | None]:
    """Returns (stream_url, status, daddy_suffix_used)."""
    if not do_probe:
        # Fast path: just hit the production daddy.php without probing each.
        url = _fetch_daddy_url(cid, "")
        return url, None, ("" if url else None)
    url, suf = resolve_stream(cid)
    if not url:
        return None, None, None
    status = probe_live(url)
    return url, status, suf


# ---------- player mapping ----------
# Each channel has 6 alternate "Player" pages at dlhd.pk/{path}/stream-{id}.php.
# Each page iframes a different upstream backend. We probe all 6 to record which
# backend each routes to, so the tester can pre-color buttons and auto-fall over.

PLAYERS: list[tuple[str, str]] = [
    ("P1", "stream"),
    ("P2", "cast"),
    ("P3", "watch"),
    ("P4", "plus"),
    ("P5", "casting"),
    ("P6", "player"),
]
IFRAME_RE = re.compile(r'<iframe[^>]+src="([^"]+)"', re.IGNORECASE)


def _host_of(url: str) -> str:
    m = re.match(r'^https?://([^/]+)', url)
    return m.group(1) if m else ""


def fetch_player_map(cid: str) -> list[dict]:
    """For one channel, fetch all 6 wrapper pages and extract the iframe target host."""
    out = []
    for name, path in PLAYERS:
        entry = {"name": name, "path": path, "target_host": None, "available": False}
        try:
            r = SESSION.get(
                f"https://dlhd.pk/{path}/stream-{cid}.php",
                headers={"Referer": f"https://dlhd.pk/watch.php?id={cid}"},
                timeout=15,
            )
            if r.ok:
                m = IFRAME_RE.search(r.text)
                if m:
                    target = m.group(1)
                    entry["target_host"] = _host_of(target)
                    # any non-placeholder iframe counts as available
                    if entry["target_host"] and "dlhd.pk" not in entry["target_host"]:
                        entry["available"] = True
        except requests.RequestException:
            pass
        out.append(entry)
    return out


# ---------- reachability ----------
# Each non-donis backend is reachable or not from THIS network. We probe each
# unique host once and cache the result. Channels whose only non-donis fallbacks
# are unreachable hosts get reclassified from 'down' to 'unreachable' so we
# can hide them by default in the tester.

REACHABILITY_CACHE = DATA_DIR / "host_reachability.json"
# Hosts we never need to probe: donis is always reachable for us; we just
# look at it via the smart resolver which iterates daddyN.
SKIP_REACHABILITY_PROBE = {"donis.jimpenopisonline.online"}


def probe_host(host: str) -> dict:
    """Test whether a backend host is reachable from this network.
    Returns {'reachable': bool, 'detail': str}."""
    url = f"https://{host}/"
    try:
        r = SESSION.get(url, timeout=8, allow_redirects=False)
        return {"reachable": True, "detail": f"HTTP {r.status_code}"}
    except requests.exceptions.ConnectionError as e:
        msg = str(e).lower()
        if "name or service" in msg or "nodename" in msg or "getaddrinfo" in msg or "no address" in msg:
            return {"reachable": False, "detail": "dns_fail"}
        return {"reachable": False, "detail": "connection_error"}
    except requests.exceptions.Timeout:
        return {"reachable": False, "detail": "timeout"}
    except requests.RequestException as e:
        return {"reachable": False, "detail": f"err:{type(e).__name__}"}


def collect_unique_backends(channels: list[dict]) -> set[str]:
    hosts = set()
    for c in channels:
        for p in c.get("players", []):
            h = p.get("target_host")
            if h and h not in SKIP_REACHABILITY_PROBE:
                hosts.add(h)
    return hosts


def classify_reachability(channels: list[dict], workers: int = 10) -> dict:
    """Probe every unique alt-backend host; reclassify channels accordingly.
    Returns the host_reachability map (host -> {reachable, detail})."""
    # load cache to avoid re-probing every run
    cache: dict[str, dict] = {}
    if REACHABILITY_CACHE.exists():
        try:
            cache = json.loads(REACHABILITY_CACHE.read_text(encoding="utf-8"))
        except Exception:
            cache = {}

    hosts = collect_unique_backends(channels)
    todo = [h for h in hosts if h not in cache]
    if todo:
        print(f"      probing {len(todo)} new backend hosts ({workers} workers)...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(probe_host, h): h for h in todo}
            for fut in concurrent.futures.as_completed(futures):
                h = futures[fut]
                cache[h] = fut.result()
    # persist cache
    REACHABILITY_CACHE.write_text(
        json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    # classify: 'down' channel with NO reachable non-donis alternate -> 'unreachable'
    bumped = 0
    for c in channels:
        if c.get("status") != "down":
            continue
        alt_hosts = []
        for p in c.get("players", []):
            if not p.get("available"):
                continue
            h = p.get("target_host")
            if not h or h in SKIP_REACHABILITY_PROBE:
                continue
            alt_hosts.append(h)
        if not alt_hosts:
            # no alt at all - donis-only channel that's down
            c["status"] = "unreachable"
            c["unreachable_reason"] = "no alternate backend in player map"
            bumped += 1
            continue
        if not any(cache.get(h, {}).get("reachable") for h in alt_hosts):
            c["status"] = "unreachable"
            c["unreachable_reason"] = (
                f"all alt backends unreachable from this network: "
                f"{', '.join(sorted(set(alt_hosts)))}"
            )
            bumped += 1
    if bumped:
        print(f"      reclassified {bumped} channels as 'unreachable'")
    return cache


# ---------- schedule ----------

CAT_RE = re.compile(
    r'<div class="schedule__catHeader"[^>]*>.*?'
    r'<div class="card__meta">([^<]+)</div>',
    re.DOTALL,
)
EVENT_RE = re.compile(
    r'<div class="schedule__event">\s*'
    r'<div class="schedule__eventHeader"[^>]*data-title="([^"]*)"[^>]*>'
    r'.*?data-time="([^"]*)"[^>]*>([^<]+)</span>'
    r'\s*<span class="schedule__eventTitle">([^<]+)</span>'
    r'.*?<div class="schedule__channels">(.*?)</div>',
    re.DOTALL,
)
CHAN_RE = re.compile(
    r'<a[^>]+href="/watch\.php\?id=(\d+)"[^>]*title="([^"]*)"'
)


def _event_start_unix(time_str: str, now_unix: int) -> int | None:
    """Best-guess Unix timestamp of event start. dlhd publishes 'HH:MM' with no
    date — we treat it as today UTC; if the resulting timestamp would be more
    than 12 h in the past, we assume it's tomorrow (overnight rollover).
    Returns None for malformed times."""
    try:
        hh, mm = map(int, time_str.split(":", 1))
    except (ValueError, AttributeError):
        return None
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return None
    today_midnight = now_unix - (now_unix % 86400)
    ts = today_midnight + hh * 3600 + mm * 60
    # Overnight rollover: if event time treated as today is >12 h ago, it's
    # almost certainly tomorrow (e.g. midnight game on a 23:00 scrape).
    if ts < now_unix - 12 * 3600:
        ts += 86400
    return ts


def _unescape_fully(s: str, rounds: int = 3) -> str:
    """Unescape until it stops changing.

    The source page is DOUBLE-encoded — it ships "&amp;amp;" (565 of them on
    a measured day), so a single pass leaves "&amp;" and the app displayed
    "Brighton &amp; Hove Albion" verbatim on the TV and the phone. Bounded so
    a pathological string can't spin here.
    """
    for _ in range(rounds):
        out = _html.unescape(s)
        if out == s:
            break
        s = out
    return s


def fetch_schedule() -> list[dict]:
    import time as _time
    r = SESSION.get(HOME_URL, timeout=30)
    r.raise_for_status()
    text = r.text

    cat_pos = [
        (m.start(), _html.unescape(m.group(1)).strip())
        for m in CAT_RE.finditer(text)
    ]
    if not cat_pos:
        return []
    positions = [p for p, _ in cat_pos]
    names = {p: n for p, n in cat_pos}

    now_unix = int(_time.time())
    # Drop events whose start was more than 3 h ago. This catches the
    # "Friday/Saturday 21:00 entries still showing on Sunday afternoon" pattern
    # that comes from dlhd.pk's CMS not always cleaning up. Live events get a
    # generous 3 h grace so an in-progress match is still listed.
    stale_cutoff = now_unix - 3 * 3600
    out = []
    dropped_stale = 0
    for m in EVENT_RE.finditer(text):
        # category = nearest catHeader before this event
        cat_name = "Uncategorized"
        for p in reversed(positions):
            if p < m.start():
                cat_name = names[p]
                break
        _data_title, data_time, time_text, title, channels_block = m.groups()
        time_str = (data_time or time_text).strip()
        start_unix = _event_start_unix(time_str, now_unix)
        if start_unix is not None and start_unix < stale_cutoff:
            dropped_stale += 1
            continue
        chans = []
        seen_ids = set()
        for cm in CHAN_RE.finditer(channels_block):
            cid = cm.group(1)
            if cid in seen_ids:
                continue
            seen_ids.add(cid)
            chans.append({"id": cid, "name": _unescape_fully(cm.group(2)).strip()})
        out.append({
            "category": cat_name,
            "time": time_str,
            "title": _unescape_fully(title).strip(),
            "channels": chans,
            "start_unix": start_unix,
            "scraped_at_unix": now_unix,
        })
    if dropped_stale:
        print(f"      dropped {dropped_stale} stale schedule events (>3h in the past)")
    return out


# ---------- schedule post-processing ----------

# dlhd.pk publishes today's events plus 2-3 days of forward-looking events.
# The forward ones are bucketed into categories whose name includes the
# date, e.g. "FIFA World Cup 2026 — Upcoming Matches Jun 14 🏆". We parse
# that date out into its own field and clean the category text so the
# downstream UI can group / tab events by day cleanly.
#
# We assume dlhd.pk publishes in UK local time (this matches the times we
# see on the home page). Events with no date hint default to today's UK
# date.

_UK_ZONE = ZoneInfo("Europe/London")

# Trailing "Month DD" anywhere near the end of the category. The leading
# group captures everything BEFORE the date, the m+d groups capture the
# date itself. We allow trailing emoji / punctuation after the day.
_DATE_IN_CAT_RE = re.compile(
    r"^(?P<lead>.+?)\s+"
    r"(?P<m>Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
    r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|"
    r"Dec(?:ember)?)\s+(?P<d>\d{1,2})\b"
    r"\s*[^\w]*\s*$",
    re.IGNORECASE,
)

_MONTH_ORDINALS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

# dlhd.pk's CMS has a recurring typo in the "Upcoming Matches" header.
# Normalize it so the cleaned category text is consistent across days.
_TYPO_FIXES = [
    (re.compile(r"Upcom[a-z0-9]*ng", re.IGNORECASE), "Upcoming"),
]

# Trailing/leading bracketed emoji or dash-only fragments — leftovers
# after we strip the date. Walk repeatedly until clean.
_CAT_TAIL_CRUFT_RE = re.compile(
    r"[\s\-—–·•:|·]+$"            # trailing dashes/colons/separators
    r"|\s*[^\w\s]+\s*$",          # trailing pure-punctuation/emoji blob
)


def _clean_category_text(s: str) -> str:
    """Apply typo fixes, then strip trailing separator/emoji cruft left
    over after pulling the date out."""
    for pat, repl in _TYPO_FIXES:
        s = pat.sub(repl, s)
    # Iterate because the trailing emoji might be followed by more
    # separator characters.
    prev = None
    while prev != s:
        prev = s
        s = _CAT_TAIL_CRUFT_RE.sub("", s).rstrip()
    return s.strip()


def _resolve_iso_date(today_uk: _dt.date, month_word: str, day: int) -> str:
    """Combine an extracted (month, day) with today's UK date into an ISO
    yyyy-MM-dd string. If the resulting date would be earlier than today,
    roll into next year — guarantees future-event dates always parse to
    a future date.
    """
    month = _MONTH_ORDINALS[month_word[:3].lower()]
    candidate = _dt.date(today_uk.year, month, day)
    if candidate < today_uk:
        candidate = _dt.date(today_uk.year + 1, month, day)
    return candidate.isoformat()


def annotate_dates(events: list[dict]) -> list[dict]:
    """Walk [events], split a trailing 'Month DD' out of each category into
    its own `date` field, clean up the category text, and default events
    with no date hint to today's UK date.

    Also annotates each event with:
      - `start_unix`: Unix timestamp of event start (UK-localised then UTC).
        Lets the app filter / sort by absolute time without re-parsing.
      - `scraped_at_unix`: when this schedule was produced. The app can warn
        if the schedule is stale (e.g. >2 h old) or trigger a refresh.

    Returns a new list (does not mutate input).
    """
    import time as _time
    today_uk = _dt.datetime.now(_UK_ZONE).date()
    now_unix = int(_time.time())
    out: list[dict] = []
    for e in events:
        cat = e.get("category", "")
        m = _DATE_IN_CAT_RE.match(cat)
        if m:
            base_cat = _clean_category_text(m.group("lead"))
            iso = _resolve_iso_date(today_uk, m.group("m"), int(m.group("d")))
        else:
            base_cat = _clean_category_text(cat)
            iso = today_uk.isoformat()
        # Compute start_unix from date + time, both UK-localised. Falls back
        # to None on malformed time strings — the app must handle that.
        start_unix: int | None = None
        time_str = e.get("time", "")
        try:
            hh, mm = map(int, time_str.split(":", 1))
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                d = _dt.date.fromisoformat(iso)
                dt_uk = _dt.datetime(d.year, d.month, d.day, hh, mm,
                                     tzinfo=_UK_ZONE)
                start_unix = int(dt_uk.timestamp())
        except (ValueError, AttributeError):
            pass
        out.append({
            "category": base_cat or "Uncategorized",
            "date": iso,
            "time": time_str,
            "title": e.get("title", ""),
            "channels": e.get("channels", []),
            "start_unix": start_unix,
            "scraped_at_unix": now_unix,
        })
    return out


def _preserve_previous_schedule(path: Path) -> list[dict]:
    """Load the previously-written schedule.json and keep only events that
    haven't passed yet (start within the last 3 h or in the future), so a
    transient schedule-fetch failure preserves the last-good upcoming
    schedule instead of blanking the app's schedule tab. Returns [] only
    if there's genuinely nothing left to keep."""
    if not path.exists():
        return []
    try:
        prev = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(prev, list):
        return []
    import time as _time
    cutoff = int(_time.time()) - 3 * 3600
    return [
        e for e in prev
        if not isinstance(e, dict)
        or e.get("start_unix") is None
        or e.get("start_unix") >= cutoff
    ]


# ---------- logos ----------

_LOGO_ENTRY_RE = re.compile(
    r'#EXTINF:-1([^\n]*),([^\n]+)\nhttps?://[^\s]*?/premium(\d+)/'
)


def _clean_logo_url(url: str | None) -> str | None:
    """Drop known-dead logos and rewrite hotlink-blocked Wikimedia URLs.

    - dtankdempse/daddylive-m3u placeholder: the upstream repo was DMCA'd, so
      every channel pointing at its generic 'ddy-logo.jpg' is a 404 now.
    - upload.wikimedia.org URLs return 403 to non-browser User-Agents; the
      Special:FilePath endpoint on commons.wikimedia.org / en.wikipedia.org
      serves the same image without hotlink protection.
    """
    if not url:
        return None
    if "dtankdempse/daddylive-m3u" in url and "ddy-logo" in url:
        return None
    m = re.match(
        r"^https?://upload\.wikimedia\.org/wikipedia/(commons|en)/(thumb/)?(.+)$",
        url,
    )
    if m:
        site = "commons.wikimedia.org" if m.group(1) == "commons" else "en.wikipedia.org"
        parts = m.group(3).split("/")
        # thumb path: [hash1, hash2, FILENAME, '<size>px-...']  (4 parts)
        # direct:     [hash1, hash2, FILENAME]                  (3 parts)
        if len(parts) >= 3:
            filename = parts[2]
            return f"https://{site}/wiki/Special:FilePath/{filename}"
    return url


def load_logo_map(path: Path = LOGOS_SOURCE) -> dict[str, dict]:
    """Parse the original repo's M3U for tvg-logo + group-title per premium-id.
    Returns {cid: {logo, tvg_id, group}}."""
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8")
    out: dict[str, dict] = {}
    for attrs, _label, cid in _LOGO_ENTRY_RE.findall(text):
        if cid in out:
            continue
        m_logo = re.search(r'tvg-logo="([^"]+)"', attrs)
        m_tvg = re.search(r'tvg-id="([^"]*)"', attrs)
        m_grp = re.search(r'group-title="([^"]*)"', attrs)
        out[cid] = {
            "logo": _clean_logo_url(m_logo.group(1)) if m_logo else None,
            "tvg_id": m_tvg.group(1) if m_tvg else None,
            "group": m_grp.group(1) if m_grp else None,
        }
    return out


def merge_logos(channels: list[dict]) -> int:
    """Annotate each channel with logo/tvg_id/group from the legacy M3U.
    Returns the number of channels we found a logo for."""
    logos = load_logo_map()
    if not logos:
        return 0
    n = 0
    for c in channels:
        info = logos.get(c["id"])
        if not info:
            continue
        for k in ("logo", "tvg_id", "group"):
            if info.get(k):
                c[k] = info[k]
        if info.get("logo"):
            n += 1
    return n


# ---------- writers ----------

def write_m3u8(channels: list[dict], path: Path, include_down: bool) -> None:
    lines = ["#EXTM3U"]
    n = 0
    for c in channels:
        if not c.get("stream_url"):
            continue
        if c.get("status") == "unreachable":
            continue  # never write truly unreachable ones
        if not include_down and c.get("status") == "down":
            continue
        tvg_id = c.get("tvg_id", "")
        logo = c.get("logo", "")
        group = c.get("group") or "DaddyLive (dlhd.pk)"
        lines.append(
            f'#EXTINF:-1 tvg-id="{tvg_id}" tvg-name="{c["name"]}" '
            f'tvg-logo="{logo}" group-title="{group}",{c["name"]}'
        )
        lines.append(c["stream_url"])
        n += 1
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"      {n} channels written to {path.name}")


def write_tester_html(channels: list[dict], schedule: list[dict], path: Path) -> None:
    chans = [c for c in channels if c.get("stream_url")]
    payload = {"channels": chans, "schedule": schedule}
    data_json = json.dumps(payload, ensure_ascii=False)
    template = TESTER_TEMPLATE.read_text(encoding="utf-8")
    path.write_text(template.replace("__DATA__", data_json), encoding="utf-8")




# ---------- driver ----------

def guard_against_collapse(
    channels: list[dict],
    prev_by_id: dict,
    prev_ok_count: int,
    ratio: float = 0.6,
) -> int:
    """Stop a single throttled scrape from wiping the channel list.

    dlhd intermittently rate-limits the CI runner's IP partway through a run.
    When it does, hundreds of channels that resolve perfectly well suddenly
    fail their live-probe in that one pass and get marked "down". The app only
    shows status=="ok" channels, so publishing such a run blanks most of the
    live grid — exactly what happened when ok cratered 715 -> 255 in a single
    refresh. A drop that steep is never real availability (the catalog turns
    over gradually), so when the new ok-count collapses below [ratio] of the
    last good run, keep the previous "ok" verdict for any channel that merely
    flipped to down/unreachable this pass. Freshly-resolved URLs are retained;
    if this run failed to resolve a URL at all, the previous one is restored so
    the channel stays playable. Returns the number of channels restored
    (0 = catalog looked healthy, no action taken)."""
    if prev_ok_count < 100:
        return 0
    new_ok = sum(1 for c in channels if c.get("status") == "ok")
    if new_ok >= prev_ok_count * ratio:
        return 0
    restored = 0
    for c in channels:
        # Anything that is not "ok" — including a status that was never set.
        #
        # This used to list only "down"/"unreachable", which quietly missed
        # the worst case: a run where the probe returns NOTHING for a
        # channel leaves its status unset. On 2026-09-20 every one of 899
        # channels came back that way, the run published
        # "live=0 down=0 unreachable=0", and the app — which lists channels
        # by status — showed users an empty channel list. The guard fired,
        # matched no channel, and restored nothing. A missing verdict is not
        # a verdict of "down"; it means this pass learned nothing, so the
        # last thing we did learn should stand.
        if c.get("status") != "ok" and \
                prev_by_id.get(c["id"], {}).get("status") == "ok":
            c["status"] = "ok"
            if not c.get("stream_url"):
                prev_url = prev_by_id.get(c["id"], {}).get("stream_url")
                if prev_url:
                    c["stream_url"] = prev_url
            restored += 1
    print(f"      !! ok-count collapsed {prev_ok_count} -> {new_ok} "
          f"(dlhd almost certainly throttled this run) — restored {restored} "
          f"channels to their last-good 'ok' status")
    return restored


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="0 = all channels")
    ap.add_argument("--workers", type=int, default=25)
    ap.add_argument("--no-probe", action="store_true",
                    help="skip the live/down probe step")
    ap.add_argument("--map-players", action="store_true",
                    help="probe all 6 alt-player wrappers per channel and record map")
    ap.add_argument("--schedule-only", action="store_true",
                    help="reuse cached channels.json, refresh only schedule + tester")
    ap.add_argument("--include-down", action="store_true",
                    help="write down channels into the M3U as well")
    ap.add_argument("--out-m3u", default="playlist_new.m3u8")
    ap.add_argument("--out-json", default="channels.json")
    ap.add_argument("--out-sched", default="schedule.json")
    ap.add_argument("--out-html", default="tester.html")
    args = ap.parse_args()

    chan_path = DATA_DIR / args.out_json
    sched_path = DATA_DIR / args.out_sched
    unreach_path = DATA_DIR / "unreachable_channels.json"

    if args.schedule_only:
        if not chan_path.exists():
            print(f"!! {chan_path} missing - run a full scrape first", file=sys.stderr)
            return 2
        channels = json.loads(chan_path.read_text(encoding="utf-8"))
        print(f"[1/2] Loaded {len(channels)} cached channels")
    else:
        print("[1/4] Fetching channel list...")
        channels = fetch_channel_list()
        print(f"      {len(channels)} channels listed")
        if args.limit:
            channels = channels[: args.limit]
            print(f"      limited to first {len(channels)}")
        # Preserve stable per-channel fields from a previous run (players map,
        # headless live_stream capture) so we don't have to rebuild them every time.
        prev_by_id: dict = {}
        prev_ok_count = 0
        if chan_path.exists():
            try:
                prev = json.loads(chan_path.read_text(encoding="utf-8"))
                prev_by_id = {c["id"]: c for c in prev}
                prev_ok_count = sum(1 for c in prev if c.get("status") == "ok")
                kept = 0
                for c in channels:
                    p = prev_by_id.get(c["id"])
                    if not p:
                        continue
                    for k in ("players", "live_stream"):
                        if k in p:
                            c[k] = p[k]
                            kept += 1
                print(f"      preserved {kept} stable fields from previous channels.json")
            except Exception as e:
                print(f"      could not preserve previous data: {e}")
        # logos + tvg_id + group from the original M3U
        n_logos = merge_logos(channels)
        if n_logos:
            print(f"      merged {n_logos} channel logos from {LOGOS_SOURCE.name}")

        do_probe = not args.no_probe
        verb = "Resolving + probing" if do_probe else "Resolving"
        print(f"[2/4] {verb} streams ({args.workers} workers)...")
        done = ok_count = live_count = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {
                pool.submit(resolve_and_probe, c["id"], do_probe): c
                for c in channels
            }
            for fut in concurrent.futures.as_completed(futures):
                c = futures[fut]
                url, st, daddy_suf = fut.result()
                c["stream_url"] = url
                if st:
                    c["status"] = st
                if daddy_suf is not None:
                    c["daddy_endpoint"] = "daddy.php" if daddy_suf == "" else f"daddy{daddy_suf}.php"
                done += 1
                if url:
                    ok_count += 1
                if st == "ok":
                    live_count += 1
                if done % 50 == 0 or done == len(channels):
                    msg = f"      {done}/{len(channels)} resolved ({ok_count} ok"
                    if do_probe:
                        msg += f", {live_count} live"
                    msg += ")"
                    print(msg)

    if not args.schedule_only:
        print("[2.5/4] Classifying reachability of alt-backend hosts...")
        classify_reachability(channels)

    if args.map_players and not args.schedule_only:
        # All 6 wrappers hit dlhd.pk - keep concurrency low and persist
        # progress every 50 channels so a transient block doesn't lose work.
        map_workers = min(args.workers, 10)
        print(f"[3/5] Mapping 6 players per channel ({map_workers} workers, ~6 fetches each)...")
        done = 0
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=map_workers) as pool:
                futures = {pool.submit(fetch_player_map, c["id"]): c for c in channels}
                for fut in concurrent.futures.as_completed(futures):
                    c = futures[fut]
                    try:
                        c["players"] = fut.result()
                    except Exception:
                        c["players"] = []
                    done += 1
                    if done % 50 == 0 or done == len(channels):
                        print(f"      {done}/{len(channels)} mapped")
                        # incremental save so a later failure doesn't lose progress
                        chan_path.write_text(
                            json.dumps(channels, indent=2, ensure_ascii=False),
                            encoding="utf-8",
                        )
        except KeyboardInterrupt:
            print(f"      interrupted at {done}/{len(channels)} - partial progress saved")
            raise

    print(f"[{'2/2' if args.schedule_only else ('4/5' if args.map_players else '3/4')}] Fetching schedule...")
    schedule_ok = True
    try:
        # Retry before giving up. The fetch fails intermittently (a single
        # connect timeout was enough to lose a whole run), and losing it has
        # become expensive — see the write guard below.
        raw_schedule = None
        last_err: Exception | None = None
        for attempt in range(1, SCHEDULE_FETCH_ATTEMPTS + 1):
            try:
                raw_schedule = fetch_schedule()
                if raw_schedule:
                    break
            except Exception as err:  # noqa: BLE001 - reported below
                last_err = err
                print(f"      schedule attempt {attempt} failed: {err}")
            if attempt < SCHEDULE_FETCH_ATTEMPTS:
                time.sleep(SCHEDULE_RETRY_SLEEP_S * attempt)
        if not raw_schedule and last_err is not None:
            raise last_err
        schedule = annotate_dates(raw_schedule)
        if not schedule:
            # Treat an empty scrape the same as a failure — dlhd.pk is
            # intermittently unreachable / rate-limited from the CI
            # runner, and a genuine "no events" is implausible.
            raise ValueError("scrape returned no events")
        days = sorted({e["date"] for e in schedule})
        print(f"      {len(schedule)} events scraped across {len(days)} day(s)")
        if days:
            for d in days:
                n = sum(1 for e in schedule if e["date"] == d)
                print(f"        {d}: {n} event(s)")
    except Exception as e:
        print(f"      schedule fetch failed: {e}")
        # CRITICAL: do NOT write an empty schedule — that blanks the
        # app's schedule tab. Preserve the previous schedule.json,
        # dropping only events whose day is already in the past, so the
        # last-good schedule persists across transient fetch failures.
        schedule = _preserve_previous_schedule(sched_path)
        print(f"      preserved {len(schedule)} still-current events "
              f"from previous schedule.json")
        # Preserving keeps only events still in the future, so once the
        # stored schedule has aged out there is nothing left to keep and this
        # lands on an EMPTY list. Writing that publishes "no events" to every
        # user — the schedule tab goes blank — on the strength of one failed
        # fetch. A stale schedule is bad; an empty one is worse and looks like
        # the app broke. Leave the existing file alone instead.
        schedule_ok = bool(schedule)

    if not args.schedule_only:
        guard_against_collapse(channels, prev_by_id, prev_ok_count)
        # Last line of defence. If even after restoring we have nothing
        # playable, this run has learned nothing worth publishing — and
        # publishing it blanks the channel list for every user. Leave the
        # last good catalog in place and fail the run so CI says so.
        playable = sum(1 for c in channels if c.get("status") == "ok")
        if playable == 0:
            print(
                "      !! refusing to publish: ZERO channels came back "
                "playable. Leaving the previous catalog in place."
            )
            return 2

    final_step = "-" if args.schedule_only else ("5/5" if args.map_players else "4/4")
    print(f"[{final_step}] Writing artifacts...")
    chan_path.write_text(json.dumps(channels, indent=2, ensure_ascii=False), encoding="utf-8")
    if schedule_ok:
        sched_path.write_text(
            json.dumps(schedule, indent=2, ensure_ascii=False), encoding="utf-8",
        )
    else:
        print(
            "      !! schedule fetch failed and nothing could be preserved — "
            f"LEAVING {sched_path.name} untouched rather than blanking it"
        )
    write_m3u8(channels, ROOT / args.out_m3u, args.include_down)
    write_tester_html(channels, schedule, ROOT / args.out_html)
    unreachable = [
        {"id": c["id"], "name": c["name"], "reason": c.get("unreachable_reason", "")}
        for c in channels if c.get("status") == "unreachable"
    ]
    unreach_path.write_text(
        json.dumps(unreachable, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"      -> {chan_path.relative_to(ROOT)}")
    if schedule_ok:
        print(f"      -> {sched_path.relative_to(ROOT)}")
    print(f"      -> {unreach_path.relative_to(ROOT)}  ({len(unreachable)} entries)")
    print(f"      -> {args.out_m3u}")
    print(f"      -> {args.out_html}  (open this in a browser)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
