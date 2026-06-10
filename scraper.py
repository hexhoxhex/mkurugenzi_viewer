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
import html as _html
import json
import re
import sys
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
HOME_URL = "https://dlhd.pk/"
CHANNELS_URL = "https://dlhd.pk/24-7-channels.php"
# donis.* serves several daddy endpoints. Each routes to a different CDN node:
#   daddy.php    -> pontos.*  (current production default - dlhd.pk site uses this)
#   daddy2.php   -> kolis.*
#   daddy3.php   -> vomos.*
#   daddy4.php   -> fomis.*
#   daddy5.php   -> zalis.*
# We try them in this order and accept the first that returns 200 + #EXTM3U.
DADDY_BASE = "https://donis.jimpenopisonline.online/premiumtv/daddy{suf}.php?id={id}"
DADDY_SUFFIXES = ["", "2", "3", "4", "5"]

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


def _fetch_daddy_url(cid: str, suf: str) -> str | None:
    """Fetch one daddyN.php (or bare daddy.php if suf='') and extract the base64'd m3u8 URL."""
    try:
        r = SESSION.get(
            DADDY_BASE.format(suf=suf, id=cid),
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
    try:
        r = SESSION.get(url, timeout=15)
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
        from urllib.parse import urljoin
        chunk_url = urljoin(url, chunk_rel)
        r2 = SESSION.get(chunk_url, timeout=10)
        body2 = r2.text or ""
        if r2.ok and body2.lstrip().startswith("#EXTM3U"):
            return "ok"
        return "down"
    except requests.RequestException:
        return "down"


def resolve_stream(cid: str) -> tuple[str | None, str | None]:
    """Try each daddy endpoint until one returns 200 + #EXTM3U. Returns (url, suffix)."""
    fallback = None
    for suf in DADDY_SUFFIXES:
        url = _fetch_daddy_url(cid, suf)
        if not url:
            continue
        if fallback is None:
            fallback = (url, suf)
        if probe_live(url) == "ok":
            return url, suf
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


def fetch_schedule() -> list[dict]:
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

    out = []
    for m in EVENT_RE.finditer(text):
        # category = nearest catHeader before this event
        cat_name = "Uncategorized"
        for p in reversed(positions):
            if p < m.start():
                cat_name = names[p]
                break
        _data_title, data_time, time_text, title, channels_block = m.groups()
        chans = []
        seen_ids = set()
        for cm in CHAN_RE.finditer(channels_block):
            cid = cm.group(1)
            if cid in seen_ids:
                continue
            seen_ids.add(cid)
            chans.append({"id": cid, "name": _html.unescape(cm.group(2)).strip()})
        out.append({
            "category": cat_name,
            "time": (data_time or time_text).strip(),
            "title": _html.unescape(title).strip(),
            "channels": chans,
        })
    return out


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
        if chan_path.exists():
            try:
                prev = json.loads(chan_path.read_text(encoding="utf-8"))
                by_id = {c["id"]: c for c in prev}
                kept = 0
                for c in channels:
                    p = by_id.get(c["id"])
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
    try:
        schedule = fetch_schedule()
        print(f"      {len(schedule)} events scraped")
    except Exception as e:
        print(f"      schedule fetch failed: {e}")
        schedule = []

    final_step = "-" if args.schedule_only else ("5/5" if args.map_players else "4/4")
    print(f"[{final_step}] Writing artifacts...")
    chan_path.write_text(json.dumps(channels, indent=2, ensure_ascii=False), encoding="utf-8")
    sched_path.write_text(json.dumps(schedule, indent=2, ensure_ascii=False), encoding="utf-8")
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
    print(f"      -> {sched_path.relative_to(ROOT)}")
    print(f"      -> {unreach_path.relative_to(ROOT)}  ({len(unreachable)} entries)")
    print(f"      -> {args.out_m3u}")
    print(f"      -> {args.out_html}  (open this in a browser)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
