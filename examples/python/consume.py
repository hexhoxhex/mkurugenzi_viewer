"""
Consumer-side example: pull the published catalog + schedule and use them.

This script doesn't depend on the scraper code in this repo - it talks only
to the raw GitHub URLs, so it's exactly what an external app would do.

Run:
  python consume.py                       # full summary
  python consume.py --channel 657         # show one channel's stream URL + headers
  python consume.py --search "premier"    # find scheduled events by keyword
  python consume.py --group "USA"         # list channels in a group
  python consume.py --m3u out.m3u         # write a clean live-only M3U

The data files are ~200KB combined - cheap to re-fetch each time.
"""
import argparse
import json
import sys
import urllib.request

REPO = "hexhoxhex/mkurugenzi_viewer"
BRANCH = "main"
BASE = f"https://raw.githubusercontent.com/{REPO}/{BRANCH}"
CHANNELS_URL = f"{BASE}/data/channels.json"
SCHEDULE_URL = f"{BASE}/data/schedule.json"


def fetch_json(url: str):
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.loads(r.read().decode("utf-8"))


def show_channel(channels: list[dict], cid: str) -> int:
    c = next((c for c in channels if c["id"] == cid), None)
    if not c:
        print(f"channel {cid} not in catalog", file=sys.stderr)
        return 2
    print(f"#{c['id']}  {c['name']}")
    print(f"  status:  {c['status']}")
    print(f"  group:   {c.get('group', '-')}")
    print(f"  logo:    {c.get('logo', '-')}")
    print(f"  stream:  {c.get('stream_url', '-')}")
    if c["status"] != "ok":
        print(f"  (not currently playable - try iframe fallback at https://dlhd.pk/cast/stream-{cid}.php)")
        return 0
    # show how to play it
    print()
    print("# Play with hls.js in the browser:")
    print(f'  hls.loadSource("{c["stream_url"]}")')
    print()
    print("# Or pipe to VLC/ffplay (origin sets CORS *, no auth headers needed):")
    print(f"  ffplay '{c['stream_url']}'")
    print(f"  vlc '{c['stream_url']}'")
    return 0


def show_group(channels: list[dict], group_substr: str) -> int:
    matches = [c for c in channels
               if c.get("group") and group_substr.lower() in c["group"].lower()
               and c["status"] == "ok"]
    if not matches:
        print(f"no live channels match group ~ '{group_substr}'", file=sys.stderr)
        return 1
    by_group = {}
    for c in matches:
        by_group.setdefault(c["group"], []).append(c)
    for g, cs in sorted(by_group.items()):
        print(f"\n=== {g} ({len(cs)} live) ===")
        for c in sorted(cs, key=lambda x: x["name"]):
            print(f"  #{c['id']:<5} {c['name']}")
    return 0


def search_schedule(schedule: list[dict], channels: list[dict], q: str) -> int:
    by_id = {c["id"]: c for c in channels}
    q = q.lower()
    hits = []
    for e in schedule:
        hay = (e["title"] + " " + e["category"] + " " +
               " ".join(c["name"] for c in e["channels"])).lower()
        if q in hay:
            hits.append(e)
    if not hits:
        print(f"no events match '{q}'", file=sys.stderr)
        return 1
    for e in hits:
        print(f"\n[{e['time']}]  {e['title']}")
        print(f"  category: {e['category']}")
        for ch in e["channels"]:
            real = by_id.get(ch["id"])
            mark = ""
            if real is None:
                mark = "  (no link)"
            elif real["status"] == "ok":
                mark = f"  -> {real['stream_url']}"
            else:
                mark = f"  (currently {real['status']})"
            print(f"  - {ch['name']}{mark}")
    return 0


def write_m3u(channels: list[dict], path: str) -> int:
    lines = ["#EXTM3U"]
    n = 0
    for c in channels:
        if c["status"] != "ok" or not c.get("stream_url"):
            continue
        tvg_id = c.get("tvg_id", "")
        logo = c.get("logo", "")
        group = c.get("group") or "DaddyLive"
        lines.append(
            f'#EXTINF:-1 tvg-id="{tvg_id}" tvg-name="{c["name"]}" '
            f'tvg-logo="{logo}" group-title="{group}",{c["name"]}'
        )
        lines.append(c["stream_url"])
        n += 1
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"wrote {n} live channels to {path}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--channel", help="show one channel's stream URL + play command")
    ap.add_argument("--group", help="list live channels whose group matches this substring")
    ap.add_argument("--search", help="search the schedule by keyword (event/team/channel)")
    ap.add_argument("--m3u", metavar="PATH", help="write a live-only M3U playlist to PATH")
    args = ap.parse_args()

    print(f"Fetching {CHANNELS_URL} ...", file=sys.stderr)
    channels = fetch_json(CHANNELS_URL)
    live = [c for c in channels if c["status"] == "ok"]
    print(f"  {len(channels)} channels in catalog, {len(live)} currently live", file=sys.stderr)

    if args.channel:
        return show_channel(channels, args.channel)
    if args.group:
        return show_group(channels, args.group)
    if args.m3u:
        return write_m3u(channels, args.m3u)

    print(f"Fetching {SCHEDULE_URL} ...", file=sys.stderr)
    schedule = fetch_json(SCHEDULE_URL)
    print(f"  {len(schedule)} scheduled events", file=sys.stderr)

    if args.search:
        return search_schedule(schedule, channels, args.search)

    # default: print a short summary
    from collections import Counter
    groups = Counter(c.get("group", "(none)") for c in live)
    print(f"\n=== Live channels by group (top 10) ===")
    for g, n in groups.most_common(10):
        print(f"  {n:4}  {g}")
    cats = Counter(e["category"] for e in schedule)
    print(f"\n=== Scheduled events by category (top 10) ===")
    for c, n in cats.most_common(10):
        print(f"  {n:4}  {c}")
    print(f"\nTry: --channel <id>, --group <substring>, --search <keyword>, --m3u <path>")
    return 0


if __name__ == "__main__":
    sys.exit(main())
