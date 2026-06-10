# mkurugenzi_viewer

DaddyLive (`dlhd.pk`) IPTV catalog + schedule scraper. Resolves stream URLs
across multiple CDN endpoints, classifies each channel as live / down /
unreachable, scrapes the live event schedule, attaches channel logos, and
emits both an M3U playlist and a self-contained browser tester.

## Consume from an app

The data files are served by GitHub raw. Apps can `fetch` them directly with
no auth and full CORS:

```
https://raw.githubusercontent.com/hexhoxhex/mkurugenzi_viewer/main/data/channels.json
https://raw.githubusercontent.com/hexhoxhex/mkurugenzi_viewer/main/data/schedule.json
https://raw.githubusercontent.com/hexhoxhex/mkurugenzi_viewer/main/playlist_new.m3u8
```

### Channel record schema

```json
{
  "id": "657",
  "name": "discovery family",
  "stream_url": "https://pontos.phantemlis.top/premium657/index.m3u8?md5v1=...&expires=...",
  "status": "ok",
  "logo": "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/united-states/discovery-family-us.png",
  "group": "USA (DADDY LIVE)",
  "tvg_id": "Discovery.Family.Channel.us",
  "daddy_endpoint": "daddy.php",
  "players": [
    {"name": "P1", "path": "stream",  "target_host": "donis.jimpenopisonline.online", "available": true},
    {"name": "P2", "path": "cast",    "target_host": "liveon5.zip",                   "available": true},
    {"name": "P3", "path": "watch",   "target_host": "donis.jimpenopisonline.online", "available": true},
    {"name": "P4", "path": "plus",    "target_host": "www.ksohls.ru",                 "available": true},
    {"name": "P5", "path": "casting", "target_host": "www.ksohls.ru",                 "available": true},
    {"name": "P6", "path": "player",  "target_host": "www.ksohls.ru",                 "available": true}
  ]
}
```

**Status values:**
- `ok` — playable now via direct hls.js (`Access-Control-Allow-Origin: *` on origin)
- `down` — resolved a URL but origin returned 5xx today. Try iframe fallback via `https://dlhd.pk/{path}/stream-{id}.php` for any `players[i].path` where `available`.
- `unreachable` — every alternate backend is unreachable from typical western IPs. Skip.

`status` flips back to `ok` automatically on the next scrape once the upstream recovers.

### Schedule record schema

```json
{
  "category": "Soccer",
  "time": "22:00",
  "title": "FIFA 2026 - Mexico vs South Africa",
  "channels": [{"id": "00", "name": "FIFATV"}]
}
```

Channel `id` matches the `id` in `channels.json` (so apps can resolve event -> playable stream by id). `id == "00"` is DaddyLive's placeholder for "no specific channel" — skip those.

## Refreshing the data

```
pip install requests playwright
playwright install chromium

python scraper.py                  # full refresh: ~5 min, hits dlhd.pk + donis.*
python scraper.py --schedule-only  # just refresh schedule.json + tester.html
python scraper.py --no-probe       # skip the playability probe (faster)
python scraper.py --map-players    # also re-probe all 6 alt-player wrappers
                                   #   (only needed every few weeks)
```

For tough channels where the static resolver can't find a playable URL:

```
python scripts/headless_resolve.py 657 --save   # one channel via real browser
python scripts/headless_resolve.py --all-down --save   # every 'down' channel
```

To diagnose a specific channel's iframe behavior:

```
python scripts/probe_channel.py 206              # default: cast (P2) path
python scripts/probe_channel.py 206 --path watch # try P3 path
python scripts/probe_channel.py 206 --headed    # show the browser window
```

## Project layout

```
scraper.py                      Main entrypoint
tester.html                     Open in a browser to browse/play the catalog
playlist_new.m3u8               Drop-in for VLC / Kodi / TiviMate
data/
  channels.json                 Catalog with per-channel state and metadata
  schedule.json                 Today's live events with channel id links
  unreachable_channels.json     Just the truly-dead-from-this-network ones
  host_reachability.json        Per-host alive/dead cache (skip re-probing)
  live_streams.json             Output of the headless resolver
scripts/
  tester_template.html          Source for tester.html (substitute __DATA__)
  headless_resolve.py           Playwright-driven resolver for tough channels
  probe_channel.py              One-shot diagnostic for a single channel
all_channels/                   Original repo's M3U files (source of channel logos)
```

## How the resolver works

DaddyLive's `donis.jimpenopisonline.online` backend serves five `daddyN.php`
endpoints, each routing to a different CDN node:

```
daddy.php   -> pontos.*   (production default - dlhd.pk's site uses this)
daddy2.php  -> kolis.*
daddy3.php  -> vomos.*
daddy4.php  -> fomis.*
daddy5.php  -> zalis.*
```

Only one node is alive per channel per day; which one varies. The scraper
iterates them in order and accepts the first that returns `200 + #EXTM3U`.
Earlier versions only hit `daddy3.php` and missed many channels.

When the donis backend is fully dead for a channel, dlhd.pk's player UI
falls through to one of 5 alternate hosts (`liveon5.zip`, `ksohls.ru`,
`out-1.welovetocare.shop`, `popcdn.day`, etc.). The `players` field in
each channel record tells you which alternate host each Player 1-6 slot
routes to — you can iframe `https://dlhd.pk/{path}/stream-{id}.php` and
DaddyLive's wrapper will handle backend selection / anti-bot natively.

## Caveats

- Signed `stream_url` tokens expire roughly every **2 months**. Re-run the
  scraper before then.
- Around 30 channels are `unreachable` from typical western IPs because their
  only alt-backends are geo-shaped to EU/ME (`ksohls.ru`, `welovetocare`).
  Status would flip if the scraper runs from a different region.
- The 30 `unreachable` channels are written to `data/unreachable_channels.json`
  for inspection but are excluded from `playlist_new.m3u8`.

## Disclaimer

This repo doesn't host any streams or control the upstream content. It only
mirrors a publicly-discoverable URL chain and emits machine-readable data.
Use is the consumer's responsibility under their local jurisdiction.
