# Examples — consuming the catalog from your app

If you're building an app on top of `mkurugenzi_viewer`, this folder has
ready-to-copy starting points. Both examples pull data **straight from
`raw.githubusercontent.com`** — they don't depend on anything in this repo
beyond the published JSON files.

## Data endpoints

All three are plain GETs, no auth, CORS-friendly:

```
https://raw.githubusercontent.com/hexhoxhex/mkurugenzi_viewer/main/data/channels.json
https://raw.githubusercontent.com/hexhoxhex/mkurugenzi_viewer/main/data/schedule.json
https://raw.githubusercontent.com/hexhoxhex/mkurugenzi_viewer/main/playlist_new.m3u8
```

Combined size: ~250 KB. Cheap to re-fetch every minute if you want fresh
schedule data. GitHub's raw CDN caches for ~5 min, so two clients won't
double-hit your origin.

## web/demo.html

Self-contained single-file browser app. Open in a browser — no build step.

What it shows:
- **Channels tab**: live channels with logos, filter by name + group dropdown
- **Schedule tab**: today's events grouped by category, click a channel chip to play
- **`<video>` player** powered by hls.js — plays directly, no proxy
- **URL hash routing** for deep links you can share:
  - `#c=657` — auto-open and play channel 657
  - `#tab=schedule&e=fifa` — open Schedule tab filtered to "fifa"
  - `#group=USA%20(DADDY%20LIVE)` — open Channels filtered to that group
- **"share link"** button copies the current state as a hash URL

This file is the demo you'd point app users at to test their setup. Or fork
it and replace the `REPO`/`BRANCH` constants at the top to point at your own
data fork.

## python/consume.py

Stdlib-only CLI for backends / scripts / scheduled jobs. No deps to install.

```bash
# Quick summary of the catalog
python consume.py

# Show one channel's playable URL and play commands
python consume.py --channel 657

# List all live channels in a group
python consume.py --group "USA"
python consume.py --group "SPORTS"

# Find events on the schedule
python consume.py --search "premier league"
python consume.py --search "f1"

# Write a clean live-only M3U
python consume.py --m3u my_playlist.m3u
```

## How to map an event to a playable channel

The schedule lists channel ids that match `channels.json`. Pseudo-code:

```js
const channels = await fetch(CHANNELS_URL).then(r => r.json());
const schedule = await fetch(SCHEDULE_URL).then(r => r.json());
const byId = new Map(channels.map(c => [c.id, c]));

for (const event of schedule) {
  // skip events with no specific channel (DaddyLive uses id "00" as placeholder)
  const playable = event.channels
    .map(ch => byId.get(ch.id))
    .filter(c => c && c.status === 'ok');
  if (playable.length) {
    console.log(event.time, event.title, '->', playable[0].stream_url);
  }
}
```

## What's in each channel record

| field | meaning |
|---|---|
| `id` | DaddyLive's numeric id (e.g. "657") |
| `name` | lowercase display name from dlhd.pk |
| `status` | `"ok"` / `"down"` / `"unreachable"` |
| `stream_url` | signed m3u8 URL — current at last scrape |
| `logo` | direct image URL (or `null`) |
| `group` | DaddyLive's country/category grouping |
| `tvg_id` | matches EPG entries |
| `daddy_endpoint` | which donis daddyN.php served this URL |
| `players` | 6-slot map of alternate backend hosts (see main README) |

For apps consuming this, **filter `status === "ok"`** for the live catalog.
The other statuses are kept in the file as diagnostic information.

## When stream_url returns HTTP 5xx

Means the upstream CDN node rotated since the last scrape. Two options:

1. Trigger a refresh of `channels.json` (call out to the scraper, or wait for
   the next scheduled scrape — typically every 6 hours).
2. Fall back to dlhd.pk's own player iframe:
   `https://dlhd.pk/cast/stream-{channel.id}.php` (this is the URL the demo's
   web tester opens when Direct fails — `dlhd.pk` handles backend selection
   server-side).

## When stream_url 403's or expires

Signed URLs include `expires=<unix>` in the query string. After expiry the
node returns 401/403. Re-run the scraper to mint fresh URLs.

## Updating the data

The data is refreshed by re-running `scraper.py` in the repo root and
committing `data/*.json` + `playlist_new.m3u8`. If you've set up the GitHub
Actions workflow (see main README), this happens automatically.
