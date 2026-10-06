"""Publish what a TV has verified about each channel back into the catalogue.

Why: CI can no longer verify channels. The wrapper source throttles
datacentre IPs so hard that the refresh resolved 0 of 899 channels on every
run (2026-10-06) and the catalogue had been frozen since 2026-10-04 behind a
green workflow, with 126 channels carrying no verdict and hidden from every
user. From a home connection the same channels resolve normally.

The TV app keeps a ledger of what it has verified from its own network — every
channel someone actually watched, plus a gentle hourly background check (see
HomeSweepWorker / HomeHealthLedger in moviebox-tv) — and serves it at
/api/live/health. This script pulls that ledger and merges it in:

  data/channels.json   status of each channel (this is what the app lists by)
    - "ok" from the TV        -> status ok. Something played on a real TV.
    - "down" twice in a row   -> status down. One failure can be a blip, and
                                 hiding a channel hides it from everyone.
  data/health.json     the sweep's per-channel results; a home verdict newer
                       than the existing entry replaces it, marked
                       source: "home".

Only verdicts newer than --max-age-hours are used.

Usage (on the PC the TV is reachable from):
    python scripts/publish_home_health.py --dry-run
    python scripts/publish_home_health.py --push

--push commits and pushes as the repo's LOCAL git identity and refuses to run
if that identity is not the expected anonymous one.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
CHANNELS = DATA / "channels.json"
HEALTH = DATA / "health.json"

EXPECTED_NAME = "hexhoxhex"
EXPECTED_EMAIL = "hexhoxhex@users.noreply.github.com"

# Anything that looks like a credential must never be committed.
SECRET_RE = re.compile(
    r"eyJ[A-Za-z0-9_-]{20,}|ghp_[A-Za-z0-9]{20,}|github_pat_|"
    r"BEGIN [A-Z ]*PRIVATE KEY|AKIA[0-9A-Z]{16}"
)


# --------------------------------------------------------------- fetching --

def adb_forward(serial: str) -> None:
    """Best effort: make 127.0.0.1:8080 reach the TV's remote server. The
    server auto-approves loopback callers, so no pairing is needed."""
    for cmd in (["adb", "connect", serial],
                ["adb", "-s", serial, "forward", "tcp:8080", "tcp:8080"]):
        try:
            subprocess.run(cmd, capture_output=True, timeout=20, check=False)
        except (OSError, subprocess.SubprocessError):
            pass


def fetch_ledger(url: str) -> list[dict]:
    with urllib.request.urlopen(url, timeout=30) as r:
        body = json.loads(r.read().decode("utf-8"))
    return body.get("results") or []


# ---------------------------------------------------------------- merging --

def write_json(path: Path, obj, *, ensure_ascii: bool) -> None:
    # LF only: these files are written by CI on Linux, and a Windows newline
    # translation would turn a handful of changed statuses into a whole-file
    # diff.
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(obj, indent=2, ensure_ascii=ensure_ascii))


def apply(ledger: list[dict], max_age_s: int) -> dict:
    now = int(time.time())
    fresh = [e for e in ledger if now - int(e.get("checked_at") or 0) <= max_age_s]

    channels = json.loads(CHANNELS.read_text(encoding="utf-8"))
    by_id = {str(c.get("id")): c for c in channels}

    promoted = demoted = from_unset = unknown = 0
    for e in fresh:
        c = by_id.get(str(e.get("id")))
        if c is None:
            unknown += 1
            continue
        before = c.get("status")
        if e.get("status") == "ok" and before != "ok":
            c["status"] = "ok"
            promoted += 1
            if not before:
                from_unset += 1
        elif (e.get("status") == "down" and int(e.get("fails") or 0) >= 2
              and before != "down"):
            c["status"] = "down"
            demoted += 1

    health = {}
    if HEALTH.exists():
        health = json.loads(HEALTH.read_text(encoding="utf-8"))
    results = {str(r.get("id")): r for r in health.get("results") or []}
    merged = 0
    for e in fresh:
        rid = str(e.get("id"))
        prev = results.get(rid)
        if prev and int(prev.get("checked_at") or 0) >= int(e.get("checked_at") or 0):
            continue
        name = (by_id.get(rid) or {}).get("name") or (prev or {}).get("name", "")
        results[rid] = {
            "id": rid,
            "name": name,
            "checked_at": int(e.get("checked_at") or 0),
            "status": e.get("status"),
            "fail_reason": e.get("fail_reason"),
            "daddy_endpoint": None,
            "host": e.get("host"),
            "first_segment_url": None,
            "source": "home",
        }
        merged += 1

    out = sorted(results.values(),
                 key=lambda r: int(r["id"]) if str(r["id"]).isdigit() else 0)
    health.update({
        "swept_at": max(int(health.get("swept_at") or 0),
                        max((int(e.get("checked_at") or 0) for e in fresh), default=0)),
        "channels_swept": len(out),
        "ok_count": sum(1 for r in out if r.get("status") == "ok"),
        "fail_count": sum(1 for r in out if r.get("status") not in ("ok", "unknown")),
        "results": out,
    })

    if promoted or demoted:
        write_json(CHANNELS, channels, ensure_ascii=False)
    if merged:
        write_json(HEALTH, health, ensure_ascii=True)

    return {
        "ledger": len(ledger), "fresh": len(fresh), "promoted": promoted,
        "from_unset": from_unset, "demoted": demoted, "health_merged": merged,
        "not_in_catalogue": unknown,
        "ok_now": sum(1 for c in channels if c.get("status") == "ok"),
        "unset_now": sum(1 for c in channels if not c.get("status")),
    }


# -------------------------------------------------------------------- git --

def git(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True,
                          text=True, check=check)


def identity_ok() -> bool:
    name = git("config", "user.name", check=False).stdout.strip()
    email = git("config", "user.email", check=False).stdout.strip()
    if (name, email) != (EXPECTED_NAME, EXPECTED_EMAIL):
        print(f"!! refusing to push: local git identity is {name!r} <{email}>, "
              f"expected {EXPECTED_NAME} <{EXPECTED_EMAIL}>.")
        return False
    return True


def commit_and_push(summary: dict) -> str:
    """"pushed", "rejected" (CI got there first; worth retrying) or "refused"."""
    files = ["data/channels.json", "data/health.json"]
    git("add", *files)
    if git("diff", "--cached", "--quiet", check=False).returncode == 0:
        print("nothing to commit")
        return "pushed"
    if SECRET_RE.search(git("diff", "--cached").stdout):
        print("!! refusing to commit: the staged diff contains something that "
              "looks like a credential.")
        git("reset", "-q", "--", *files, check=False)
        return "refused"
    msg = (f"home-health: +{summary['promoted']} ok "
           f"({summary['from_unset']} previously unclassified), "
           f"-{summary['demoted']} down, from the TV's own network")
    git("-c", "commit.gpgsign=false", "commit", "-q", "-m", msg)
    head = git("log", "-1", "--format=%an <%ae>|%cn <%ce>").stdout.strip()
    expected = f"{EXPECTED_NAME} <{EXPECTED_EMAIL}>"
    if head != f"{expected}|{expected}":
        print(f"!! commit carries the wrong identity ({head}); undoing it.")
        git("reset", "-q", "--soft", "HEAD~1", check=False)
        return "refused"
    if git("push", "-q", "origin", "HEAD:main", check=False).returncode != 0:
        return "rejected"
    local = git("rev-parse", "HEAD").stdout.strip()
    remote = git("ls-remote", "origin", "refs/heads/main").stdout.split()[0]
    print(f"pushed {local[:12]}" if local == remote else
          f"!! remote {remote[:12]} != local {local[:12]}")
    return "pushed" if local == remote else "rejected"


# ------------------------------------------------------------------- main --

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://127.0.0.1:8080/api/live/health")
    ap.add_argument("--serial", default="192.168.100.8:5555",
                    help="adb serial of the TV ('' to skip adb forwarding)")
    ap.add_argument("--max-age-hours", type=float, default=72.0)
    ap.add_argument("--dry-run", action="store_true",
                    help="report what would change; write nothing")
    ap.add_argument("--push", action="store_true",
                    help="commit and push the result")
    args = ap.parse_args()

    if args.serial:
        adb_forward(args.serial)
    try:
        ledger = fetch_ledger(args.url)
    except Exception as e:  # noqa: BLE001
        print(f"!! could not read the TV's ledger at {args.url}: {e}")
        return 1
    print(f"TV ledger: {len(ledger)} verdicts")
    if not ledger:
        return 0

    if args.push:
        if not identity_ok():
            return 2
        # Start from the latest catalogue so the push applies cleanly on top
        # of whatever CI committed since.
        git("pull", "-q", "--rebase", "--autostash", "origin", "main", check=False)

    if args.dry_run:
        snapshot = {p: p.read_bytes() for p in (CHANNELS, HEALTH) if p.exists()}
        try:
            summary = apply(ledger, int(args.max_age_hours * 3600))
        finally:
            for p, b in snapshot.items():
                p.write_bytes(b)
        print("DRY RUN —", json.dumps(summary))
        return 0

    for attempt in range(3):
        summary = apply(ledger, int(args.max_age_hours * 3600))
        print(json.dumps(summary))
        if not args.push:
            return 0
        outcome = commit_and_push(summary)
        if outcome == "pushed":
            return 0
        if outcome == "refused":
            return 2
        # CI pushed in between. Drop our commit (keeping any other local
        # work), take theirs, and apply again on top.
        print(f"push rejected (attempt {attempt + 1}); re-applying on latest")
        git("reset", "-q", "--keep", "HEAD~1", check=False)
        git("checkout", "--", "data/channels.json", "data/health.json", check=False)
        git("pull", "-q", "--rebase", "--autostash", "origin", "main", check=False)
    return 1


if __name__ == "__main__":
    sys.exit(main())
