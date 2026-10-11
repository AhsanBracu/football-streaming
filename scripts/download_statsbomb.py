#!/usr/bin/env python3
"""Download selected StatsBomb open-data competitions into ./data/statsbomb.

Only the files the pipeline needs are fetched (matches + events), instead of
cloning the full multi-GB repository.

    python3 scripts/download_statsbomb.py                 # FIFA World Cup 2022
    python3 scripts/download_statsbomb.py --competition 55:282 --limit 10
"""
import argparse
import json
import pathlib
import subprocess
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"
OUT = pathlib.Path(__file__).resolve().parent.parent / "data" / "statsbomb"


def fetch(url: str, dest: pathlib.Path) -> None:
    if dest.exists():
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        with urllib.request.urlopen(url, timeout=60) as resp:
            dest.write_bytes(resp.read())
    except urllib.error.URLError as err:
        # python.org builds on macOS ship without CA certificates; curl uses the system store.
        if "CERTIFICATE_VERIFY_FAILED" not in str(err):
            raise
        subprocess.run(["curl", "-fsSL", "-o", str(dest), url], check=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--competition", action="append", default=None,
        help="competition_id:season_id (repeatable). Default 43:106 = World Cup 2022",
    )
    parser.add_argument("--limit", type=int, default=None, help="max matches per competition")
    args = parser.parse_args()

    for comp in args.competition or ["43:106"]:
        comp_id, season_id = comp.split(":")
        matches_file = OUT / "matches" / f"{comp_id}_{season_id}.json"
        fetch(f"{BASE}/matches/{comp_id}/{season_id}.json", matches_file)
        matches = json.loads(matches_file.read_text())[: args.limit]
        print(f"competition {comp}: downloading events for {len(matches)} matches")

        def get_events(m):
            mid = m["match_id"]
            fetch(f"{BASE}/events/{mid}.json", OUT / "events" / f"{mid}.json")
            return mid

        with ThreadPoolExecutor(max_workers=8) as pool:
            for i, _ in enumerate(pool.map(get_events, matches), 1):
                print(f"  {i}/{len(matches)}", end="\r")
        print()

    print(f"done -> {OUT}")


if __name__ == "__main__":
    main()
