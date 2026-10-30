"""Summarize what the pipeline found per match: who had the ball, who pressed, who
created chances. Reads Spark's windows from MongoDB, prints a table and writes
evaluation/match_summary.csv.

Run it after a replay has finished:

    python3 evaluation/match_summary.py
"""
import csv
from collections import Counter
from pathlib import Path

from pymongo import MongoClient

# Our MongoDB container, seen from the Mac (Homebrew's mongod has 27017).
MONGO_URI = "mongodb://localhost:27018/?directConnection=true"
OUTPUT = Path(__file__).parent / "match_summary.csv"


def complete_replays(windows):
    """For each match, the kickoff of its most complete replay (most windows;
    the newest one if several are complete)."""
    counts = Counter((w["match_id"], w["kickoff"]) for w in windows.find({}, {"match_id": 1, "kickoff": 1}))
    best = {}
    for (match_id, kickoff), n in counts.items():
        if match_id not in best or (n, kickoff) > best[match_id]:
            best[match_id] = (n, kickoff)
    return {match_id: kickoff for match_id, (n, kickoff) in best.items()}


def summarize_match(windows, match_id, kickoff):
    """One row per team. Windows before kickoff (negative minute) are skipped."""
    docs = list(windows.find({"match_id": match_id, "kickoff": kickoff, "minute": {"$gte": 0}}))
    by_key = {(w["team"], w["minute"]): w for w in docs}
    teams = sorted({w["team"] for w in docs})
    minutes = sorted({w["minute"] for w in docs})

    def value(team, minute, field):
        return by_key.get((team, minute), {}).get(field, 0)

    rows = []
    for team in teams:
        opponent = next(t for t in teams if t != team)
        # Whole-match totals: the windows starting at minute 0, 5, 10, ... don't
        # overlap and together cover the match, so their sum counts each event once.
        # (xG is rounded per window, so its total can differ by ~0.01 per window.)
        def total(field):
            return round(sum(value(team, m, field) for m in range(0, minutes[-1] + 1, 5)), 2)

        # "Who is pressing": in how many minutes did this team make more pressures
        # than the opponent over the 5-minute window?
        pressed_more = sum(value(team, m, "pressures") > value(opponent, m, "pressures") for m in minutes)
        peak = max(minutes, key=lambda m: value(team, m, "pressures"))
        rows.append({
            "match_id": match_id, "team": team, "opponent": opponent,
            "passes": total("passes"), "pressures": total("pressures"),
            "pressed_more_pct": round(100 * pressed_more / len(minutes)),
            "peak_pressures": value(team, peak, "pressures"), "peak_minute": f"{peak}-{peak + 5}",
            "shots": total("shots"), "xg": total("xg"), "goals": total("goals"),
        })
    return rows


def main():
    windows = MongoClient(MONGO_URI).football.window_stats
    rows = []
    for match_id, kickoff in sorted(complete_replays(windows).items()):
        rows += summarize_match(windows, match_id, kickoff)
    if not rows:
        print("no windows in MongoDB: run a replay first")
        return

    columns = list(rows[0])
    width = {c: max(len(c), *(len(str(r[c])) for r in rows)) for c in columns}
    print("  ".join(f"{c:>{width[c]}}" for c in columns))
    for r in rows:
        print("  ".join(f"{str(r[c]):>{width[c]}}" for c in columns))

    with OUTPUT.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nwritten to {OUTPUT}")


if __name__ == "__main__":
    main()
