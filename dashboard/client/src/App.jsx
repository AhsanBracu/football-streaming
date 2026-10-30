import { useState } from "react";
import StatChart from "./StatChart.jsx";
import { useWindows } from "./useWindows.js";

const TEAM_COLORS = ["#2563eb", "#ea580c"]; // blue / orange: distinguishable for colour-blind viewers

const STATS = [
  { key: "pressures", title: "Pressures (per 5 min)" },
  { key: "passes", title: "Passes (per 5 min)" },
  { key: "shots", title: "Shots (per 5 min)" },
  { key: "xg", title: "Expected goals, xG (per 5 min)", decimals: true },
];

// Every replay of a match gets its own kickoff time, so the same match_id can
// appear several times in MongoDB. match_id + kickoff identifies one replay.
function groupReplays(windows) {
  const replays = new Map();
  for (const w of windows.values()) {
    const key = `${w.match_id}|${w.kickoff}`;
    if (!replays.has(key)) replays.set(key, { key, matchId: w.match_id, kickoff: w.kickoff, teams: new Set(), windows: [] });
    const replay = replays.get(key);
    replay.teams.add(w.team);
    replay.windows.push(w);
  }
  // Newest replay first.
  return [...replays.values()].sort((a, b) => (a.kickoff < b.kickoff ? 1 : -1));
}

// One row per minute with each team's window: {minute, Canada: {...}, Morocco: {...}}.
// Windows that start before kickoff (negative minute) are left out.
function rowsByMinute(replay) {
  const rows = new Map();
  for (const w of replay.windows) {
    if (w.minute == null || w.minute < 0) continue;
    if (!rows.has(w.minute)) rows.set(w.minute, { minute: w.minute });
    rows.get(w.minute)[w.team] = w;
  }
  return [...rows.values()].sort((a, b) => a.minute - b.minute);
}

const time = (iso) => new Date(iso).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });

export default function App() {
  const { windows, latest, connected } = useWindows();
  const [selected, setSelected] = useState(""); // "" = always show the newest replay

  const replays = groupReplays(windows);
  const replay = replays.find((r) => r.key === selected) ?? replays[0];
  const teams = replay ? [...replay.teams].sort() : [];
  const rows = replay ? rowsByMinute(replay) : [];
  const lastMinute = rows.length ? rows[rows.length - 1].minute : null;

  // End-to-end latency of the last live update: producer sent -> we received it.
  const latency = latest ? ((latest.received_at - latest.last_sent_at) / 1000).toFixed(1) : null;

  return (
    <main>
      <header>
        <h1>{replay ? teams.join(" vs ") : "Football live stats"}</h1>
        <select value={selected} onChange={(e) => setSelected(e.target.value)}>
          <option value="">Newest replay</option>
          {replays.map((r) => (
            <option key={r.key} value={r.key}>
              {[...r.teams].sort().join(" vs ")} · match {r.matchId} · kickoff {time(r.kickoff)}
            </option>
          ))}
        </select>
      </header>

      <div className="tiles">
        <Tile label="Connection" value={connected ? "live" : "reconnecting…"} />
        <Tile label="Latest window" value={lastMinute == null ? "–" : `minute ${lastMinute}`} />
        <Tile label="Windows (this replay)" value={replay ? replay.windows.length : 0} />
        <Tile label="End-to-end latency" value={latency == null ? "–" : `${latency} s`} />
      </div>

      {!replay ? (
        <p className="empty">No windows yet. Start the Spark job and the producer.</p>
      ) : (
        <div className="charts">
          {STATS.map((s) => (
            <StatChart
              key={s.key}
              title={s.title}
              teams={teams}
              colors={TEAM_COLORS}
              decimals={s.decimals}
              rows={rows.map((r) => Object.fromEntries([["minute", r.minute], ...teams.map((t) => [t, r[t]?.[s.key]])]))}
            />
          ))}
        </div>
      )}
    </main>
  );
}

function Tile({ label, value }) {
  return (
    <div className="tile">
      <div className="tile-label">{label}</div>
      <div className="tile-value">{value}</div>
    </div>
  );
}
