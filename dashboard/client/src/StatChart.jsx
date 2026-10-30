import { CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

// One stat for both teams: x = match minute (window start), y = the stat over
// the 5-minute window starting at that minute.
export default function StatChart({ title, rows, teams, colors, decimals = false }) {
  return (
    <section className="card">
      <h2>{title}</h2>
      <ResponsiveContainer width="100%" height={220}>
        <LineChart data={rows} margin={{ top: 8, right: 16, bottom: 0, left: -16 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
          <XAxis dataKey="minute" type="number" domain={[0, "dataMax"]} tickCount={10} />
          <YAxis allowDecimals={decimals} />
          <Tooltip labelFormatter={(m) => `minute ${m}–${m + 5}`} />
          <Legend />
          {teams.map((team, i) => (
            <Line
              key={team}
              dataKey={team}
              stroke={colors[i]}
              strokeWidth={2}
              dot={false}
              type="monotone"
              isAnimationActive={false} // redraws many times per second; animation would lag behind
              connectNulls
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </section>
  );
}
