// Dashboard server: watch MongoDB for changed windows and push them to browsers.
//
// Spark rewrites a window document every time a new event lands in it. A Change
// Stream lets us subscribe to those writes instead of polling the collection.
// Change Streams read MongoDB's oplog, which only exists on a replica set; that
// is why our MongoDB runs as the single-node replica set rs0.
//
//   MongoDB --Change Stream--> this server --WebSocket--> every open browser
//
// A browser that connects first gets a snapshot (all current windows), then
// every change as it happens. Both are JSON messages:
//   {type: "snapshot", windows: [...]}   once, right after connecting
//   {type: "window", window: {...}}      for every insert/replace by Spark
import { createServer } from "node:http";
import { fileURLToPath } from "node:url";
import express from "express";
import { MongoClient } from "mongodb";
import { WebSocketServer } from "ws";

// On the Mac our container is on port 27018 (Homebrew's mongod has 27017).
// directConnection: talk to this one server instead of looking up the replica
// set's member address (mongodb:27017), which only resolves inside Docker.
const MONGO_URI = process.env.MONGO_URI ?? "mongodb://localhost:27018/?directConnection=true";
const DB = "football";
const COLLECTION = "window_stats";
const PORT = Number(process.env.PORT ?? 3000);

const client = new MongoClient(MONGO_URI);
await client.connect();
const windows = client.db(DB).collection(COLLECTION);
console.log(`connected, ${await windows.countDocuments()} windows in ${DB}.${COLLECTION}`);

// HTTP serves the page; the WebSocket server shares the same port on path /ws
// (a separate path so the React dev server can forward just that to us).
const app = express();
// public/ next to this file, whatever folder the server is started from.
// (fileURLToPath, not .pathname: it decodes the %20 of the space in our path.)
app.use(express.static(fileURLToPath(new URL("public", import.meta.url))));
const http = createServer(app);
const wss = new WebSocketServer({ server: http, path: "/ws" });

wss.on("connection", async (socket) => {
  // The socket is registered (in wss.clients) before the snapshot is read, so a
  // change that happens meanwhile is still pushed; the page keeps one entry per
  // _id, so seeing a window twice does no harm.
  const snapshot = await windows.find().sort({ window_start: 1 }).toArray();
  socket.send(JSON.stringify({ type: "snapshot", windows: snapshot }));
  console.log(`browser connected (${wss.clients.size} open), sent ${snapshot.length} windows`);
});

http.listen(PORT, () => console.log(`dashboard on http://localhost:${PORT}`));

// Spark writes with "replace", so every change event already carries the whole
// new document in fullDocument (no extra lookup needed).
const changes = windows.watch();

// Ctrl+C (SIGINT) on the Mac, "docker compose stop" (SIGTERM) in Docker. In a
// container Node is process 1, which gets no default signal handling: without
// this, Docker waits 10 s and then kills it.
async function shutdown() {
  await changes.close();
  await client.close();
  process.exit(0);
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

for await (const change of changes) {
  const w = change.fullDocument;
  if (!w) continue; // e.g. a delete or a drop: nothing to show

  // When the update reached us. received_at - last_sent_at is the end-to-end
  // latency: producer -> Kafka -> Spark -> MongoDB -> here. (processed_at is set
  // when Spark's micro-batch starts, so it misses the batch's own run time.)
  w.received_at = Date.now();

  const message = JSON.stringify({ type: "window", window: w });
  for (const socket of wss.clients) socket.send(message);

  console.log(
    `${change.operationType.padEnd(7)} ${w.team.padEnd(8)} minute ${String(w.minute).padStart(3)}` +
    `  pressures ${w.pressures}  latency ${w.received_at - w.last_sent_at} ms`
  );
}
