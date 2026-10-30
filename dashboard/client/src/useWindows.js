import { useEffect, useRef, useState } from "react";

// Connects to the dashboard server and keeps every window it has sent.
//
// The server sends {type: "snapshot", windows} right after connecting, then
// {type: "window", window} for every change. A window that comes again
// replaces the old one (same _id), exactly like Spark's upsert in MongoDB.
export function useWindows() {
  const windows = useRef(new Map()); // _id -> latest version of that window
  const latest = useRef(null);       // the last live update (for the latency tile)
  const [, setVersion] = useState(0); // bumped to make React redraw
  const [connected, setConnected] = useState(false);

  useEffect(() => {
    let socket;
    let retry;
    let frame = 0;
    let stopped = false;

    // One Spark batch can push a few hundred windows within milliseconds.
    // Redrawing the charts for each one is wasted work, so redraw at most
    // once per screen frame.
    const redraw = () => {
      if (!frame) frame = requestAnimationFrame(() => { frame = 0; setVersion((v) => v + 1); });
    };

    function connect() {
      const protocol = location.protocol === "https:" ? "wss" : "ws";
      socket = new WebSocket(`${protocol}://${location.host}/ws`);
      socket.onopen = () => setConnected(true);
      socket.onclose = () => {
        setConnected(false);
        // Server restarted? Try again; the new snapshot fills in what we missed.
        if (!stopped) retry = setTimeout(connect, 2000);
      };
      socket.onmessage = (event) => {
        const msg = JSON.parse(event.data);
        if (msg.type === "snapshot") {
          windows.current = new Map(msg.windows.map((w) => [w._id, w]));
        } else {
          windows.current.set(msg.window._id, msg.window);
          latest.current = msg.window;
        }
        redraw();
      };
    }

    connect();
    return () => {
      stopped = true;
      clearTimeout(retry);
      cancelAnimationFrame(frame);
      socket.close();
    };
  }, []);

  return { windows: windows.current, latest: latest.current, connected };
}
