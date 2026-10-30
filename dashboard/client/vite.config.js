import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// `npm run dev` serves the React app on :5173 with live reload. The browser's
// WebSocket to /ws is forwarded to the dashboard server on :3000, so the app
// uses the same address in development and in Docker (where Express serves
// the built files and /ws itself).
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/ws": { target: process.env.DASHBOARD_SERVER ?? "ws://localhost:3000", ws: true },
    },
  },
});
