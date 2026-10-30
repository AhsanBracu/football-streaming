# Dashboard: one container that serves the React page and the WebSocket.
#
# Two stages: the first builds the React app (needs Vite and all dev tools),
# the second only gets the built files plus the server. The final image has no
# build tools in it, which keeps it small.

# Stage 1: build the React client into static files (client/dist).
FROM node:24-alpine AS client
WORKDIR /client
# package files first: Docker caches the npm install layer until they change
COPY client/package.json client/package-lock.json ./
RUN npm ci
COPY client/ ./
RUN npm run build

# Stage 2: the server, with the built page as its public/ folder.
FROM node:24-alpine
WORKDIR /app
COPY server/package.json server/package-lock.json ./
RUN npm ci --omit=dev
COPY server/server.js ./
# Replaces the 7b test page: Express now serves the React app.
COPY --from=client /client/dist ./public
ENV PORT=3000
EXPOSE 3000
# Cap Node's heap (the rest of the container limit is for Node itself and buffers).
CMD ["node", "--max-old-space-size=96", "server.js"]
