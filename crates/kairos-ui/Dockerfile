################################################################################
# Stage 1: Build the Vue 3 / Vite frontend
################################################################################
FROM node:20-bookworm-slim AS build
WORKDIR /ui

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    make \
    g++ \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies (cached layer)
COPY frontend/kairos-ui/package.json frontend/kairos-ui/package-lock.json* ./
RUN npm install --no-audit --no-fund

# Build production bundle
COPY frontend/kairos-ui/ ./
RUN npm run build

################################################################################
# Stage 2: Serve with Caddy
################################################################################
FROM caddy:2.8-alpine AS runtime

# SPA fallback + reverse proxy to gateway (reachable on the docker network as `kairos-gateway`)
COPY frontend/kairos-ui/docker/Caddyfile /etc/caddy/Caddyfile

# Static assets
COPY --from=build /ui/dist /srv

EXPOSE 80
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -qO- http://127.0.0.1:80/ >/dev/null 2>&1 || exit 1

CMD ["caddy", "run", "--config", "/etc/caddy/Caddyfile"]
