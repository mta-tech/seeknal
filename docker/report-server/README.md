# Seeknal Report Server — Docker Operator Guide

## Build

```bash
docker build -f docker/report-server/Dockerfile -t seeknal-report-server .
```

## Run (api_key mode — default)

```bash
docker run \
  -e SEEKNAL_REPORT_SERVER_KEYS=<key-1> \
  -p 127.0.0.1:8787:8787 \
  -v /srv/reports:/data \
  seeknal-report-server
```

Multiple keys are comma-separated: `SEEKNAL_REPORT_SERVER_KEYS=<key-1>,<key-2>`.

> **Note:** The report server listens on plain HTTP inside the container.
> The `Bearer` API key and owner secret travel over plain HTTP unless TLS is
> terminated in front of the server (for example, by a TLS reverse proxy or
> the gateway).

## Local development (open mode opt-in)

```bash
docker run \
  -e SEEKNAL_REPORT_SERVER_AUTH_MODE=open \
  -p 127.0.0.1:8787:8787 \
  -v /srv/reports:/data \
  seeknal-report-server
```

> **WARNING:** Open mode accepts publishes and revocations from anyone who can
> reach the port. Use only for local development on loopback; do **not** expose
> an open-mode server to a shared network or the internet.

## Health check

```bash
curl http://localhost:8787/healthz
```

Expected response: `{"status": "ok"}`

## Persistent storage

Mount a host directory to `/data` so reports survive container restarts:

```bash
-v /srv/reports:/data
```

The server stores extracted report assets under `/data/assets/` and its
report registry under `/data/registry.db`.

## Deploying with `deploy/deploy-report-server.sh`

The deploy script's pre-flight key check reads `deploy/.env.report-server`
with a simple `KEY=VALUE` parser. It does not interpret Docker Compose `.env`
features such as inline `# comments` or `${...}` variable interpolation, so
keep `SEEKNAL_REPORT_SERVER_KEYS` on a plain `SEEKNAL_REPORT_SERVER_KEYS=...`
line. The post-deploy `/healthz` poll is the authoritative startup check and
fails closed with a non-zero exit if the server does not become healthy.
