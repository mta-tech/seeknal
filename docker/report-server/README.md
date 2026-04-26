# Seeknal Report Server — Docker Operator Guide

## Build

```bash
docker build -f docker/report-server/Dockerfile -t seeknal-report-server .
```

## Run (open mode — LAN only)

```bash
docker run -p 8787:8787 -v /srv/reports:/data seeknal-report-server
```

> **WARNING:** Open mode accepts publishes from anyone who can reach the port.
> Do **not** expose this to the internet without enabling `api_key` auth.

## Run (api_key mode)

```bash
docker run \
  -e SEEKNAL_REPORT_SERVER_AUTH_MODE=api_key \
  -e SEEKNAL_REPORT_SERVER_KEYS=your-secret-key \
  -p 8787:8787 \
  -v /srv/reports:/data \
  seeknal-report-server
```

Multiple keys are comma-separated: `SEEKNAL_REPORT_SERVER_KEYS=key1,key2`.

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
