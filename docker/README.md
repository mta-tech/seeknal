# Seeknal Docker images

This directory contains the Dockerfiles that are maintained with the Seeknal
repository. Build commands should use the repository root as the Docker build
context so the Dockerfiles can copy `pyproject.toml`, `README.md`, and `src/`.

## Images

| Dockerfile | Image purpose | Default command |
| --- | --- | --- |
| `docker/Dockerfile.gateway` | Seeknal Ask HTTP gateway for REST, SSE, WebSocket, Telegram, and Temporal client mode | `seeknal gateway start --project /app/project --port 8000 --host 0.0.0.0 --temporal --no-worker` |
| `docker/Dockerfile.worker` | Standalone Ask worker. Supports Temporal SDK mode and HTTP-only gateway-routed mode | `seeknal gateway worker` |
| `docker/Dockerfile.prefect` | Prefect deployment runner for Seeknal pipeline projects | `seeknal prefect serve --project-path /app` |
| `docker/report-server/Dockerfile` | Report server for published Evidence-style reports | `seeknal report-server` |

## Build

```bash
docker build -f docker/Dockerfile.gateway -t seeknal-gateway:local .
docker build -f docker/Dockerfile.worker -t seeknal-worker:local .
docker build -f docker/Dockerfile.prefect -t seeknal-prefect:local .
```

## Published images

GitHub Actions publishes the worker image to GitHub Container Registry:

```bash
docker pull ghcr.io/<owner>/<repo>-worker:latest
docker pull ghcr.io/<owner>/<repo>-worker:<seeknal-version>
```

For this repository, that resolves to:

```bash
docker pull ghcr.io/mta-tech/seeknal-worker:latest
```

## Run gateway locally

```bash
docker run --rm \
  -p 8000:8000 \
  -v /path/to/seeknal-project:/app/project \
  --env-file /path/to/seeknal-project/.env \
  -e TEMPORAL_ADDRESS=host.docker.internal:7233 \
  seeknal-gateway:local
```

Pass extra gateway flags after the image name, for example:

```bash
docker run --rm -p 8000:8000 seeknal-gateway:local --project /app/project --port 8000 --host 0.0.0.0
```

## Run worker locally

```bash
docker run --rm \
  -v /path/to/seeknal-project:/app/project \
  --env-file /path/to/seeknal-project/.env \
  -e TEMPORAL_ADDRESS=host.docker.internal:7233 \
  seeknal-worker:local --project /app/project
```

For token-routed multi-tenant Temporal worker deployments, let the worker fetch
its queue and callback configuration from the gateway:

```bash
docker run --rm \
  -v /path/to/seeknal-project:/app/project \
  --env-file /path/to/seeknal-project/.env \
  -e SEEKNAL_GATEWAY_URL=https://gateway.example.com \
  -e SEEKNAL_API_TOKEN="$SEEKNAL_API_TOKEN" \
  seeknal-worker:local --project /app/project
```

For HTTP-only worker deployments, the worker has no Temporal address, queue, or
credentials. It long-polls the gateway/kc-service for work and posts streaming
events/completion back over HTTP:

```bash
docker run --rm \
  -v /path/to/seeknal-project:/app/project \
  --env-file /path/to/seeknal-project/.env \
  -e SEEKNAL_WORKER_TRANSPORT=http \
  -e SEEKNAL_GATEWAY_URL=https://gateway.example.com \
  -e SEEKNAL_API_TOKEN="$SEEKNAL_API_TOKEN" \
  seeknal-worker:local --project /app/project
```

The gateway side must run a Temporal worker activity in HTTP broker mode, for
example `seeknal gateway start --temporal --worker-transport http` or `seeknal
gateway backend --worker-transport http`. In this topology the gateway owns
Temporal routing and the external worker only needs outbound HTTPS.

## Compose

Gateway stack:

```bash
docker compose -f deploy/docker-compose.yml up --build
```

Worker only:

```bash
docker compose -f deploy/docker-compose.worker.yml up --build
```

When running compose from a copied deployment directory, set
`SEEKNAL_BUILD_CONTEXT` to the directory containing the Seeknal source checkout or
tarball extraction:

```bash
SEEKNAL_BUILD_CONTEXT=./seeknal-src docker compose -f deploy/docker-compose.yml up --build
```

## Notes

- Do not bake credentials into images. Use `.env`, Docker secrets, or your
  platform secret manager.
- The gateway and worker images install the full Ask analytics stack plus
  Temporal support, so they can run SQL, Python, and ML-style analysis tools.
- The gateway and worker images intentionally skip libsql-only dependencies
  because these deployments are optimized for network databases and avoiding
  platform-specific Rust builds.
- The images clean package/build caches at build time to avoid retaining large
  Rust and Python download caches in the final runtime layer.
