FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

WORKDIR /build

# Copy source for local install
COPY pyproject.toml README.md ./
COPY src/ src/

# Install seeknal from local source + prefect extra.
# On arm64, libsql-experimental fails (no wheel, needs Rust toolchain).
# Fallback: install without libsql deps — Docker deployments
# use network databases (PostgreSQL/Iceberg), not SQLite/libsql.
RUN uv pip install --system ".[prefect]" \
    || ( \
      echo "Full install failed (likely arm64), retrying without libsql..." && \
      uv pip install --system --no-deps . && \
      python3 -c "import importlib.metadata, re; deps = importlib.metadata.requires('seeknal') or []; skip = {'libsql-experimental', 'sqlalchemy-libsql'}; lines = [d.split(';')[0].strip() for d in deps if re.split(r'[><=!~\[]', d)[0].strip().lower() not in skip and 'extra ==' not in d]; open('/tmp/deps.txt','w').write(chr(10).join(lines) + chr(10))" && \
      echo 'prefect>=3.1.10,<4.0' >> /tmp/deps.txt && \
      uv pip install --system -r /tmp/deps.txt \
    )

WORKDIR /app

# Default project path for remote workers
ENV SEEKNAL_PROJECT_PATH=/app

# Create target directory for pipeline state
RUN mkdir -p target

# Mount your project at runtime:
#   docker run -v /path/to/your/project:/app ghcr.io/mta-tech/seeknal:latest

ENTRYPOINT ["seeknal", "prefect", "serve", "--project-path", "/app"]
