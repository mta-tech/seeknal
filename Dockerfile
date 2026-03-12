FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

WORKDIR /app

# Install seeknal from PyPI
ARG SEEKNAL_VERSION
RUN uv pip install --system "seeknal==${SEEKNAL_VERSION}" "seeknal[prefect]==${SEEKNAL_VERSION}"

# Default project path for remote workers
ENV SEEKNAL_PROJECT_PATH=/app

# Create target directory for pipeline state
RUN mkdir -p target

# profiles.yml and seeknal/ project definitions are volume-mounted at runtime:
#   docker run \
#     -v /path/to/profiles.yml:/app/profiles.yml \
#     -v /path/to/seeknal/:/app/seeknal/ \
#     ghcr.io/mta-tech/seeknal:latest

ENTRYPOINT ["prefect", "worker", "start", "--pool", "default"]
