#!/usr/bin/env bash
# Run the Seeknal test suite inside Docker, mirroring the hosted CI test job.
#
# The production .dockerignore excludes tests/ and docs/, so this script builds
# from a temporary context that includes them while still excluding the host
# secrets, local agent state, virtualenvs, VCS data, caches, and build output.
#
# Usage:
#   scripts/test-in-docker.sh                          # full CI-equivalent suite
#   scripts/test-in-docker.sh tests/ask/test_config.py # focused file
#   scripts/test-in-docker.sh tests/cli/test_gateway.py -q
#
# Environment:
#   SEEKNAL_TEST_IMAGE   image tag (default: seeknal-test:local)
#   DOCKER_BUILDKIT      honoured by docker build (default: 1)

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${SEEKNAL_TEST_IMAGE:-seeknal-test:local}"
BUILD_CONTEXT="$(mktemp -d)"
cleanup() {
    rm -rf "$BUILD_CONTEXT"
}
trap cleanup EXIT

# One list controls both the archive and Docker COPY. Tests generate their own
# .env.example fixtures, so no host .env files need to enter the build context.
CONTEXT_EXCLUDES=(
    '.git'
    '.env'
    '.env.*'
    '.claude'
    '.omc'
    '.omx'
    '.codex'
    '.agents'
    'node_modules'
    '.venv*'
    '.pytest_cache'
    '.ruff_cache'
    '__pycache__'
    '*.egg-info'
    'dist'
    'target'
    '.worktrees'
    'LANE_REPORT.md'
)
TAR_EXCLUDES=()
for pattern in "${CONTEXT_EXCLUDES[@]}"; do
    TAR_EXCLUDES+=("--exclude=$pattern")
done

# Keep tests/, docs/, examples/, and qa/ available to tests.
tar "${TAR_EXCLUDES[@]}" -C "$ROOT_DIR" -cf - . | tar -C "$BUILD_CONTEXT" -xf -

# Docker patterns need an explicit recursive form for nested paths such as the
# TUI's node_modules. The tar excludes already match at every directory depth.
for pattern in "${CONTEXT_EXCLUDES[@]}"; do
    printf '%s\n' "$pattern" "**/$pattern"
done > "$BUILD_CONTEXT/.dockerignore"

echo "Building $IMAGE from a CI-equivalent dependency set..."
DOCKER_BUILDKIT="${DOCKER_BUILDKIT:-1}" docker build \
    --file "$BUILD_CONTEXT/docker/Dockerfile.test" \
    --tag "$IMAGE" \
    "$BUILD_CONTEXT"

if [ "$#" -eq 0 ]; then
    set -- tests/ -x --ignore=tests/workflow/test_prefect_integration.py --ignore=tests/e2e
fi

echo "Running: uv run pytest $*"
docker run --rm "$IMAGE" "$@"
