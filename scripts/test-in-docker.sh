#!/usr/bin/env bash
# Run the Seeknal test suite inside Docker, mirroring the hosted CI test job.
#
# The production .dockerignore excludes tests/ and docs/, so this script builds
# from a temporary context that includes them while still excluding the host
# virtualenv, VCS data, caches, and build output.
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

# Copy the repository into the temporary context. Excludes mirror the intent of
# .dockerignore but keep tests/, docs/, examples/, and qa/ available to tests.
tar \
    --exclude='.git' \
    --exclude='.venv' \
    --exclude='.pytest_cache' \
    --exclude='.ruff_cache' \
    --exclude='__pycache__' \
    --exclude='*.egg-info' \
    --exclude='dist' \
    --exclude='target' \
    --exclude='.worktrees' \
    -C "$ROOT_DIR" -cf - . | tar -C "$BUILD_CONTEXT" -xf -

# The temporary context owns its ignore rules so tests/ and docs/ are copied.
cat > "$BUILD_CONTEXT/.dockerignore" <<'EOF'
.git
.venv
.pytest_cache
.ruff_cache
__pycache__
*.egg-info
dist
target
.worktrees
EOF

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
