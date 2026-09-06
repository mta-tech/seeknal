#!/usr/bin/env bash
# Build a seeknal Docker image and push it to the team's GCP Artifact Registry.
#
# Mirrors iba-deploy-runbook/configs/build-seeknal-worker.sh so the same
# registry and tag conventions are used. Use this to ship a seeknal build
# (e.g. the forecast tool on a feature branch) to the Dev VM BEFORE the
# upstream seeknal release image is published to GHCR.
#
# Usage (from anywhere inside the seeknal repo):
#   ./scripts/build-gcp.sh                              # worker image, tag :dev
#   ./scripts/build-gcp.sh -i gateway                   # gateway image
#   ./scripts/build-gcp.sh -i worker -t seek5-2026-07-03  # custom tag
#   ./scripts/build-gcp.sh -i worker --tag-version      # tag = version from pyproject.toml
#
# Images: worker (default) | gateway | prefect | report-server
#
# Env:
#   SEEKNAL_REGISTRY   override the registry base
#                      (default: asia-southeast2-docker.pkg.dev/arched-jetty-392811/mta-docker)
#
# Prereqs:
#   - docker running
#   - one-time auth against the registry:
#       gcloud auth login
#       gcloud config set project arched-jetty-392811
#       gcloud auth configure-docker asia-southeast2-docker.pkg.dev
set -euo pipefail

DEFAULT_REGISTRY="asia-southeast2-docker.pkg.dev/arched-jetty-392811/mta-docker"
REGISTRY="${SEEKNAL_REGISTRY:-$DEFAULT_REGISTRY}"

IMAGE="worker"
TAG="dev"
TAG_VERSION=0

usage() {
  sed -n '2,21p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

while [ $# -gt 0 ]; do
  case "$1" in
    -i|--image)    IMAGE="$2"; shift 2;;
    -t|--tag)      TAG="$2"; shift 2;;
    --tag-version) TAG_VERSION=1; shift;;
    -h|--help)     usage 0;;
    *) echo "unknown arg: $1" >&2; usage 2;;
  esac
done

case "$IMAGE" in
  worker)        DOCKERFILE="docker/Dockerfile.worker";    ARTIFACT="seeknal-worker";;
  gateway)       DOCKERFILE="docker/Dockerfile.gateway";   ARTIFACT="seeknal-gateway";;
  prefect)       DOCKERFILE="docker/Dockerfile.prefect";   ARTIFACT="seeknal-prefect";;
  report-server) DOCKERFILE="docker/report-server/Dockerfile"; ARTIFACT="seeknal-report-server";;
  *) echo "unknown image: $IMAGE (worker|gateway|prefect|report-server)" >&2; exit 2;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ ! -f "$REPO_ROOT/$DOCKERFILE" ]; then
  echo "$DOCKERFILE not found under $REPO_ROOT" >&2
  echo "Run this script from a seeknal repo checkout." >&2
  exit 1
fi

VERSION="$(python3 -c '
import re
with open("'"$REPO_ROOT"'/pyproject.toml", encoding="utf-8") as f:
    match = re.search(r"^version\s*=\s*\"([^\"]+)\"", f.read(), re.MULTILINE)
    print(match.group(1) if match else "0.0.0")
')"
if [ "$TAG_VERSION" = "1" ]; then
  TAG="$VERSION"
fi

IMAGE_REF="${REGISTRY}/${ARTIFACT}:${TAG}"

echo "==> Building ${IMAGE_REF}"
echo "    context : ${REPO_ROOT}"
echo "    docker  : ${DOCKERFILE}"
echo "    version : ${VERSION} (seeknal)"
docker build --platform=linux/amd64 -f "$REPO_ROOT/$DOCKERFILE" -t "$IMAGE_REF" "$REPO_ROOT"

echo "==> Pushing ${IMAGE_REF}"
docker push "$IMAGE_REF"

cat <<EOF

Done. ${IMAGE_REF}

EOF

if [ "$IMAGE" = "worker" ]; then
  cat <<EOF
To run it on the Dev VM, set in iba-deploy-runbook/configs/.env.seeknal-vm:
  SEEKNAL_WORKER_IMAGE=${REGISTRY}/${ARTIFACT}
  SEEKNAL_WORKER_VERSION=${TAG}
Then on the VM:
  docker compose -f docker-compose.seeknal-vm.yml --env-file .env.seeknal-vm up -d
EOF
fi
