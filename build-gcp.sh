#!/bin/bash
# Build and push seeknal-gateway image to GCP Artifact Registry (nightly-gateway tag).
# Usage: bash build-gcp.sh
set -euo pipefail

registry='asia-southeast2-docker.pkg.dev/arched-jetty-392811/mta-docker'
artifact_id='kcenter/seeknal-gateway'
tag='nightly-gateway'
full_tag="$registry/$artifact_id:$tag"

echo "Building $full_tag ..."

docker build --platform=linux/amd64 --no-cache \
    -f Dockerfile.gateway \
    -t "$full_tag" \
    .

echo "Pushing $full_tag ..."
docker push "$full_tag"

echo "Done. Pull with: docker pull $full_tag"
