#!/bin/bash
# Build and push seeknal-gateway image to GitHub Container Registry.
# Usage: bash build-ghcr.sh
set -euo pipefail

registry='ghcr.io'
owner='mta-tech'
image='seeknal'
tag='nightly-gateway'
full_tag="$registry/$owner/$image:$tag"

echo "Building $full_tag ..."

docker build --platform=linux/amd64 --no-cache \
    -f Dockerfile.gateway \
    -t "$full_tag" \
    .

echo "Pushing $full_tag ..."
docker push "$full_tag"

echo "Done. Pull with: docker pull $full_tag"
