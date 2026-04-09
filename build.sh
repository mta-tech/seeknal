#!/bin/bash
# Build and push seeknal gateway image to Artifact Registry.
# Version is read from pyproject.toml [project] version field.
set -euo pipefail

# Extract version from pyproject.toml (e.g. version = "2.5.2")
version=$(grep '^version = ' pyproject.toml | head -1 | sed 's/version = "\(.*\)"/\1/')

if [ -z "$version" ]; then
    echo "ERROR: Could not extract version from pyproject.toml"
    exit 1
fi

docker_registry='asia-southeast2-docker.pkg.dev/arched-jetty-392811/mta-docker'
artifact_id='kcenter/seeknal-gateway'

echo "Building seeknal-gateway:$version ..."

# Build the gateway image (ask + temporal extras, no Prefect)
docker build --platform=linux/amd64 --no-cache \
    -f Dockerfile.gateway \
    -t $docker_registry/$artifact_id:$version \
    .

echo "Pushing $docker_registry/$artifact_id:$version ..."
docker push $docker_registry/$artifact_id:$version

echo "Done. Image: $docker_registry/$artifact_id:$version"
