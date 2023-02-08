#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

# Build docker image ${BUILDKITE_COMMIT}-${arch}

date="$(date +%Y%m%d)"
ghcraddr="ghcr.io/risingwavelabs/risingwave"
dockerhubaddr="risingwavelabs/risingwave"
arch="$(uname -m)"

echo "--- docker build and tag"
docker build -f docker/Dockerfile -t "${ghcraddr}:${BUILDKITE_COMMIT}-${arch}" --target risingwave .

echo "--- check the image can start correctly"
container_id=$(docker run -d "${ghcraddr}:${BUILDKITE_COMMIT}-${arch}" risingwave)
ret_code=$(docker inspect --format='{{.State.ExitCode}}' "$container_id")
if [ "$ret_code" -ne 0 ]; then
  echo "docker run failed with exit code $ret_code"
  docker logs "$container_id"
  exit 1
fi

echo "--- docker images"
docker images

echo "--- ghcr login"
echo "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin

echo "--- dockerhub login"
echo "$DOCKER_TOKEN" | docker login -u "risingwavelabs" --password-stdin

echo "--- docker push to ghcr"
docker push "${ghcraddr}:${BUILDKITE_COMMIT}-${arch}"

echo "--- docker push to dockerhub"
docker tag "${ghcraddr}:${BUILDKITE_COMMIT}-${arch}" "${dockerhubaddr}:${BUILDKITE_COMMIT}-${arch}"
docker push "${dockerhubaddr}:${BUILDKITE_COMMIT}-${arch}"