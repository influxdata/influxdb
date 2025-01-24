#!/bin/sh

# Fast fail the script on failures
set -e

# This script is used to build the InfluxDB 3 Enterprise trial license
# service Docker container and push it to a private Google artifact registry.

# Runnning this script will requires Docker to be installed locally

# Confirm that the user wants to build and deploy the service
echo "This script will build the InfluxDB 3 Enterprise trial license service"
echo "Docker container and push it to a private Google artifact registry."
echo "Do you want to continue? (y/N)"
read -r response
if [ "$response" != "y" ]; then
  echo "Exiting..."
  exit 1
fi

# Set the project ID
PROJECT_ID="influxdata-v3-pro-licensing"

# Find the git project root directory
GIT_ROOT=$(git rev-parse --show-toplevel)

# Get the git SHA
GIT_SHA=$(git rev-parse --short HEAD)

# Change to the service directory where the Dockerfile is located
cd $GIT_ROOT/influxdb3_license/service

# Define the image name and tags
IMAGE_NAME="us-east4-docker.pkg.dev/${PROJECT_ID}/influxdb3-pro-licensing/server"
SHA_TAG="${IMAGE_NAME}:${GIT_SHA}"
LATEST_TAG="${IMAGE_NAME}:latest"

# Build the Docker image using docker buildx and tag it with both SHA and latest
docker buildx build --platform linux/amd64 -t "${SHA_TAG}" -t "${LATEST_TAG}" .

# Push both tags to the private Google artifact registry
docker push "${SHA_TAG}"
docker push "${LATEST_TAG}"

echo "Successfully pushed image with tags:"
echo "- ${SHA_TAG}"
echo "- ${LATEST_TAG}"