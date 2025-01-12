#!/bin/sh

# Fast fail the script on failures
set -e

# This script is used to build and deploy the InfluxDB 3 Enterprise
# trial license service to GCP Cloud Run. Currently, the process is
# a simple but manual process. The script will build the Docker image,
# push it to GCP Container Registry, and deploy it to Cloud Run.

# Runnning this script will requires:
# - gcloud CLI to be installed locally
# - gcloud CLI to be authenticated with the correct GCP project
# - Docker to be installed locally

# Confirm that the user wants to build and deploy the service
echo "This script will build and deploy the InfluxDB 3 Enterprise trial license service to GCP Cloud Run."
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

# Change to the service directory where the Dockerfile is located
cd $GIT_ROOT/influxdb3_license/service

# Build the Docker image using docker buildx and tag it
docker buildx build --platform linux/amd64 -t us-east4-docker.pkg.dev/${PROJECT_ID}/influxdb3-pro-licensing/server:latest .

# Push the Docker image to a private Google artifact registry
docker push us-east4-docker.pkg.dev/${PROJECT_ID}/influxdb3-pro-licensing/server:latest

# Deploy the Docker image to Cloud Run
gcloud run deploy influxdb3-pro-licensing \
  --region us-central1 \
  --project influxdata-v3-pro-licensing \
  --image us-east4-docker.pkg.dev/influxdata-v3-pro-licensing/influxdb3-pro-licensing/server:latest \
  --labels="git_sha=$(git rev-parse HEAD)" \
  --tag="git-sha-$(git rev-parse --short HEAD)"