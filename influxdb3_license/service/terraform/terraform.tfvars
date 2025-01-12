project_id = "influxdata-v3-pro-licensing"
#container_image = "quay.io/influxdb/influxdb3-license-service:latest"
container_image = "us-east4-docker.pkg.dev/influxdata-v3-pro-licensing/influxdb3-pro-licensing/server:latest"
region          = "us-central1" # This is optional since it matches the default in variables.tf
environment     = "prod"        # This is optional since it matches the default in variables.tf