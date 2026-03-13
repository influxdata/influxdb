target "img" {
  context = "."
  dockerfile = "Dockerfile"
  args = {
    PBS_DATE = "20260303"
    PBS_VERSION = "3.12.13"
  }
  platforms = ["linux/arm64", "linux/amd64"]
  tags = [
    "ghcr.io/adudek/influxdb:3.8.3"
  ]
  output = ["type=oci,dest=./artifacts/influxdb3-mulitarch.tar"]
}

target "img-amd64" {
  inherits = ["img"]
  platforms = ["linux/amd64"]
  args = {
    PBS_TARGET = "x86_64-unknown-linux-gnu"
  }
  tags = [
    "ghcr.io/adudek/influxdb:3.8.3-amd64"
  ]
  output = ["type=oci,dest=./artifacts/influxdb3-amd64.tar"]
}

target "img-arm64" {
  inherits = ["img"]
  platforms = ["linux/arm64"]
  args = {
    PBS_TARGET = "aarch64-unknown-linux-gnu"
  }
  tags = [
    "ghcr.io/adudek/influxdb:3.8.3-arm64"
  ]
  output = ["type=oci,dest=./artifacts/influxdb3-arm64.tar"]
}