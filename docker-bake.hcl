target "img" {
  context = "."
  dockerfile = "Dockerfile"
  args = {
    PBS_DATE = "20260610"
    PBS_VERSION = "3.13.14"
  }
  platforms = ["linux/arm64", "linux/amd64"]
  tags = [
    "docker.io/localhost/influxdb:3.11.0"
  ]
  output = [
    "type=oci,dest=./artifacts/influxdb3-mulitarch.tar",
    "type=image,push=true"
  ]
}

target "img-amd64" {
  inherits = ["img"]
  platforms = ["linux/amd64"]
  args = {
    PBS_TARGET = "x86_64-unknown-linux-gnu"
  }
  tags = [
    "docker.io/localhost/influxdb:3.11.0-amd64"
  ]
  output = ["type=oci,dest=./artifacts/influxdb3-mulitarch.tar"]
}

target "img-arm64" {
  inherits = ["img"]
  platforms = ["linux/arm64"]
  args = {
    PBS_TARGET = "aarch64-unknown-linux-gnu"
  }
  tags = [
    "docker.io/localhost/influxdb:3.11.0-arm64"
  ]
  output = ["type=oci,dest=./artifacts/influxdb3-mulitarch.tar"]
}
