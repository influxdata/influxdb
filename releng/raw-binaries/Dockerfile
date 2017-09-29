ARG GO_VERSION
FROM golang:${GO_VERSION}

RUN apt-get update && apt-get install -y --no-install-recommends \
  jq \
  && rm -rf /var/lib/apt/lists/*

COPY fs/ /

ENTRYPOINT ["influxdb_raw_binaries.bash"]
