ARG GO_VERSION
FROM golang:${GO_VERSION}-alpine

RUN apk add --no-cache \
      bash \
      git \
      openssh-client \
      tar

# Build the gdm binary and then clean out /go.
RUN go get github.com/sparrc/gdm && \
      mv /go/bin/gdm /usr/local/bin/gdm && \
      rm -rf /go/*

COPY fs/ /

ENTRYPOINT ["influxdb_tarball.bash"]
