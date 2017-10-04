ARG GO_VERSION
FROM golang:${GO_VERSION}-alpine

RUN apk add --no-cache \
      asciidoc \
      bash \
      git \
      openssh-client \
      make \
      tar \
      xmlto

# Build the gdm binary and then clean out /go.
RUN go get github.com/sparrc/gdm && \
      mv /go/bin/gdm /usr/local/bin/gdm && \
      rm -rf /go/*

COPY fs/ /

ENTRYPOINT ["influxdb_tarball.bash"]
