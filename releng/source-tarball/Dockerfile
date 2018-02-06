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

# Build the dep binary and then clean out /go.
RUN go get github.com/golang/dep/cmd/dep && \
      mv /go/bin/dep /usr/local/bin/dep && \
      rm -rf /go/*

COPY fs/ /

ENTRYPOINT ["influxdb_tarball.bash"]
