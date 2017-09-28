ARG GO_VERSION
FROM golang:${GO_VERSION}-alpine

RUN apk add --no-cache \
      bash \
      jq \
      git

RUN go get -u github.com/jstemmer/go-junit-report && \
      mv /go/bin/go-junit-report /usr/bin/go-junit-report && \
      rm -rf /go/*

COPY fs/ /

ENTRYPOINT ["influxdb_prebuild_tests.bash"]
