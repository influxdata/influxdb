FROM ioft/i386-ubuntu:xenial

RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
    wget \
    mercurial \
    git && \
    rm -rf /var/lib/apt/lists/*

# Install go
ENV GOPATH /go
ENV GO_VERSION 1.9.2
ENV GO_ARCH 386
RUN wget --no-verbose -q https://storage.googleapis.com/golang/go${GO_VERSION}.linux-${GO_ARCH}.tar.gz && \
   tar -C /usr/local/ -xf /go${GO_VERSION}.linux-${GO_ARCH}.tar.gz && \
   mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH" && \
   rm /go${GO_VERSION}.linux-${GO_ARCH}.tar.gz
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH
