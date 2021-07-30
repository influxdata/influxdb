ARG GO_VERSION
FROM golang:${GO_VERSION}

# Install system dependencies + cross build system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    asciidoc \
    build-essential \
    bzr \
    clang \
    cmake \
    jq \
    libprotobuf-dev \
    libssl-dev \
    libxml2-dev \
    llvm-dev \
    lzma-dev \
    mingw-w64 \
    patch \
    protobuf-compiler \
    xmlto \
    xz-utils \
    zlib1g-dev \
  && rm -rf /var/lib/apt/lists/*

# Install rust
ENV RUST_VERSION=1.52.1
RUN curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain ${RUST_VERSION} -y \
  && . $HOME/.cargo/env && rustup target add \
    aarch64-unknown-linux-musl \
    x86_64-apple-darwin \
    x86_64-pc-windows-gnu \
    x86_64-unknown-linux-musl

ENV PATH="/root/.cargo/bin:${PATH}"

# Install osxcross
# The default osxcross deployment target is 10.6, which doesn't support thread local variables.
ENV MACOSX_DEPLOYMENT_TARGET=10.11
# The `git checkout` line specifies the same commit of osxcross that flux CI runs against.
# It doesn't necessarily have to match but it would be good to keep things similar.
RUN mkdir -p /opt/osxcross && \
    cd /opt && \
    git clone https://github.com/tpoechtrager/osxcross.git && \
    cd osxcross && \
    git checkout c2ad5e859d12a295c3f686a15bd7181a165bfa82 && \
    curl -L -o ./tarballs/MacOSX${MACOSX_DEPLOYMENT_TARGET}.sdk.tar.xz \
        https://macos-sdks.s3.amazonaws.com/MacOSX${MACOSX_DEPLOYMENT_TARGET}.sdk.tar.xz && \
    UNATTENDED=1 PORTABLE=true OCDEBUG=1 ./build.sh && \
    rm -rf .git build tarballs

ENV PATH="/opt/osxcross/target/bin:${PATH}"

RUN cd /tmp && \
    git clone https://github.com/richfelker/musl-cross-make.git && \
    cd musl-cross-make && \
    git checkout v0.9.9 && \
    make MUSL_VER=1.1.24 TARGET=aarch64-unknown-linux-musl DL_CMD="curl -C - -L -o" install && \
    mv output /opt/musl-cross && \
    rm -rf /tmp/musl-cross-make

ENV PATH="/opt/musl-cross/bin:$PATH"

# Make sure we have the newer version of pkg-config that works with windows,
# and install go-junit-report for unit tests
RUN GO111MODULE=on go get github.com/jstemmer/go-junit-report@v0.9.1 github.com/influxdata/pkg-config@v0.2.7

COPY fs/ /

ENTRYPOINT ["influxdb_raw_binaries.bash"]
