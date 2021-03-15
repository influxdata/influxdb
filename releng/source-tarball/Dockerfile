ARG GO_VERSION
FROM golang:${GO_VERSION}

# Install system dependencies + cross build system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    asciidoc \
    bzr \
    clang \
    cmake \
    jq \
    libprotobuf-dev \
    libssl-dev \
    libxml2-dev \
    llvm-dev \
    lzma-dev \
    patch \
    protobuf-compiler \
    xmlto \
    xz-utils \
    zlib1g-dev \
  && rm -rf /var/lib/apt/lists/*

# Install rust
RUN curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain stable -y \
 && . $HOME/.cargo/env && rustup target add x86_64-apple-darwin x86_64-unknown-linux-musl

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

# Wrapper to pull a version of pkg-config at runtime
ENV PKG_CONFIG=/usr/local/bin/pkg-config.sh

# install go-junit-report for unit tests
RUN GO111MODULE=on go get github.com/jstemmer/go-junit-report@v0.9.1

COPY fs/ /

ENTRYPOINT ["influxdb_tarball.bash"]
