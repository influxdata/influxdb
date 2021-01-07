FROM cimg/go:1.15.6-browsers

WORKDIR /tmp

# Install libc and cross-compilers for our target platforms.
ENV MUSL_VERSION=1.1.24
ENV MUSL_BUILD_TIME="20210105214054"

ENV MUSL_CROSS_MAKE_VERSION=0.9.9
ENV MUSL_CROSS_ARM64_BUILD_TIME="20210105204119"

ENV OSXCROSS_VERSION=c2ad5e859d12a295c3f686a15bd7181a165bfa82
ENV OSXCROSS_BUILD_TIME="20210105214145"

RUN sudo apt-get update && \
    sudo apt-get install -y --no-install-recommends \
        clang \
        cmake \
        libssl-dev \
        libxml2-dev \
        llvm-dev \
        lzma-dev \
        zlib1g-dev && \
    sudo rm -rf /var/lib/apt/lists/*

# AMD64 musl
# Archive is built using `./xcc-builds/build-amd64-musl-gcc.sh`
ENV MUSL_ARCHIVE=musl-${MUSL_VERSION}-${MUSL_BUILD_TIME}.tar.gz
RUN curl https://dl.influxdata.com/influxdb-ci/musl/${MUSL_VERSION}/${MUSL_ARCHIVE} -O && \
    sudo tar xzf ${MUSL_ARCHIVE} -C /usr/local && \
    rm ${MUSL_ARCHIVE}
ENV PATH=/usr/local/musl/bin:${PATH}

# ARM64 musl
# Archive is built using `./xcc-builds/build-aarch64-musl-gcc.sh`
ENV MUSL_CROSS_ARM64_ARCHIVE=musl-${MUSL_VERSION}-cross-aarch64-${MUSL_CROSS_MAKE_VERSION}-${MUSL_CROSS_ARM64_BUILD_TIME}.tar.gz
RUN curl https://dl.influxdata.com/influxdb-ci/musl/${MUSL_VERSION}/musl-cross/${MUSL_CROSS_MAKE_VERSION}/${MUSL_CROSS_ARM64_ARCHIVE} -O && \
    sudo tar xzf ${MUSL_CROSS_ARM64_ARCHIVE} -C /usr/local && \
    rm ${MUSL_CROSS_ARM64_ARCHIVE}
ENV PATH=/usr/local/musl-cross/bin:${PATH}

# macOS clang.
# Archive is built using `./xcc-builds/build-osxcross-clang.sh`
ENV OSXCROSS_ARCHIVE=osxcross-${OSXCROSS_VERSION}-${OSXCROSS_BUILD_TIME}.tar.gz
RUN curl https://dl.influxdata.com/influxdb-ci/osxcross/${OSXCROSS_VERSION}/${OSXCROSS_ARCHIVE} -O && \
    sudo tar xzf ${OSXCROSS_ARCHIVE} -C /usr/local && \
    rm ${OSXCROSS_ARCHIVE}
ENV PATH=/usr/local/osxcross/target/bin:${PATH}

# Install the rust compiler.
RUN curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain stable -y
ENV PATH=/home/circleci/.cargo/bin:${PATH}

# Install rust toolchains for cross-building.
RUN rustup target add \
        x86_64-unknown-linux-musl \
        aarch64-unknown-linux-musl \
        x86_64-apple-darwin

# Configure linkers for new targets.
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=/usr/local/musl/bin/musl-gcc
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=/usr/local/musl-cross/bin/aarch64-unknown-linux-musl-gcc
ENV CARGO_TARGET_X86_64_APPLE_DARWIN_LINKER=/usr/local/osxcross/target/bin/x86_64-apple-darwin15-clang

# Install Docker buildkit to support multi-arch image builds.
ENV DOCKER_BUILDX_VERSION=0.5.1
RUN mkdir -p /home/circleci/.docker/cli-plugins && \
    curl -sfL -o /home/circleci/.docker/cli-plugins/docker-buildx \
        https://github.com/docker/buildx/releases/download/v${DOCKER_BUILDX_VERSION}/buildx-v${DOCKER_BUILDX_VERSION}.linux-amd64 && \
    chmod a+x /home/circleci/.docker/cli-plugins/docker-buildx

WORKDIR $GOPATH

# Install goreleaser.
ENV GORELEASER_VERSION=v0.152.0
RUN curl -sfL -o goreleaser-install https://install.goreleaser.com/github.com/goreleaser/goreleaser.sh && \
    sh goreleaser-install ${GORELEASER_VERSION} && \
    rm goreleaser-install

# Install our cross-compiler.
COPY --chown=circleci:circleci ./xcc.sh $GOPATH/bin/xcc
RUN chmod a+x $GOPATH/bin/xcc

# Install build and test dependencies.
RUN sudo apt-get update && \
    sudo apt-get install -y --no-install-recommends \
        bzr \
        libprotobuf-dev \
        netcat-openbsd \
        pkg-config \
        protobuf-compiler && \
    sudo rm -rf /var/lib/apt/lists/*
