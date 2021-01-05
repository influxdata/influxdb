FROM circleci/golang:1.15-node-browsers

WORKDIR /tmp

ENV MUSL_VERSION=1.1.24
ENV MUSL_CROSS_MAKE_VERSION=0.9.9

RUN sudo apt-get update && \
    sudo apt-get install -y --no-install-recommends \
        patch && \
    sudo rm -rf /var/lib/apt/lists/*

# Install musl cross-compiler for ARM64.
# The archive downloaded below was built using these commands:
#RUN git clone https://github.com/richfelker/musl-cross-make.git && \
#    cd musl-cross-make && \
#    git checkout v${MUSL_CROSS_MAKE_VERSION} && \
#    make MUSL_VER=${MUSL_VERSION} TARGET=aarch64-unknown-linux-musl install && \
#    mv output musl-cross && \
#    tar czf /tmp/musl-cross-aarch64-${MUSL_CROSS_MAKE_VERSION}.tar.gz musl && \
#    cd .. && \
#    rm -rf musl-cross-make
RUN curl https://dl.influxdata.com/influxdb-ci/musl/${MUSL_VERSION}/musl-cross/${MUSL_CROSS_MAKE_VERSION}/musl-cross-aarch64-${MUSL_CROSS_MAKE_VERSION}.tar.gz -O && \
    sudo tar xzf musl-cross-aarch64-${MUSL_CROSS_MAKE_VERSION}.tar.gz -C /usr/local && \
    rm musl-cross-aarch64-${MUSL_CROSS_MAKE_VERSION}.tar.gz

ENV PATH=/usr/local/musl-cross/bin:${PATH}

# Install native musl.
# The archive downloaded below was built using these commands:
#RUN curl https://musl.libc.org/releases/musl-${MUSL_VERSION}.tar.gz -O && \
#    tar xzf musl-${MUSL_VERSION}.tar.gz && \
#    cd musl-${MUSL_VERSION} && \
#    ./configure && \
#    make && \
#    sudo make install && \
#    tar czf /tmp/musl-${MUSL_VERSION}.tar.gz /usr/local/musl
#    cd .. && \
#    rm -rf musl-${MUSL_VERSION}*
RUN curl https://dl.influxdata.com/influxdb-ci/musl/${MUSL_VERSION}/musl-${MUSL_VERSION}.tar.gz -O && \
    sudo tar xzf musl-${MUSL_VERSION}.tar.gz -C /usr/local && \
    rm musl-${MUSL_VERSION}.tar.gz

ENV PATH=/usr/local/musl/bin:${PATH}

# Install macOS cross-compiler.
ENV OSX_CROSS_VERSION=c2ad5e859d12a295c3f686a15bd7181a165bfa82

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

# The archive downloaded below was built using these commands:
#RUN sudo git clone https://github.com/tpoechtrager/osxcross.git /usr/local/osxcross && \
#    cd /usr/local/osxcross && \
#    sudo git checkout ${OSX_CROSS_VERSION} && \
#    sudo curl -L -o ./tarballs/MacOSX10.11.sdk.tar.xz https://macos-sdks.s3.amazonaws.com/MacOSX10.11.sdk.tar.xz && \
#    sudo UNATTENDED=1 PORTABLE=true OCDEBUG=1 ./build.sh && \
#    sudo rm -rf .git build tarballs && \
#    cd /tmp && \
#    tar czf osxcross-${OSX_CROSS_VERSION}.tar.gz /usr/local/osxcross && \
#    sudo rm -rf /usr/local/osxcross
RUN curl https://dl.influxdata.com/influxdb-ci/osxcross/${OSX_CROSS_VERSION}/osxcross-${OSX_CROSS_VERSION}.tar.gz -O && \
    sudo tar xzf osxcross-${OSX_CROSS_VERSION}.tar.gz -C /usr/local && \
    rm osxcross-${OSX_CROSS_VERSION}.tar.gz

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

WORKDIR /go

# Install Docker buildkit to support multi-arch image builds.
ENV DOCKER_BUILDX_VERSION=0.5.1

RUN mkdir -p /home/circleci/.docker/cli-plugins && \
    curl -sfL -o /home/circleci/.docker/cli-plugins/docker-buildx \
        https://github.com/docker/buildx/releases/download/v${DOCKER_BUILDX_VERSION}/buildx-v${DOCKER_BUILDX_VERSION}.linux-amd64 && \
    chmod a+x /home/circleci/.docker/cli-plugins/docker-buildx

# Install goreleaser.
ENV GORELEASER_VERSION=v0.152.0
RUN curl -sfL -o goreleaser-install https://install.goreleaser.com/github.com/goreleaser/goreleaser.sh && \
    sh goreleaser-install ${GORELEASER_VERSION} && \
    rm goreleaser-install

# Install our cross-compiler.
COPY --chown=circleci:circleci ./xcc.sh /go/bin/xcc
RUN chmod a+x /go/bin/xcc

# Install build and test dependencies.
RUN sudo apt-get update && \
    sudo apt-get install -y --no-install-recommends \
        bzr \
        libprotobuf-dev \
        netcat-openbsd \
        pkg-config \
        protobuf-compiler && \
    sudo rm -rf /var/lib/apt/lists/*
