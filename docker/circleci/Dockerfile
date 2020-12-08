FROM circleci/golang:1.15-node-browsers

# Install dependencies.
RUN sudo apt-get update && \
    sudo apt-get install -y --no-install-recommends \
        build-essential \
        bzr \
        ca-certificates \
        clang \
        cmake \
        curl \
        gnupg \
        libgmp-dev \
        libmpc-dev \
        libmpfr-dev \
        libssl-dev \
        libtinfo5 \
        libtool \
        libxml2-dev \
        llvm-dev \
        lzma-dev \
        netcat-openbsd \
        patch \
        xutils-dev \
        zlib1g-dev && \
    sudo rm -rf /var/lib/apt/lists/*

WORKDIR /tmp

# Install native musl, and cross-compilers.
ENV MUSL_VERSION=1.1.24
RUN curl https://musl.libc.org/releases/musl-${MUSL_VERSION}.tar.gz -O && \
    tar xzf musl-${MUSL_VERSION}.tar.gz && \
    cd musl-${MUSL_VERSION} && \
    ./configure && \
    make && \
    sudo make install && \
    cd .. && \
    rm -rf musl-${MUSL_VERSION}*
# TODO: Convert these to curl calls to download from S3.
ADD ./musl-cross-${MUSL_VERSION}-aarch64.tar.gz /usr/local
ADD ./musl-cross-${MUSL_VERSION}-armv6.tar.gz /usr/local
ADD ./musl-cross-${MUSL_VERSION}-armv7.tar.gz /usr/local
ENV PATH=/usr/local/musl/bin:/usr/local/musl-cross/bin:${PATH}

# Install macOS cross-compiler.
# TODO: Consider pre-building this and stashing the release archive in S3,
# especially if we start adding more SDKs for newer macOS versions / archs.
ENV OSX_CROSS_VERSION=c2ad5e859d12a295c3f686a15bd7181a165bfa82
RUN sudo git clone https://github.com/tpoechtrager/osxcross.git /usr/local/osxcross && \
    cd /usr/local/osxcross && \
    sudo git checkout ${OSX_CROSS_VERSION} && \
    sudo curl -L -o ./tarballs/MacOSX10.11.sdk.tar.xz https://macos-sdks.s3.amazonaws.com/MacOSX10.11.sdk.tar.xz && \
    sudo UNATTENDED=1 PORTABLE=true OCDEBUG=1 ./build.sh && \
    sudo rm -rf .git build tarballs && \
    cd /tmp
ENV PATH=/usr/local/osxcross/target/bin:${PATH}

# Install the rust compiler.
RUN curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain stable -y
ENV PATH=/home/circleci/.cargo/bin:${PATH}

# Install rust toolchains for cross-building.
RUN rustup target add \
        x86_64-unknown-linux-musl \
        aarch64-unknown-linux-musl \
        arm-unknown-linux-musleabihf \
        armv7-unknown-linux-musleabihf \
        x86_64-apple-darwin

# Configure linkers for new targets.
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=/usr/local/musl/bin/musl-gcc
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=/usr/local/musl-cross/bin/aarch64-unknown-linux-musl-gcc
ENV CARGO_TARGET_ARM_UNKNOWN_LINUX_MUSLEABIHF_LINKER=/usr/local/musl-cross/armv6-unknown-linux-musleabihf-gcc
ENV CARGO_TARGET_ARMV7_UNKNOWN_LINUX_MUSLEABIHF_LINKER=/usr/local/musl-cross/armv7-unknown-linux-musleabihf-gcc
ENV CARGO_TARGET_X86_64_APPLE_DARWIN_LINKER=/usr/local/osxcross/target/bin/x86_64-apple-darwin15-clang

# Set a work-around for a bug in cc-rs that prevents FPU from being set when needed on ARMv7.
ENV CFLAGS_armv7_unknown_linux_musleabihf='-mfpu=vfpv3-d16'

WORKDIR /go

# Install goreleaser.
ENV GORELEASER_VERSION=v0.142.0
RUN curl -sfL -o goreleaser-install https://install.goreleaser.com/github.com/goreleaser/goreleaser.sh && \
    sh goreleaser-install ${GORELEASER_VERSION} && \
    rm goreleaser-install

# Install our cross-compiler.
COPY --chown=circleci:circleci ./xcc.sh /go/bin/xcc
RUN chmod a+x /go/bin/xcc
