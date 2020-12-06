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
        file \
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
    sudo rm -rf rm -rf /var/lib/apt/lists/*

WORKDIR /tmp

# Install native musl.
ENV MUSL_VERSION=1.1.24
RUN curl -O -k https://musl.libc.org/releases/musl-${MUSL_VERSION}.tar.gz -O && \
    tar xzf musl-${MUSL_VERSION}.tar.gz && \
    cd musl-${MUSL_VERSION} && \
    ./configure && \
    make && \
    sudo make install && \
    cd .. && \
    rm -rf musl-${MUSL_VERSION}*
ENV PATH=/usr/local/musl/bin:${PATH}

## Install cross-compilers.

# Linux.
#
# TODO: This step alone takes around an hour, we should consider caching the outputs
# someplace and changing this step to just download & extract the tarballs.
ENV MUSL_CROSS_MAKE_VERSION=v0.9.9
RUN git clone https://github.com/richfelker/musl-cross-make.git && \
    cd musl-cross-make && \
    git checkout ${MUSL_CROSS_MAKE_VERSION} && \
    make DL_CMD='curl -C - -L -k -o' MUSL_VER=${MUSL_VERSION} TARGET=aarch64-unknown-linux-musl install && \
    make DL_CMD='curl -C - -L -k -o' MUSL_VER=${MUSL_VERSION} TARGET=armv6-unknown-linux-musleabihf GCC_CONFIG='--with-arch=armv6+fp' install && \
    make DL_CMD='curl -C - -L -k -o' MUSL_VER=${MUSL_VERSION} TARGET=armv7-unknown-linux-musleabihf GCC_CONFIG='--with-arch=armv7+fp' install && \
    sudo mv ./output /usr/local/musl-cross && \
    cd .. && \
    rm -rf musl-cross-make
ENV PATH=/usr/local/musl-cross/bin:${PATH}

# macOS.
#
# TODO: For some reason, this breaks if I move the output out of the git repo and then delete the git dir.
# It'd be nice for build time and final image size if we could fix that problem.
ENV OSX_CROSS_VERSION=c2ad5e859d12a295c3f686a15bd7181a165bfa82
RUN sudo git clone https://github.com/tpoechtrager/osxcross.git /usr/local/osxcross && \
    cd /usr/local/osxcross && \
    sudo git checkout ${OSX_CROSS_VERSION} && \
    sudo curl -L -o ./tarballs/MacOSX10.11.sdk.tar.xz https://macos-sdks.s3.amazonaws.com/MacOSX10.11.sdk.tar.xz && \
    sudo UNATTENDED=1 PORTABLE=true OCDEBUG=1 ./build.sh && \
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
ENV CARGO_TARGET_ARM7_UNKNOWN_LINUX_MUSLEABIHF_LINKER=/usr/local/musl-cross/armv7-unknown-linux-musleabihf-gcc
ENV CARGO_TARGET_X86_64_APPLE_DARWIN_LINKER=/usr/local/osxcross/target/bin/x86_64-apple-darwin15-clang

WORKDIR /go

# Install goreleaser.
ENV GORELEASER_VERSION=v0.142.0
RUN curl -sfL -o goreleaser-install https://install.goreleaser.com/github.com/goreleaser/goreleaser.sh && \
    sh goreleaser-install ${GORELEASER_VERSION} && \
    rm goreleaser-install

# Install our cross-compiler.
COPY --chown=circleci:circleci ./xcc.sh /go/bin/xcc
RUN chmod a+x /go/bin/xcc
