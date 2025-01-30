FROM rust:1.84-slim-bookworm

# Install system dependencies
RUN apt-get update && apt-get install --no-install-recommends -y \
    binutils \
    build-essential \
    pkg-config \
    libssl-dev \
    clang \
    lld \
    git \
    protobuf-compiler \
    python3 \
    python3-full \
    python3-dev \
    python3-pip \
    curl \
    cmake \
    protobuf-compiler-grpc \
    libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv installer
ADD https://astral.sh/uv/install.sh /uv-installer.sh
RUN sh /uv-installer.sh && rm /uv-installer.sh
ENV PATH="/root/.local/bin/:$PATH"

# Install nextest
RUN curl -LsSf https://get.nexte.st/latest/linux-arm | tar zxf - -C ${CARGO_HOME:-~/.cargo}/bin

# Set working directory
WORKDIR /app

# Set environment variables
ENV CARGO_INCREMENTAL=yes \
    CARGO_NET_GIT_FETCH_WITH_CLI=false \
    PROFILE=release \
    FEATURES=aws,gcp,azure,jemalloc_replacing_malloc,system-py \
    PACKAGE=influxdb3