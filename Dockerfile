#syntax=docker/dockerfile:1.2
ARG RUST_VERSION=1.57
FROM rust:${RUST_VERSION}-slim-bullseye as build

# cache mounts below may already exist and owned by root
USER root

RUN apt update \
    && apt install --yes binutils build-essential pkg-config libssl-dev clang \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

# Build influxdb_iox
COPY . /influxdb_iox
WORKDIR /influxdb_iox

ARG CARGO_INCREMENTAL=yes
ARG CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1
ARG CARGO_PROFILE_RELEASE_LTO=thin
ARG FEATURES=aws,gcp,azure,jemalloc_replacing_malloc
ARG ROARING_ARCH="haswell"
ARG RUSTFLAGS=""
ENV CARGO_INCREMENTAL=$CARGO_INCREMENTAL \
    CARGO_PROFILE_RELEASE_CODEGEN_UNITS=$CARGO_PROFILE_RELEASE_CODEGEN_UNITS \
    CARGO_PROFILE_RELEASE_LTO=$CARGO_PROFILE_RELEASE_LTO \
    FEATURES=$FEATURES \
    ROARING_ARCH=$ROARING_ARCH \
    RUSTFLAGS=$RUSTFLAGS

RUN \
  --mount=type=cache,id=influxdb_iox_registry,sharing=locked,target=/usr/local/cargo/registry \
  --mount=type=cache,id=influxdb_iox_git,sharing=locked,target=/usr/local/cargo/git \
  --mount=type=cache,id=influxdb_iox_target,sharing=locked,target=/influxdb_iox/target \
    du -cshx /usr/local/cargo/registry /usr/local/cargo/git /influxdb_iox/target && \
    cargo build --target-dir /influxdb_iox/target --release --no-default-features --features="$FEATURES" && \
    objcopy --compress-debug-sections target/release/influxdb_iox && \
    cp /influxdb_iox/target/release/influxdb_iox /root/influxdb_iox && \
    du -cshx /usr/local/cargo/registry /usr/local/cargo/git /influxdb_iox/target



FROM debian:bullseye-slim

RUN apt update \
    && apt install --yes ca-certificates gettext-base libssl1.1 --no-install-recommends \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

RUN groupadd --gid 1500 iox \
    && useradd --uid 1500 --gid iox --shell /bin/bash --create-home iox

USER iox

RUN mkdir ~/.influxdb_iox
RUN ls -la ~/.influxdb_iox

COPY --from=build /root/influxdb_iox /usr/bin/influxdb_iox
COPY docker/entrypoint.sh /usr/bin/entrypoint.sh

ENV INFLUXDB_IOX_SERVER_MODE=database

EXPOSE 8080 8082

ENTRYPOINT ["/usr/bin/entrypoint.sh"]

CMD ["run", "$INFLUXDB_IOX_SERVER_MODE"]
