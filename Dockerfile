#syntax=docker/dockerfile:1.2
ARG RUST_VERSION=1.57
FROM rust:${RUST_VERSION}-slim-bullseye as build

# cache mounts below may already exist and owned by root
USER root

RUN apt update \
    && apt install --yes binutils build-essential pkg-config libssl-dev clang lld git protobuf-compiler \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

# Build influxdb_iox
COPY . /influxdb_iox
WORKDIR /influxdb_iox

ARG CARGO_INCREMENTAL=yes
ARG PROFILE=release
ARG FEATURES=aws,gcp,azure,jemalloc_replacing_malloc
ARG PACKAGE=influxdb_iox
ARG RUSTFLAGS=""
ENV CARGO_INCREMENTAL=$CARGO_INCREMENTAL \
    PROFILE=$PROFILE \
    FEATURES=$FEATURES \
    PACKAGE=$PACKAGE \
    RUSTFLAGS=$RUSTFLAGS

RUN \
  --mount=type=cache,id=influxdb_iox_rustup,sharing=locked,target=/usr/local/rustup \
  --mount=type=cache,id=influxdb_iox_registry,sharing=locked,target=/usr/local/cargo/registry \
  --mount=type=cache,id=influxdb_iox_git,sharing=locked,target=/usr/local/cargo/git \
  --mount=type=cache,id=influxdb_iox_target,sharing=locked,target=/influxdb_iox/target \
    du -cshx /usr/local/rustup /usr/local/cargo/registry /usr/local/cargo/git /influxdb_iox/target && \
    cargo build --target-dir /influxdb_iox/target --package="$PACKAGE" --profile="$PROFILE" --no-default-features --features="$FEATURES" && \
    objcopy --compress-debug-sections "target/$PROFILE/$PACKAGE" && \
    cp "/influxdb_iox/target/$PROFILE/$PACKAGE" /root/$PACKAGE && \
    du -cshx /usr/local/rustup /usr/local/cargo/registry /usr/local/cargo/git /influxdb_iox/target



FROM debian:bullseye-slim

RUN apt update \
    && apt install --yes ca-certificates gettext-base libssl1.1 --no-install-recommends \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

RUN groupadd --gid 1500 iox \
    && useradd --uid 1500 --gid iox --shell /bin/bash --create-home iox

USER iox

RUN mkdir ~/.influxdb_iox
RUN ls -la ~/.influxdb_iox

ARG PACKAGE=influxdb_iox
ENV PACKAGE=$PACKAGE

COPY --from=build "/root/$PACKAGE" "/usr/bin/$PACKAGE"
COPY docker/entrypoint.sh /usr/bin/entrypoint.sh


EXPOSE 8080 8082

ENTRYPOINT ["/usr/bin/entrypoint.sh"]

CMD ["run"]
