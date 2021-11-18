#syntax=docker/dockerfile:1.2
FROM rust:1.56-slim-bullseye as build

# cache mounts below may already exist and owned by root
USER root

RUN apt update \
    && apt install --yes build-essential pkg-config libssl-dev clang \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

# Build influxdb_iox
COPY . /influxdb_iox
WORKDIR /influxdb_iox

RUN \
  --mount=type=cache,id=influxdb_iox_registry,sharing=locked,target=/usr/local/cargo/registry \
  --mount=type=cache,id=influxdb_iox_git,sharing=locked,target=/usr/local/cargo/git \
  --mount=type=cache,id=influxdb_iox_target,sharing=locked,target=/influxdb_iox/target \
  du -cshx /usr/local/cargo/registry /usr/local/cargo/git /influxdb_iox/target && \
  cargo build --target-dir /influxdb_iox/target --release --no-default-features --features=aws,gcp,azure && \
  cp /influxdb_iox/target/release/influxdb_iox /root/influxdb_iox && \
  du -cshx /usr/local/cargo/registry /usr/local/cargo/git /influxdb_iox/target

FROM debian:bullseye-slim

RUN apt update \
    && apt install --yes libssl1.1 ca-certificates --no-install-recommends \
	&& rm -rf /var/lib/{apt,dpkg,cache,log}

RUN groupadd --gid 1500 iox \
  && useradd --uid 1500 --gid iox --shell /bin/bash --create-home iox

USER iox

RUN mkdir ~/.influxdb_iox
RUN ls -la ~/.influxdb_iox

COPY --from=build /root/influxdb_iox /usr/bin/influxdb_iox

EXPOSE 8080 8082

ENTRYPOINT ["/usr/bin/influxdb_iox"]

CMD ["run", "database"]
