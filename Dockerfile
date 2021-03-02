#syntax=docker/dockerfile:1.2
FROM rust:slim-buster AS build

# Build flatbuffers, a dependency of influxdb_iox
ARG flatbuffers_version="v1.12.0"
RUN apt-get update \
  && apt-get install -y \
    git make clang cmake llvm libssl-dev pkg-config \
    --no-install-recommends \
  && git clone -b ${flatbuffers_version} -- https://github.com/google/flatbuffers.git /usr/local/src/flatbuffers \
  && cmake -S /usr/local/src/flatbuffers -B /usr/local/src/flatbuffers \
    -G "Unix Makefiles" \
    -DCMAKE_BUILD_TYPE=Release \
  && make -C /usr/local/src/flatbuffers -j $(nproc) flatc \
  && ln /usr/local/src/flatbuffers/flatc /usr/bin/flatc

# Build influxdb_iox
COPY . /influxdb_iox
WORKDIR /influxdb_iox
RUN \
  --mount=type=cache,id=influxdb_iox_rustup,sharing=locked,target=/usr/local/rustup \
  rustup component add rustfmt
RUN \
  --mount=type=cache,id=influxdb_iox_rustup,sharing=locked,target=/usr/local/rustup \
  --mount=type=cache,id=influxdb_iox_registry,sharing=locked,target=/usr/local/cargo/registry \
  --mount=type=cache,id=influxdb_iox_git,sharing=locked,target=/usr/local/cargo/git \
  --mount=type=cache,id=influxdb_iox_target,sharing=locked,target=/influxdb_iox/target \
  du -cshx /usr/local/rustup /usr/local/cargo/registry /usr/local/cargo/git /influxdb_iox/target && \
  cargo build --target-dir /influxdb_iox/target --release && \
  cp /influxdb_iox/target/release/influxdb_iox /root/influxdb_iox && \
  du -cshx /usr/local/rustup /usr/local/cargo/registry /usr/local/cargo/git /influxdb_iox/target

FROM debian:buster-slim

RUN apt-get update \
    && apt-get install -y libssl1.1 libgcc1 libc6 --no-install-recommends \
	&& rm -rf /var/lib/{apt,dpkg,cache,log}

RUN groupadd -g 1500 rust \
  && useradd -u 1500 -g rust -s /bin/bash -m rust

USER rust

RUN mkdir ~/.influxdb_iox
RUN ls -la ~/.influxdb_iox

COPY --from=build /root/influxdb_iox /usr/bin/influxdb_iox

EXPOSE 8080 8082

ENTRYPOINT ["/usr/bin/influxdb_iox"]
