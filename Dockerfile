#syntax=docker/dockerfile:1.2
ARG RUST_VERSION=1.92
FROM rust:${RUST_VERSION}-slim-bookworm as build

# cache mounts below may already exist and owned by root
USER root

RUN apt update \
    && apt install --yes binutils build-essential curl pkg-config libssl-dev clang lld git patchelf protobuf-compiler zstd libz-dev \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

RUN mkdir /influxdb3
WORKDIR /influxdb3

ARG CARGO_INCREMENTAL=yes
ARG CARGO_NET_GIT_FETCH_WITH_CLI=false
ARG PROFILE=release
#ARG FEATURES=aws,gcp,azure,jemalloc_replacing_malloc
ARG FEATURES=aws,jemalloc_replacing_malloc
ARG PACKAGE=influxdb3
ARG PBS_DATE=unset
ARG PBS_VERSION=unset
ARG PBS_TARGET=unset
ARG TARGETARCH
ENV CARGO_INCREMENTAL=$CARGO_INCREMENTAL \
    CARGO_NET_GIT_FETCH_WITH_CLI=$CARGO_NET_GIT_FETCH_WITH_CLI \
    PROFILE=$PROFILE \
    FEATURES=$FEATURES \
    PACKAGE=$PACKAGE \
    PBS_TARGET=$PBS_TARGET \
    PBS_DATE=$PBS_DATE \
    JEMALLOC_SYS_WITH_LG_PAGE=16 \
    CARGO_BUILD_JOBS=1 \
    PBS_VERSION=$PBS_VERSION

RUN case "${TARGETARCH}" in \
        "amd64")  export PBS_TARGET="x86_64-unknown-linux-gnu" ;; \
        "arm64") export PBS_TARGET="aarch64-unknown-linux-gnu" ;; \
        *) echo "unsupported architecture ${TARGETARCH}" && exit 1 ;; \
    esac && echo "PBS_TARGET=${PBS_TARGET}" > /tmp/pbs.properties
# obtain python-build-standalone and configure PYO3_CONFIG_FILE
COPY .circleci /influxdb3/.circleci
RUN . /tmp/pbs.properties && \
  sed -i "s/^readonly TARGETS=.*/readonly TARGETS=${PBS_TARGET}/" ./.circleci/scripts/fetch-python-standalone.bash && \
  ./.circleci/scripts/fetch-python-standalone.bash /influxdb3/python-artifacts "${PBS_DATE}" "${PBS_VERSION}" && \
  tar -C /influxdb3/python-artifacts -zxf /influxdb3/python-artifacts/all.tar.gz "./${PBS_TARGET}" && \
  sed -i 's#tmp/workspace#influxdb3#' "/influxdb3/python-artifacts/${PBS_TARGET}/pyo3_config_file.txt" && \
  cat "/influxdb3/python-artifacts/${PBS_TARGET}/pyo3_config_file.txt"

COPY . /influxdb3
#COPY corporateproxy.crt /usr/local/share/ca-certificates/
RUN update-ca-certificates

RUN rustup toolchain install

RUN . /tmp/pbs.properties && \
    PYO3_CONFIG_FILE="/influxdb3/python-artifacts/$PBS_TARGET/pyo3_config_file.txt" cargo build --target-dir /influxdb3/target --package="$PACKAGE" --profile="$PROFILE" --no-default-features --features="$FEATURES" && \
    objcopy --compress-debug-sections "target/$PROFILE/$PACKAGE" && \
    cp "/influxdb3/target/$PROFILE/$PACKAGE" "/root/$PACKAGE" && \
    patchelf --set-rpath '$ORIGIN/python/lib:$ORIGIN/../lib/influxdb3/python/lib' "/root/$PACKAGE" && \
    cp -a "/influxdb3/python-artifacts/$PBS_TARGET/python" /root/python && \
    du -cshx /usr/local/rustup /usr/local/cargo/registry /usr/local/cargo/git /influxdb3/target


FROM debian:bookworm-slim

RUN apt update \
    && apt install --yes ca-certificates gettext-base libssl3 wget curl --no-install-recommends \
    && rm -rf /var/lib/{apt,dpkg,cache,log} \
    && groupadd --gid 1500 influxdb3 \
    && useradd --uid 1500 --gid influxdb3 --shell /bin/bash --create-home influxdb3

RUN mkdir /var/lib/influxdb3 && \
    chown influxdb3:influxdb3 /var/lib/influxdb3

RUN mkdir -p /usr/lib/influxdb3
COPY --from=build /root/python /usr/lib/influxdb3/python
RUN chown -R root:root /usr/lib/influxdb3

RUN mkdir /plugins && \
    chown influxdb3:influxdb3 /plugins

USER influxdb3

RUN mkdir ~/.influxdb3

ARG PACKAGE=influxdb3
ENV PACKAGE=$PACKAGE
ENV INFLUXDB3_PLUGIN_DIR=/plugins

COPY --from=build "/root/$PACKAGE" "/usr/bin/$PACKAGE"
COPY docker/entrypoint.sh /usr/bin/entrypoint.sh

EXPOSE 8181

ENV INFLUXDB3_LOG_FILTER=info

ENTRYPOINT ["/usr/bin/entrypoint.sh"]

CMD ["serve"]
