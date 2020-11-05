FROM ubuntu:20.04 AS dbuild

ENV DEBIAN_FRONTEND noninteractive

RUN apt update
RUN apt install --yes \
        cargo \
        curl \
        git \
        golang \
        libclang-dev \
        llvm-dev \
        make \
        protobuf-compiler \
        ragel \
        rustc

FROM dbuild AS dshell

ARG USERID=1000
RUN adduser --quiet --home /code --uid ${USERID} --disabled-password --gecos "" influx
USER influx

ENTRYPOINT [ "/bin/bash" ]

FROM dbuild AS dbuild-all

COPY . /code
WORKDIR /code
RUN make

##
# InfluxDB Image (Monolith)
##
FROM debian:stretch-slim AS influx

COPY --from=dbuild-all /code/bin/linux/influxd /usr/bin/influxd
COPY --from=dbuild-all /code/bin/linux/influx /usr/bin/influx

EXPOSE 8086

ENTRYPOINT [ "/usr/bin/influxd" ]

##
# InfluxDB UI Image
##
FROM nginx:alpine AS ui

EXPOSE 80

COPY --from=dbuild-all /code/ui /usr/share/nginx/html
