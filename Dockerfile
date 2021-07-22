FROM ubuntu:20.04 AS dbuild

ENV DEBIAN_FRONTEND noninteractive

# Needed for Yarn steps to veryify the keys
RUN apt update
RUN apt install --yes curl gnupg2
RUN curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add -
RUN echo "deb https://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list

# Now update index with Yarn
RUN apt update
RUN apt install --yes \
        cargo \
        git \
        golang \
        libclang-dev \
        llvm-dev \
        make \
        nodejs \
        protobuf-compiler \
        ragel \
        rustc \
        yarn

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

COPY --from=dbuild-all /code/ui/build /usr/share/nginx/html
