FROM ruby:2.6-alpine3.9

RUN apk add --no-cache \
      bash \
      jq \
      gcc \
      make \
      musl-dev \
      rpm \
      ruby-dev \
      tar \
      zip

RUN gem install fpm -v 1.11.0

COPY fs/ /

ENTRYPOINT ["influxdb_packages.bash"]
