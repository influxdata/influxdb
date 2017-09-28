FROM ruby:2.4.1-alpine3.6

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

RUN gem install fpm -v 1.8.1

COPY fs/ /

ENTRYPOINT ["influxdb_packages.bash"]
