FROM debian:stable-slim
COPY influxd /usr/bin/influxd
COPY influx /usr/bin/influx

EXPOSE 9999

RUN apt-get update && apt-get install -y --no-install-recommends \
  ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY docker/influxd/entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
CMD ["influxd"]
