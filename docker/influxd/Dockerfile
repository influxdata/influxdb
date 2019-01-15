FROM debian:stable-slim
COPY influxd /usr/bin/influxd
COPY influx /usr/bin/influx

EXPOSE 9999

COPY docker/influxd/entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
CMD ["influxd"]
