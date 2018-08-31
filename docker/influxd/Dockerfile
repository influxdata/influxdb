FROM debian:stable-slim
COPY influxd /usr/bin/influxd

EXPOSE 9999

COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
CMD ["influxd"]
