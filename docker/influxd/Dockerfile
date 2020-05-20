FROM debian:stable-slim

EXPOSE 9999

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update \
	&& apt-get install -y \
	ca-certificates \
	tzdata \
	&& apt-get clean autoclean \
	&& apt-get autoremove --yes \
	&& rm -rf /var/lib/{apt,dpkg,cache,log}

COPY docker/influxd/entrypoint.sh /entrypoint.sh
COPY influx influxd /usr/bin/

ENTRYPOINT ["/entrypoint.sh"]
CMD ["influxd"]
