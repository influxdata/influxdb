FROM debian:stable-slim
COPY influx influxd /usr/bin/

EXPOSE 9999

ENV DEBIAN_FRONTEND noninteractive

COPY docker/influxd/entrypoint.sh /entrypoint.sh

RUN apt-get update \
	&& apt-get install -y \
	ca-certificates \
	tzdata \
	&& apt-get clean autoclean \
	&& apt-get autoremove --yes \
	&& rm -rf /var/lib/{apt,dpkg,cache,log}

ENTRYPOINT ["/entrypoint.sh"]
CMD ["influxd"]
