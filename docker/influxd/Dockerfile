FROM debian:stable-slim AS dependency-base

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update \
	&& apt-get install -y \
	ca-certificates \
	tzdata \
	&& apt-get clean autoclean \
	&& apt-get autoremove --yes \
	&& rm -rf /var/lib/{apt,dpkg,cache,log}

# NOTE: We separate these two stages so we can run the above
# quickly in CI, in case of flaky failure.
FROM dependency-base

EXPOSE 8086

COPY influxd /usr/bin/
COPY docker/influxd/entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["influxd"]
