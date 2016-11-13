FROM gliderlabs/alpine
MAINTAINER Chris Goller <chris@influxdb.com>

RUN apk add --update ca-certificates && \
    rm /var/cache/apk/*

ADD chronograf /usr/bin/chronograf
ADD canned/*.json /usr/share/chronograf/canned/

CMD ["/usr/bin/chronograf", "-b", "/var/lib/chronograf/chronograf-v1.db", "-c", "/usr/share/chronograf/canned"]
