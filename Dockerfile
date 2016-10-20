FROM gliderlabs/alpine
MAINTAINER Chris Goller <chris@influxdb.com>

RUN apk add --update ca-certificates && \
    rm /var/cache/apk/*

ADD chronograf /chronograf
ADD canned/*.json /canned/

CMD ["/chronograf"]
