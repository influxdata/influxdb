FROM gliderlabs/alpine
MAINTAINER Chris Goller <chris@influxdb.com>

RUN apk add --update ca-certificates && \
    rm /var/cache/apk/*
    
ENV BOLT_PATH /var/lib/chronograf/chronograf.db
ENV CANNED_PATH /var/lib/chronograf/cannded/

ADD chronograf /usr/bin/chronograf
ADD canned/*.json /var/lib/chronograf/canned/

CMD ["/usr/bin/chronograf"]
