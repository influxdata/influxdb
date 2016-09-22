FROM gliderlabs/alpine
MAINTAINER Chris Goller <chris@influxdb.com>

RUN apk add --update ca-certificates && \
    rm /var/cache/apk/*

ADD mrfusion /mrfusion

EXPOSE 8888

# ENV PORT ||= 8888

CMD ["/mrfusion", "--port=$PORT"]
