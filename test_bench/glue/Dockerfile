FROM alpine:latest

ENV CURL_FLAGS="--proto =https --tlsv1.2 -sSf" \
    GRPCURL_VERSION="1.8.5"

RUN apk add --no-cache bash curl

RUN curl ${CURL_FLAGS} -Lo grpcurl.tar.gz https://github.com/fullstorydev/grpcurl/releases/download/v${GRPCURL_VERSION}/grpcurl_${GRPCURL_VERSION}_linux_x86_64.tar.gz \
    && tar xvf grpcurl.tar.gz \
    && mv grpcurl /bin

COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
