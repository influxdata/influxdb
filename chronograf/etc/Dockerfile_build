FROM ubuntu:trusty

RUN apt update && DEBIAN_FRONTEND=noninteractive apt install -y \
    apt-transport-https \
    python-dev \
    wget \
    curl \
    git \
    mercurial \
    make \
    ruby \
    ruby-dev \
    rpm \
    zip \
    python-pip \
    autoconf \
    libtool

RUN pip install boto requests python-jose --upgrade
RUN gem install fpm

# Install node
ENV NODE_VERSION v8.10.0
RUN wget -q https://nodejs.org/dist/${NODE_VERSION}/node-${NODE_VERSION}-linux-x64.tar.gz; \
   tar -xvf node-${NODE_VERSION}-linux-x64.tar.gz -C / --strip-components=1; \
   rm -f node-${NODE_VERSION}-linux-x64.tar.gz

# Install go
ENV GOPATH /root/go
ENV GO_VERSION 1.10
ENV GO_ARCH amd64
RUN wget https://storage.googleapis.com/golang/go${GO_VERSION}.linux-${GO_ARCH}.tar.gz; \
   tar -C /usr/local/ -xf /go${GO_VERSION}.linux-${GO_ARCH}.tar.gz ; \
   rm /go${GO_VERSION}.linux-${GO_ARCH}.tar.gz
ENV PATH /usr/local/go/bin:$PATH

ENV PROJECT_DIR $GOPATH/src/github.com/influxdata/influxdb/chronograf
ENV PATH $GOPATH/bin:$PATH
RUN mkdir -p $PROJECT_DIR
WORKDIR $PROJECT_DIR

VOLUME $PROJECT_DIR

ENTRYPOINT [ "/root/go/src/github.com/influxdata/influxdb/chronograf/etc/build.py" ]
