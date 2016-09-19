FROM node

RUN apt-get update -qq && apt-get install -y build-essential git-core

ENV GOPATH=/go
ENV APP_HOME=$GOPATH/src/github.com/influxdata/enterprise
ENV UI_HOME=$APP_HOME/ui

ARG GITHUB_TOKEN

RUN git config --global url."https://${GITHUB_TOKEN}:x-oauth-basic@github.com/".insteadOf "https://github.com/"

ADD . $APP_HOME
WORKDIR $UI_HOME
RUN npm install
