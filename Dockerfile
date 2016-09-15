FROM mhart/alpine-node:latest

WORKDIR /ui

ADD ui /ui
RUN npm install

# Build assets to ./ui/build
RUN npm build

# Run the image as a non-root user
RUN adduser -D mrfusion
USER mrfusion

# Start the server.
# in the future we'd start the go process here and serve the static assets in /build
CMD npm start
