FROM mhart/alpine-node:latest

ADD package.json /package.json 
ADD webpack.config.js /webpack.config.js
ADD ui /ui
# Install node deps
RUN npm install

# Build assets to ./dist
RUN $(npm bin)/webpack

# Run the image as a non-root user
RUN adduser -D mrfusion
USER mrfusion

# Start the server. Currently this is just a webpack dev server
# in the future we'd start the go process here.
CMD $(npm bin)/webpack-dev-server --host 0.0.0.0 --port $PORT
