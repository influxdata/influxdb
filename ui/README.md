# Enterprise UI

Here lies a collection of React components used in the Enterprise web app.

Currently they're organized per "page", with each separate directly having it's own components/containers folders, index.js, etc.  For example:

/ui
  /overview
    /components
    /containers
    index.js
  /chronograf
    /components
    /containers
    index.js

## Getting started

This project uses Node v5.

It depends on at least one private Node module.
In order to successfully `npm install`, you'll need an [npm authentication token](http://blog.npmjs.org/post/118393368555/deploying-with-npm-private-modules).

There are two ways to get an npm token.

1. You can copy someone else's token (have them give you a copy of their `~/.npmrc`).
This is fine for most cases, and it's probably necessary for CI machines.
2. You can create an npm account and coordinate with Mark R to join the `influxdata` npm org.
You probably don't need this if you aren't ever going to create/publish a private Node module.

### Development

First, in the `ui/` folder run `npm install`.

run `npm run build:dev` to start the webpack process which bundles the JS into `assets/javascripts/generated/`.

### Tests

We use mocha, sinon, chai, and React's TestUtils library.

Run tests against jsdom from the command line:

```
npm run test       # single run
npm run test:watch # re-run tests on file changes
```

Run tests in the browser:

```
# This starts a webpack process that you'll need to leave running.
# It rebuilds your tests on each file change.
npm run test:browser
# open http://localhost:7357/spec/test.html
```

### Production

As before, `npm install` first.
Then `npm run build` to generate a production build into the `assets/javascripts/generated/` folder.

If you want to run tests against the production build, `npm run test` will build the test files and run the tests in a headless Phantom browser.
