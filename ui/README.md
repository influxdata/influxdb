## Packages

### Adding new packages

To add a new package, run

```sh
yarn add packageName
```

### Adding devDependency

```sh
yarn add packageName --dev
```

### Updating a package

First, run the command

```sh
yarn outdated
```

... to determine which packages may need upgrading.

We _really_ should not upgrade all packages at once, but, one at a time and make darn sure
to test.

To upgrade a single package named `packageName`:

```sh
yarn upgrade packageName
```

## Testing

Tests can be run via command line with `yarn test`, from within the `/ui` directory. For more detailed reporting, use `yarn test -- --reporters=verbose`.


## Cypress Testing

e2e tests:
For the end to end tests to run properly, the server needs to be running in the e2e testing mode with the in memory data store.
From the influxdb directory
`$ ./bin/darwin/influxd --assets-path=ui/build --e2e-testing --store=memory`

From the ui directory. Build the javascript with
`$ yarn start`
 To run Cypress locally
`$ yarn cy:dev`

## Starting Dev Server

The assets are built by running `yarn start` from withing the `/ui` directory. The dev server with hot reloading runs at `localhost:8080`.
