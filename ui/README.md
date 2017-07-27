## Packages
We are using [yarn](https://yarnpkg.com/en/docs/install) 0.19.1.

### Adding new packages
To add a new package, run

```sh
yarn add packageName
```

### Adding devDependency

```sh
yarn add --dev packageName
```

### Updating a package
First, run

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
Tests can be run via command line with `npm test`, from within the `/ui` directory. For more detailed reporting, use `npm test -- --reporters=verbose`.
