## Packages

### Adding new packages
To add a new package, run

```sh
npm i packageName
```

### Adding devDependency

```sh
npm i packageName -D
```

### Updating a package
First, run

```sh
npm outdated
```

... to determine which packages may need upgrading.

We _really_ should not upgrade all packages at once, but, one at a time and make darn sure
to test.

To upgrade a single package named `packageName`:

```sh
npm upgrade packageName
```

## Testing
Tests can be run via command line with `npm test`, from within the `/ui` directory. For more detailed reporting, use `yarn test -- --reporters=verbose`.
