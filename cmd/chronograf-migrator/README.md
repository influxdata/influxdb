# Chronograf Migrator

This tool is used to migrate `1.x` Chronograf `Dashboards` and `Template Variables` to their `2.x`
equivalents using `pkger` packages. The tool expects the user to have the 1.x Chronograf database.

```sh
chronograf-migrator -h
Usage of chronograf-migrator:
  -db string
    	path to the chronograf database
  -output string
    	path to the output yaml file (default "dashboards.yml")
```

## Example Usage

```sh
$ chronograf-migrator -db chronograf-v1.db -output dashboards.yml
$ INFLUX_TOKEN=<token> influx pkg -o <org-name> -f dashboards.yml
```
