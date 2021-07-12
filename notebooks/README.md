## Notebooks

This package provides an HTTP API for interacting with InfluxDB notebooks. The
HTTP handlers are located in the `transport` folder. The code for interacting
with the sqlite datastore is located in the `service.go` file. Definitions for
the basic types & interfaces associated with notebooks used throughout the
platform are located in the top-level `influxdb` package, in the `notebook.go`
file.

### Anatomy

The backend representation of a notebook is very simple: An object with an ID,
Name, associated organization ID, created/modified times, and a "spec". The
"spec" is a mechanism for storing a JSON string defined entirely by the frontend
UI. The notebook spec will likely be further defined in the future as the
notebooks feature is developed and more sophisticated backend behaviors are
required.

### Use

Basic CRUD actions are available for interacting with notebooks through the API.
Notebooks are persisted in the relational sqlite database, although they
currently do not make use of any relational features. Again, it is likely that
the more advanced features of the datastore will be utilized in the future as
the notebooks feature evolves.
