## Annotations

This package provides an HTTP API for interacting with both annotations and
streams independently. The HTTP handlers are located in the `transport` folder.
The code for interacting with the sqlite datastore is located in the
`service.go` file. Definitions for the basic types & interfaces associated with
annotations and streams used throughout the platform are located in the
top-level `influxdb` package, in the `annotation.go` file.

### Anatomy

An annotation is, at its simplest, a textual note on a range of time. The start
and stop time of that range can be the same point in time, which represents an
annotation at a single instance. Annotations can also have "stickers".
"Stickers" allow users to "tag" the annotation with further granularity for
filtering in key-value pairs. Some examples of sticker key-value pairs are:
`"product: oss"`, `"product: cloud"`, or `"service: tasks"`, but keys and values
can be any string.

Every annotation belongs to a single "stream". A "stream" represents a logical
grouping of annotated events. Some examples of stream names are: `"incidents"`,
`"deployments"`, or `"marketing"`, but can be any string. A stream can also have
a description to further clarify what annotated events may be expected in the
stream.

### Use

Requested annotations may be filtered by stream name, stickers, and/or time
range. Streams may also be retrieved, in order to view their description. If a
stream is deleted, all annotations associated with that stream are deleted as
well. Every annotation that is created must have a stream associated with it -
if a stream name is not provided when creating an annotation, it will be
assigned to the default stream.
