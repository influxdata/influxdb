# Propagation and handling observability context within IOx.

## Background

Observability context is how a component exposes metrics, traces, logs, panics, etc... in a way that places them in the context of the wider system. Most commonly this might be the namespace name, but might also be the table name, chunk ID, etc... Crucially this information may not be relevant to the component's primary function, e.g. if we never needed to observe `Db` it wouldn't need to know the namespace name at all, as only `Server` would need to know

Broadly speaking there are 3 approaches to how to inject this context:

1. Explicit Construction Time: The information is injected at construction time and kept around on the object. This is the approach used by our current observability plumbing
2. Implicit Construction Time: the information is injected into some wrapper type that carries it along. This was the approach of the old metrics registry construct
3. Explicit Invocation Time: The information is injected at every callsite to the object. This is the approach used by Snafu and tokio-tracing spans (which we aren't using).

Effectively the 3 trade-off between "polluting" the object or the callsites.

To give a concrete example, from a purely logic perspective the catalog does not need to know the namespace name, only the path to the object store. However, it is helpful for logs, metrics, etc... to be in terms of namespace name.

The three approaches would therefore be

1. Inject namespace name on construction onto the catalog object
2. Inject metrics, logs, etc... wrappers that carry the namespace name context internally
3. Inject namespace name at every call site, either explicitly passing it as an argument, or implicitly by wrapping in a span, mapping the returned error, etc...

## Outcome

The following key points were decided:

* It is acceptable for a struct to hold context information necessary for observability but not for its primary function
* This information should be a struct with explicit fields, as opposed to say a `HashMap<String,String>`
* Information that won't change for the lifetime of the object, should be provided at construction time


## See also
* Issue [#2795](https://github.com/influxdata/influxdb_iox/issues/2795#issue-1022839394)
