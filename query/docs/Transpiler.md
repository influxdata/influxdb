# Transpiler Design

Flux will support transpiling various other languages into query specification that can be executed.

Executing a transpiled query involes two steps outside the normal execution process.

1. Transpile the query to an query spec.
2. Write the result in the desired format.

The following interfaces found in the `crossexecute` package represent these concepts.

```
type QueryTranspiler interface {
	Transpile(ctx context.Context, txt string) (*query.Spec, error)
}

type ResultWriter interface {
	WriteTo(w io.Writer, results map[string]execute.Result) error
}
```

Each different language/system need only implement a query transpiler and result writer.

## Producing Flux txt via transpilation

The various transpilers only define the `somelang txt -> spec` transformation.
In general the reverse process will be possible, `spec -> Flux txt`.
Once any transpiler has been implemented then Flux txt can be produced from that source language.


## InfluxQL

Specific to writing the InfluxQL transpiler there is a major problem to overcome:


### How can the transpiler disambiguate fields and tags?

The transpiler will need a service that can report whether an identifier is a field or a tag for a given measurement.
The service will also need to be able to report which measurements exist for a given regexp pattern.
With this extra information the transpiler should be able to process any InfluxQL query.


#### Open Questions

 * What to do about measurements that have different schemas and are queried as if they have the same schema?

    ```
    select sum("value") from /(a|b)/ where "c" == 'foo'
    ```

    `c` is a tag on measurement `a`.
    `c` is a field on measurement `b`.

    For 1.x does this error or return some result?

 * Does a query spec contain enough information to convert 2.0 results into InfluxQL 1.x result JSON?
     The final table wil contain the correct column names etc to produce the correct JSON output.
     Flux Table -> InfluxQL JSON

