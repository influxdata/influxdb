# Flux Builder

`flub` is a library for building Flux scripts.
It can be used for generating Flux AST or formatting to generate Flux source code.

## Usage

The library is used to construct an AST by constructing an `*ast.File`.
Using methods on the `*ast.File`, new pipelines can be created and methods can be chained to each other.
At any time during the chain, a node can be used by more than one node as a parent.
When multiple nodes refer to one node as a parent, the script generator will automatically generate a variable name.
Variable names can also be manually specified for more readable scripts.

As an example:

```go
f := flub.NewFile("main")
data := f.From("telegraf").Range(-time.Minute)
data.Yield("initial_data")
data.Call("filter").Yield("filtered_data")
fmt.Println(f.Format())
```

This will produce the following query:

```
package main


var0 = from(bucket: "telegraf")
	|> range(start: -1m)

var0
	|> yield(name: "initial_data")
var0
	|> filter()
	|> yield(name: "filtered_data")
```

The `As()` function can be used on any node in the pipeline to force the name of the variable or to force a variable to be created.
