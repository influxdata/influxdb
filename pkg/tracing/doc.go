/*
Package tracing provides a way for capturing hierarchical traces.

To start a new trace with a root span named select

    trace, span := tracing.NewTrace("select")

It is recommended that a span be forwarded to callees using the
context package. Firstly, create a new context with the span associated
as follows

	ctx = tracing.NewContextWithSpan(ctx, span)

followed by calling the API with the new context

	SomeAPI(ctx, ...)

Once the trace is complete, it may be converted to a graph with the Tree method.

	tree := t.Tree()

The tree is intended to be used with the Walk function in order to generate
different presentations. The default Tree#String method returns a tree.

*/
package tracing
