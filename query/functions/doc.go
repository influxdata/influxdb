/*
Package functions is a collection of built-in functions that are callable in the flux query processor.  While flux may
be extended at runtime by writing function expressions, there are some limitations for which a built-in function is
necessary, such as the need for custom data structures, stateful procedures, complex looping and branching, and
connection to external services.  Another reason for implementing a built-in function is to provide a function that is
broadly applicable for many users (e.g., sum() or max()).

The functions package is rarely accessed as a direct API.  Rather, the query processing engine accepts named registrations
for various interfaces implemented within the functions package and executes them generically using an API that is common to all
functions.  The registration process is executed by running the init() function in each function file, and is then finalized
by importing package builtin, which itself imports the functions package and runs a final setup routine that finalizes
installation of the builtin functions to the query processor.

Because of this design, a built-in function implementation consists of a bundle of different interface implementations
that are required at various phases of query execution.  These phases are query, plan and execute.  The query phase is
for identifying initializing the parameter sets for each function, and initializing the internal representation of a query.
The plan phase is for creating a final execution plan, and the execution phase is for physically accessing the data
and computing a final result.

The query phase takes each function call in a query and performs a match against the registry to see if
there are type definitions for a built-in operation.  If matched, it will instantiate the correct query.OperationSpec
type for that function, given the runtime parameters.
If a builtin OperationSpec is not found, then it will check for functions defined at runtime, and otherwise return an error.
The following registrations are typically executed in the function's init() for the query phase to execute properly:

	query.RegisterFunction(name string, c query.CreateOperationSpec, sig semantic.FunctionSignature)
	query.RegisterOpSpec(k query.OperationKind, c query.NewOperationSpec)

In the plan phase, an operation spec must be converted to a plan.ProcedureSpec.  A query plan must know what operations to
carry out, including the function names and parameters.    In the trivial case, the OperationSpec
and ProcedureSpec have identical fields and the operation spec may be encapsulated as part of the procedure spec. The base
interface for a plan.ProcedureSpec requires a Kind() function, as well as a Copy() function which should perform a deep copy
of the object.  Refer to the following interfaces for more information about designing a procedure spec:
	plan.ProcedureSpec
	plan.PushDownProcedureSpec
	plan.BoundedProcedureSpec
	plan.YieldProcedureSpec
	plan.AggregateProcedureSpec
	plan.ParentAwareProcedureSpec

Once you have determined the interface(s) that must be implemented for your function, you register them with
	plan.RegisterProcedureSpec(k ProcedureKind, c CreateProcedureSpec, qks ...query.OperationKind)

The registration in this phase creates two lookups.  First, it creates a named lookup in a similar fashion as for OperationSpecs
in the query phase.  Second, it creates a mapping from OperationSpec types to ProcedureSpec types so that the collection of
OperationSpecs for the query can be quickly converted to corresponding Procedure specs.  One feature to note is that the
registration takes a list of query.OperationSpec values. This is because several user-facing query functions may map
to the same internal procedure.

The primary function of the plan phase is to re-order, re-write and possibly combine the operations
described in the incoming query in order to improve the performance of the query execution.  The planner has two primary
operations for doing this: Pushdowns and ReWrites.

A push down operation is a planning technique for pushing the logic from one operation into another so that only a single
composite function needs to be called instead of two simpler function call.
A pushdown is implemented by implementing the plan.PushDownProcedureSpec interface, which requires functions that define
the rules and methods for executing a pushdown operation.

A Rewrite rule is used to modify one or more ProcedureSpecs in cases where redundant or complementary operations can be
combined to get a simpler result.  Similar to a pushdown operation, the rewrite is triggered whenever certain rules apply.
Rewrite rules are implemented differently and require a separate registration:
	plan.RegisterRewriteRule(r RewriteRule)

Which in turn requires an implementation of plan.RewriteRule.

Finally, the execute phase is tasked with executing the specific data processing algorithm for the function.  A function
implementation registers an implementation of the  execute.Transformation interface that implements functions that
control how the execution engine will take an input table, apply the function, and produce an output table.  A transformation
implementation is registered via:
	execute.RegisterTransformation(k plan.ProcedureKind, c execute.CreateTransformation)

The registration will record a mapping of the procedure's kind to the given transformation type.

In addition to implementing the transformation type, a number of helper types and functions are provided that facilitate
the transformation process:
	execute.Administration
	execute.Dataset
	execute.TableBuilderCache
	execute.TableBuilder
	execute.NewAggregateTransformationAndDataset
	execute.NewRowSelectorTransformationAndDataset
	query.Table
	query.GroupKey
	query.ColMeta
	query.ColReader

The most important part of a function implementation is for the interface method execute.Transformation.Process(id execute.DatasetID, tbl query.Table).
While the full details of how to implement this function are out of the scope of this document, a typical implementation
will do the following:
1.  Validate the incoming table schema if needed
2.  Construct the column and group key schema for the output table via the table builder.
3.  Process the incoming table via query.Table.Do, and use the input data to determine the output rows for the table builder.
4.  Add rows to the output table.

Finally, there is a special class of functions do not receive an input table from another function's output.
In other words, these transformations do not have a parent process that supplies it with table data.  These transformation
functions are referred to as sources, and naturally implement a connection to a data source (e.g. influxdb, prometheus, csvFile, etc.).
They are registered using:
	execute.RegisterSource(k plan.ProcedureKind, c execute.CreateSource)

The substantial part of a source implementation is its Run method, which should connect to the data source,
collect its data into query.Table structures, and apply any transformations associated with the source.
*/
package functions
