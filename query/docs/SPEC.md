# Flux Specification

The following document specifies the Flux language and query execution.

This document is a living document and does not represent the current implementation of Flux.
Any section that is not currently implemented is commented with a IMPL#XXX where XXX is an issue number tracking discussion and progress towards implementation.

## Language

The Flux language is centered on querying and manipulating time series data.

### Notation

The syntax of the language is specified using Extended Backus-Naur Form (EBNF):

    Production  = production_name "=" [ Expression ] "." .
    Expression  = Alternative { "|" Alternative } .
    Alternative = Term { Term } .
    Term        = production_name | token [ "…" token ] | Group | Option | Repetition .
    Group       = "(" Expression ")" .
    Option      = "[" Expression "]" .
    Repetition  = "{" Expression "}" .

Productions are expressions constructed from terms and the following operators, in increasing precedence:

    |   alternation
    ()  grouping
    []  option (0 or 1 times)
    {}  repetition (0 to n times)

Lower-case production names are used to identify lexical tokens.
Non-terminals are in CamelCase.
Lexical tokens are enclosed in double quotes "" or back quotes \`\`.

### Representation

Source code is encoded in UTF-8.
The text need not be canonicalized.

#### Characters

This document will use the term _character_ to refer to a Unicode code point.

The following terms are used to denote specific Unicode character classes:

    newline        = /* the Unicode code point U+000A */ .
    unicode_char   = /* an arbitrary Unicode code point except newline */ .
    unicode_letter = /* a Unicode code point classified as "Letter" */ .
    unicode_digit  = /* a Unicode code point classified as "Number, decimal digit" */ .

In The Unicode Standard 8.0, Section 4.5 "General Category" defines a set of character categories.
Flux treats all characters in any of the Letter categories Lu, Ll, Lt, Lm, or Lo as Unicode letters, and those in the Number category Nd as Unicode digits.

#### Letters and digits

The underscore character _ (U+005F) is considered a letter.

    letter        = unicode_letter | "_" .
    decimal_digit = "0" … "9" .

### Lexical Elements

#### Comments

Comment serve as documentation.
Comments begin with the character sequence `//` and stop at the end of the line.

Comments cannot start inside string or regexp literals.
Comments act like newlines.

#### Tokens

Flux is built up from tokens.
There are several classes of tokens: _identifiers_, _keywords_, _operators_, and _literals_.
_White space_, formed from spaces, horizontal tabs, carriage returns, and newlines, is ignored except as it separates tokens that would otherwise combine into a single token.
While breaking the input into tokens, the next token is the longest sequence of characters that form a valid token.

#### Identifiers

Identifiers name entities within a program.
An identifier is a sequence of one or more letters and digits.
An identifier must start with a letter.

    identifier = letter { letter | unicode_digit } .

Examples:

    a
    _x
    longIdentifierName
    αβ

#### Keywords

The following keywords are reserved and may not be used as identifiers:

    and    import  not  return
    empty  in      or

[IMPL#308](https://github.com/influxdata/platform/query/issues/308) Add in and empty operator support
[IMPL#142](https://github.com/influxdata/platform/query/issues/142) Add "import" support

#### Operators

The following character sequences represent operators:

    +   ==   !=   (   )
    -   <    !~   [   ]
    *   >    =~   {   }
    /   <=   =    ,   :
    %   >=   <-   .   |>

#### Numeric literals

Numeric literals may be integers or floating point values.
Literals have arbitrary precision and will be coerced to a specific type when used.

The following coercion rules apply to numeric literals:

* an integer literal can be coerced to an "int", "uint", or "float" type,
* an float literal can be coerced to a "float" type,
* an error will occur if the coerced type cannot represent the literal value.


[IMPL#309](https://github.com/influxdata/platform/query/issues/309) Allow numeric literal coercion.

##### Integer literals

An integer literal is a sequence of digits representing an integer value.
Only decimal integers are supported.

    int_lit     = "0" | decimal_lit .
    decimal_lit = ( "1" … "9" ) { decimal_digit } .

Examples:

    0
    42
    317316873

##### Floating-point literals

A floating-point literal is a decimal representation of a floating-point value.
It has an integer part, a decimal point, and a fractional part.
The integer and fractional part comprise decimal digits.
One of the integer part or the fractional part may be elided.

    float_lit = decimals "." [ decimals ] |
        "." decimals .
    decimals  = decimal_digit { decimal_digit } .

Examples:

    0.
    72.40
    072.40  // == 72.40
    2.71828
    .26

[IMPL#310](https://github.com/influxdata/platform/query/issues/310) Parse float literals

#### Duration literals

A duration literal is a representation of a length of time.
It has an integer part and a duration unit part.
Multiple duration may be specified together and the resulting duration is the sum of each smaller part.

    duration_lit        = { int_lit duration_unit } .
    duration_unit       = "ns" | "u" | "µ" | "ms" | "s" | "m" | "h" | "d" | "w" .

| Units    | Meaning                                 |
| -----    | -------                                 |
| ns       | nanoseconds (1 billionth of a second)   |
| us or µs | microseconds (1 millionth of a second)  |
| ms       | milliseconds (1 thousandth of a second) |
| s        | second                                  |
| m        | minute (60 seconds)                     |
| h        | hour (60 minutes)                       |
| d        | day (24 hours)                          |
| w        | week (7 days)                           |

Durations represent a fixed length of time.
They do not change based on time zones or other time related events like daylight savings or leap seconds.

Examples:

    1s
    10d
    1h15m // 1 hour and 15 minutes
    5w

[IMPL#311](https://github.com/influxdata/platform/query/issues/311) Parse duration literals

#### Date and time literals

A date and time literal represents a specific moment in time.
It has a date part, a time part and a time offset part.
The format follows the RFC 3339 specification.

    date_time_lit     = date "T" time .
    date              = year_lit "-" month "-" day .
    year              = decimal_digit decimal_digit decimal_digit decimal_digit .
    month             = decimal_digit decimal_digit .
    day               = decimal_digit decimal_digit .
    time              = hour ":" minute ":" second [ fractional_second ] time_offset .
    hour              = decimal_digit decimal_digit .
    minute            = decimal_digit decimal_digit .
    second            = decimal_digit decimal_digit .
    fractional_second = "."  { decimal_digit } .
    time_offset       = "Z" | ("+" | "-" ) hour ":" minute .


#### String literals

A string literal represents a sequence of characters enclosed in double quotes.
Within the quotes any character may appear except an unescaped double quote.
String literals support several escape sequences.

    \n   U+000A line feed or newline
    \r   U+000D carriage return
    \t   U+0009 horizontal tab
    \"   U+0022 double quote
    \\   U+005C backslash
    \{   U+007B open curly bracket
    \}   U+007D close curly bracket

Additionally any byte value may be specified via a hex encoding using `\x` as the prefix.


    string_lit       = `"` { unicode_value | byte_value | StringExpression | newline } `"` .
    byte_value       = `\` "x" hex_digit hex_digit .
    hex_digit        = "0" … "9" | "A" … "F" | "a" … "f" .
    unicode_value    = unicode_char | escaped_char .
    escaped_char     = `\` ( "n" | "r" | "t" | `\` | `"` ) .
    StringExpression = "{" Expression "}" .

TODO(nathanielc): With string interpolation string_lit is not longer a lexical token as part of a literal, but an entire expression in and of itself.


[IMPL#312](https://github.com/influxdata/platform/query/issues/312) Parse string literals


Examples:

    "abc"
    "string with double \" quote"
    "string with backslash \\"
    "日本語"
    "\xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e" // the explicit UTF-8 encoding of the previous line

String literals are also interpolated for embedded expressions to be evaluated as strings.
Embedded expressions are enclosed in curly brackets "{}".
The expressions are evaluated in the scope containing the string literal.
The result of an expression is formatted as a string and replaces the string content between the brackets.
All types are formatted as strings according to their literal representation.
A function "printf" exists to allow more precise control over formatting of various types.
To include the literal curly brackets within a string they must be escaped.


[IMPL#316](https://github.com/influxdata/platform/query/issues/316) Add printf function

Interpolation example:

    n = 42
    "the answer is {n}" // the answer is 42
    "the answer is not {n+1}" // the answer is not 43
    "openinng curly bracket \{" // openinng curly bracket {
    "closing curly bracket \}" // closing curly bracket }

[IMPL#313](https://github.com/influxdata/platform/query/issues/313) Add string interpolation support


#### Regular expression literals

A regular expression literal represents a regular expression pattern, enclosed in forward slashes.
Within the forward slashes, any unicode character may appear except for an unescaped forward slash.
The `\x` hex byte value representation from string literals may also be present.

Regular expression literals support only the following escape sequences:

    \/   U+002f forward slash
    \\   U+005c backslash


    regexp_lit         = "/" { unicode_char | byte_value | regexp_escape_char } "/" .
    regexp_escape_char = `\` (`/` | `\`)

Examples:

    /.*/
    /http:\/\/localhost:9999/
    /^\xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e(ZZ)?$/
    /^日本語(ZZ)?$/ // the above two lines are equivalent
    /\\xZZ/ // this becomes the literal pattern "\xZZ"

The regular expression syntax is defined by [RE2](https://github.com/google/re2/wiki/Syntax).


[IMPL#314](https://github.com/influxdata/platform/query/issues/314) Parse regular expression literals

### Variables

A variable holds a value.
A variable can only hold values defined by its type.

### Types

A type defines the set of values and operations on those values.
Types are never explicitly declared as part of the syntax.
Types are always inferred from the usage of the value.

[IMPL#315](https://github.com/influxdata/platform/query/issues/315) Specify type inference rules

#### Boolean types

A _boolean type_ represents a truth value, corresponding to the preassigned variables `true` and `false`.
The boolean type name is `bool`.

#### Numeric types

A _numeric type_ represents sets of integer or floating-point values.

The following numeric types exist:

    uint    the set of all unsigned 64-bit integers
    int     the set of all signed 64-bit integers
    float   the set of all IEEE-754 64-bit floating-point numbers

#### Time types

A _time type_ represents a single point in time with nanosecond precision.
The time type name is `time`.


#### Duration types

A _duration type_ represents a length of time with nanosecond precision.
The duration type name is `duration`.

#### String types

A _string type_ represents a possibly empty sequence of characters.
Strings are immutable: once created they cannot be modified.
The string type name is `string`.

The length of a string is its size in bytes, not the number of characters, since a single character may be multiple bytes.

#### Regular expression types

A _regular expression type_ represents the set of all patterns for regular expressions.
The regular expression type name is `regexp`.

#### Array types

An _array type_ represents a sequence of values of any other type.
All values in the array must be of the same type.
The length of an array is the number of elements in the array.

#### Object types

An _object type_ represents a set of unordered key and value pairs.
The key must always be a string.
The value may be any other type, and need not be the same as other values within the object.

#### Function types

A _function type_ represents a set of all functions with the same argument and result types.


[IMPL#315](https://github.com/influxdata/platform/query/issues/315) Specify type inference rules

### Blocks

A _block_ is a possibly empty sequence of statements within matching brace brackets.

    Block = "{" StatementList "} .
    StatementList = { Statement } .

In addition to explicit blocks in the source code, there are implicit blocks:

1. The _options block_ is the top-level block for all Flux programs. All option declarations are contained in this block.
2. The _universe block_ encompasses all Flux source text aside from option statements. It is nested directly inside of the _options block_.
3. Each package has a _package block_ containing all Flux source text for that package.
4. Each file has a _file block_ containing all Flux source text in that file.
5. Each function literal has its own _function block_ even if not explicitly declared.

Blocks nest and influence scoping.

### Assignment and scope

An assignment binds an identifier to a variable or function.
Every identifier in a program must be assigned.
An identifier may not change type via assignment within the same block.
An identifier may change value via assignment within the same block.

Flux is lexically scoped using blocks:

1. The scope of an option identifier is the options block.
2. The scope of a preassigned (non-option) identifier is in the universe block.
3. The scope of an identifier denoting a variable or function at the top level (outside any function) is the package block.
4. The scope of a package name of an imported package is the file block of the file containing the import declaration.
5. The scope of an identifier denoting a function argument is the function body.
6. The scope of a variable assigned inside a function is the innermost containing block.

An identifier assigned in a block may be reassigned in an inner block with the exception of option identifiers.
While the identifier of the inner assignment is in scope, it denotes the entity assigned by the inner assignment.

Option identifiers have default assignments that are automatically defined in the _options block_.
Because the _options block_ is the top-level block of a Flux program, options are visible/available to any and all other blocks.
However option values may only be reassigned or overridden in the explicit block denoting the main (executable) package.
Assignment of option identifiers in any non-executable package is strictly prohibited.

The package clause is not a assignment; the package name does not appear in any scope.
Its purpose is to identify the files belonging to the same package and to specify the default package name for import declarations.


[IMPL#317](https://github.com/influxdata/platform/query/issues/317) Add package/namespace support

#### Variable assignment

A variable assignment creates a variable bound to the identifier and gives it a type and value.
When the identifier was previously assigned within the same block the identifier now holds the new value.
An identifier cannot change type within the same block.

    VarAssignment = identifier "=" Expression

Examples:

    n = 1
    n = 2
    f = 5.4
    r = z()


### Expressions

An expression specifies the computation of a value by applying the operators and functions to operands.

#### Operands

Operands denote the elementary values in an expression.
An operand may be a literal, identifier denoting a variable, or a parenthesized expression.


TODO(nathanielc): Fill out expression details...
PEG parsers don't understand operators precedence so it difficult to express operators in expressions with the grammar.
We should simplify it and use the EBNF grammar.
This requires redoing the parser in something besides PEG.


[IMPL#318](https://github.com/influxdata/platform/query/issues/318) Update parser to use formal EBNF grammar.

#### Function literals

A function literal defines a new function with a body and parameters.
The function body may be a block or a single expression.
The function body must have a return statement if it is an explicit block, otherwise the expression is the return value.

    FunctionLit        = FunctionParameters "=>" FunctionBody .
    FunctionParameters = "(" [ ParameterList [ "," ] ] ")" .
    ParameterList      = ParameterDecl { "," ParameterDecl } .
    ParameterDecl      = identifier [ "=" Expression ] .
    FunctionBody       = Expression | Block .

Examples:

    () => 1 // function returns the value 1
    (a, b) => a + b // function returns the sum of a and b
    (x=1, y=1) => x * y // function with default values
    (a, b, c) => { // function with a block body
        d = a + b
        return d / c
    }

All function literals are anonymous.
A function may be given a name using a variable assignment.

    add = (a,b) => a + b
    mul = (a,b) => a * b

Function literals are _closures_: they may refer to variables defined is a surrounding block.
Those variables are shared between the function literal and the surrounding block.

#### Call expressions

A call expressions invokes a function with the provided arguments.
Arguments must be specified using the argument name, positional arguments not supported.
Argument order does not matter.
When an argument has a default value, it is not required to be specified.

Examples:

    f(a:1, b:9.6)
    float(v:1)

#### Pipe expressions

A pipe expression is a call expression with an implicit piped argument.
Pipe expressions simplify creating long nested call chains.

Pipe expressions pass the result of the left hand expression as the _pipe argument_ to the right hand call expression.
Function literals specify which if any argument is the pipe argument using the _pipe literal_ as the argument's default value.
It is an error to use a pipe expression if the function does not declare a pipe argument.

    pipe_lit = "<-" .

Examples:

    foo = () => // function body elided
    bar = (x=<-) => // function body elided
    baz = (y=<-) => // function body elided
    foo() |> bar() |> baz() // equivalent to baz(x:bar(y:foo()))

### Statements

A statement controls execution.

    Statement = OptionStatement | VarAssignment | ReturnStatement |
                ExpressionStatement | BlockStatment .

#### Option statements

Options specify a context in which a Flux query is to be run. They define variables
that describe how to execute a Flux query. For example, the following Flux script sets
the `task` option to schedule a query to run periodically every hour:

    option task = {
        name: "mean",
        every: 1h,
    }

    from(db:"metrics")
        |> range(start:-task.every)
        |> group(by:["level"])
        |> mean()
        |> yield(name:"mean")

All options are designed to be completely optional and have default values to be used when not specified.
Grammatically, an option statement is just a variable assignment preceded by the "option" keyword.

    OptionStatement = "option" VarAssignment

Below is a list of all options that are currently implemented in the Flux language:

* task
* now

##### task

The `task` option is used by a scheduler to schedule the execution of a Flux query.

    option task = {
        name: "foo",        // name is required
        every: 1h,          // task should be run at this interval
        delay: 10m,         // delay scheduling this task by this duration
        cron: "0 2 * * *",  // cron is a more sophisticated way to schedule. every and cron are mutually exclusive
        retry: 5,           // number of times to retry a failed query
    }

##### now

The `now` option is a function that returns a time value to be used as a proxy for the current system time.

    // Query should execute as if the below time is the current system time
    option now = () => 2006-01-02T15:04:05Z07:00

#### Return statements

A terminating statement prevents execution of all statements that appear after it in the same block.
A return statement is a terminating statement.

    ReturnStatement = "return" Expression .

#### Expression statements

An expression statement is an expression where the computed value is discarded.

    ExpressionStatement = Expression .

Examples:

    1 + 1
    f()
    a

### Built-in functions

The following functions are preassigned in the universe block.

#### Functions and Operations

Many function's purpose is to construct an operation that will be part of a query specification.
As a result these functions return an object that represents the specific operation being defined.
The object is then passed into the next function and added as a parent, constructing a directed acyclic graph of operations.
The yield function then traverses the graph of operations and produces a query specification that can be executed.

The following is a list of functions whose main purpose is to construct an operation.
Details about their arguments and behavior can be found in the Operations section of this document.

* count
* covariance
* cumulativeSum
* derivative
* difference
* distinct
* filter
* first
* from
* group
* integral
* join
* last
* limit
* map
* max
* mean
* min
* percentile
* range
* sample
* set
* shift
* skew
* sort
* spread
* stateTracking
* stddev
* sum
* window
* yield

Other functions make use of existing operations to create composite operations.

##### Cov

Cov computes the covariance between two streams by first joining the streams and then performing the covariance operation.

##### HighestMax

HighestMax computes the top N records from all tables using the maximum of each table.

##### HighestAverage

HighestAverage computes the top N records from all tables using the average of each table.

##### HighestCurrent

HighestCurrent computes the top N records from all tables using the last value of each table.

##### LowestMin

LowestMin computes the bottom N records from all tables using the minimum of each table.

##### LowestAverage

LowestAverage computes the bottom N records from all tables using the average of each table.

##### LowestCurrent

LowestCurrent computes the bottom N records from all tables using the last value of each table.

##### Pearsonr

Pearsonr computes the Pearson R correlation coefficient between two streams by first joining the streams and then performing the covariance operation normalized to compute R.

##### StateCount

StateCount computes the number of consecutive records in a given state.

##### StateDuration

StateDuration computes the duration of a given state.


##### Top/Bottom

Top and Bottom sort a table and limits the table to only n records.

## Query engine

The execution of a query is separate and distinct from the execution of Flux the language.
The input into the query engine is a query specification.

The output of an Flux program is a query specification, which then may be passed into the query execution engine.

### Query specification

A query specification consists of a set of operations and a set of edges between those operations.
The operations and edges must form a directed acyclic graph (DAG).

#### Encoding

The query specification may be encoded in different formats.
An encoding must consist of three properties:

* operations -  a list of operations and their specification.
* edges - a list of edges declaring a parent child relation between operations.
* resources - an optional set of contraints on the resources the query can consume.

Each operation has three properties:

* kind - kind is the name of the operation to perform.
* id - an identifier for this operation, it must be unique per query specification.
* spec - a set of properties that specify details of the operation.
    These vary by the kind of operation.

JSON encoding is supported and the following is an example encoding of a query:

```
from(db:"mydatabase") |> last()
```

```
{
  "operations": [
    {
      "kind": "from",
      "id": "from0",
      "spec": {
        "db": "mydatabase"
      }
    },
    {
      "kind": "last",
      "id": "last1",
      "spec": {
        "column": ""
      }
    }
  ],
  "edges": [
    {
      "parent": "from0",
      "child": "last1"
    }
  ],
  "resources": {
    "priority": "high",
    "concurrency_quota": 0,
    "memory_bytes_quota": 0
  }
}
```

### Data model

The data model for the query engine consists of tables, records, columns and streams.

#### Record

A record is a tuple of values.

#### Column

A column has a label and a data type.

The available data types for a column are:

    bool     a boolean value, true or false.
    uint     an unsigned 64-bit integer
    int      a signed 64-bit integer
    float    an IEEE-754 64-bit floating-point number
    string   a sequence of unicode characters
    bytes    a sequence of byte values
    time     a nanosecond precision instant in time
    duration a nanosecond precision duration of time


#### Table

A table is set of records, with a common set of columns and a group key.

The group key is a list of columns.
A table's group key denotes which subset of the entire dataset is assigned to the table.
As such, all records within a table will have the same values for each column that is part of the group key.
These common values are referred to as the group key value, and can be represented as a set of key value pairs.

A tables schema consists of its group key, and its column's labels and types.


[IMPL#294](https://github.com/influxdata/platform/query/issues/294) Remove concept of Kind from table columns
[IMPL#319](https://github.com/influxdata/platform/query/issues/319) Remove concept of Bounds from tables
[IMPL#320](https://github.com/influxdata/platform/query/issues/320) Rename block to table

#### Stream

A stream represents a potentially unbounded dataset.
A stream grouped into individual tables.
Within a stream each table's group key value is unique.

#### Missing values

A record may be missing a value for a specific column.
Missing values are represented with a special _null_ value.
The _null_ value can be of any data type.


[IMPL#219](https://github.com/influxdata/platform/query/issues/219) Design how nulls behave

#### Operations

An operation defines a transformation on a stream.
All operations may consume a stream and always produce a new stream.

Most operations output one table for every table they receive from the input stream.

Operations that modify the group keys or values will need to regroup the tables in the output stream.

### Built-in operations

#### From

From produces a stream of tables from the specified bucket.
Each unique series is contained within its own table.
Each record in the table represents a single point in the series.

The tables schema will include the following columns:

* `_time`
    the time of the record
* `_value`
    the value of the record
* `_start`
    the inclusive lower time bound of all records
* `_stop`
    the exclusive upper time bound of all records

Additionally any tags on the series will be added as columns.

Example:

    from(bucket:"telegraf")

From has the following properties:

* `bucket` string
    The name of the bucket to query.
* `db` string
    The name of the database to query.

#### Yield

Yield indicates that the stream received by the yield operation should be delivered as a result of the query.
A query may have multiple results, each identified by the name provided to yield.

Yield outputs the input stream unmodified.

Yield has the following properties:

* `name` string
    unique name to give to yielded results

#### Aggregate operations

Aggregate operations output a table for every input table they receive.
A list of columns to aggregate must be provided to the operation.
The aggregate function is applied to each column in isolation.

Any output table will have the following properties:

* It always contains a single record.
* It will have the same group key as the input table.
* It will have a column `_time` which represents the time of the aggregated record.
    This can be set as the start or stop time of the input table.
    By default the stop time is used.
* It will contain a column for each provided aggregate column.
    The column label will be the same as the input table.
    The type of the column depends on the specific aggregate operation.

All aggregate operations have the following properties:

* `columns` list of string
    columns specifies a list of columns to aggregate.
* `timeSrc` string
    timeSrc is the source time column to use on the resulting aggregate record.
    The value must be column with type `time` and must be part of the group key.
    Defaults to `_stop`.
* `timeDst` string
    timeDst is the destination column to use for the resulting aggregate record.
    Defaults to `_time`.

[IMPL#294](https://github.com/influxdata/platform/query/issues/294) Remove concept of Kind from table columns

##### Covariance

Covariance is an aggregate operation.
Covariance computes the covariance between two columns.

Covariance has the following properties:

* `pearsonr` bool
    pearsonr indicates whether the result should be normalized to be the Pearson R coefficient.
* `valueDst` string
    valueDst is the column into which the result will be placed.
    Defaults to `_value`.

Additionally exactly two columns must be provided to the `columns` property.

##### Count

Count is an aggregate operation.
For each aggregated column, it outputs the number of non null records as an integer.

##### Integral

Integral is an aggregate operation.
For each aggregate column, it outputs the area under the curve of non null records.
The curve is defined as function where the domain is the record times and the range is the record values.

Integral has the following properties:

* `unit` duration
    unit is the time duration to use when computing the integral

##### Mean

Mean is an aggregate operation.
For each aggregated column, it outputs the mean of the non null records as a float.

##### Percentile

Percentile is an aggregate operation.
For each aggregated column, it outputs the value that represents the specified percentile of the non null record as a float.

Percentile has the following properties:

* `percentile` float
    A value between 0 and 1 indicating the desired percentile.
* `exact` bool
    If true an exact answer is computed, otherwise an approximate answer is computed.
    Using exact requires that the entire dataset fit in available memory.
    Defaults to false.
* `compression` float
   Compression indicates how many centroids to use when compressing the dataset.
   A larger number produces a more accurate result at the cost of increased memory requirements.
   Defaults to 1000.

##### Skew

Skew is an aggregate operation.
For each aggregated column, it outputs the skew of the non null record as a float.

##### Spread

Spread is an aggregate operation.
For each aggregated column, it outputs the difference between the min and max values.
The type of the output column depends on the type of input column: for input columns with type `uint` or `int`, the output is an `int`; for `float` input columns the output is a `float`.
All other input types are invalid.

##### Stddev

Stddev is an aggregate operation.
For each aggregated column, it outputs the standard deviation of the non null record as a float.

##### Sum

Stddev is an aggregate operation.
For each aggregated column, it outputs the sum of the non null record.
The output column type is the same as the input column type.

#### Multiple aggregates

TODO(nathanielc): Need a way to apply multiple aggregates to same table


#### Selector operations

Selector operations output a table for every input table they receive.
A single column on which to operate must be provided to the operation.

Any output table will have the following properties:

* It will have the same group key as the input table.
* It will contain the same columns as the input table.
* It will have a column `_time` which represents the time of the selected record.
    This can be set as the value of any time column on the input table.
    By default the `_stop` time column is used.

All selector operations have the following properties:

* `column` string
    column specifies a which column to use when selecting.

[IMPL#294](https://github.com/influxdata/platform/query/issues/294) Remove concept of Kind from table columns

##### First

First is a selector operation.
First selects the first non null record from the input table.

##### Last

Last is a selector operation.
Last selects the last non null record from the input table.

##### Max

Max is a selector operation.
Max selects the maximum record from the input table.

##### Min

Min is a selector operation.
Min selects the minimum record from the input table.

##### Sample

Sample is a selector operation.
Sample selects a subset of the records from the input table.

The following properties define how the sample is selected.

* `n`
    Sample every Nth element
* `pos`
    Position offset from start of results to begin sampling.
    The `pos` must be less than `n`.
    If `pos` is less than 0, a random offset is used.
    Default is -1 (random offset).


#### Filter

Filter applies a predicate function to each input record, output tables contain only records which matched the predicate.
One output table is produced for each input table.
The output tables will have the same schema as their corresponding input tables.

Filter has the following properties:

* `fn` function(record) bool
    Predicate function.
    The function must accept a single record parameter and return a boolean value.
    Each record will be passed to the function.
    Records which evaluate to true, will be included in the output tables.
    TODO(nathanielc): Do we need a syntax for expressing type signatures?

#### Limit

Limit caps the number of records in output tables to a fixed size n.
One output table is produced for each input table.
The output table will contain the first n records from the input table.
If the input table has less than n records all records will be output.

Limit has the following properties:

* `n` int
    The maximum number of records to output.

#### Map

Map applies a function to each record of the input tables.
The modified records are assigned to new tables based on the group key of the input table.
The output tables are the result of applying the map function to each record on the input tables.

When the output record contains a different value for the group key the record is regroup into the appropriate table.
When the output record drops a column that was part of the group key that column is removed from the group key.

Map has the following properties:

* `fn` function
    Function to apply to each record.
    The return value must be an object.
* `mergeKey` bool
    MergeKey indicates if the record returned from fn should be merged with the group key.
    When merging, all columns on the group key will be added to the record giving precedence to any columns already present on the record.
    When not merging, only columns defined on the returned record will be present on the output records.
    Defaults to true.

#### Range

Range filters records based on provided time bounds.
Each input tables records are filtered to contain only records that exist within the time bounds.
Each input table's group key value is modified to fit within the time bounds.
Tables where all records exists outside the time bounds are filtered entirely.


[IMPL#321](https://github.com/influxdata/platform/query/issues/321) Update range to default to aligned window ranges.

Range has the following properties:

* `start` duration or timestamp
    Specifies the oldest time to be included in the results
* `stop` duration or timestamp
    Specifies the exclusive newest time to be included in the results.
    Defaults to "now"

#### Rename 

Rename will rename specified columns in a data table. 
There are two variants: one which takes a map of old column names to new column names,
 and one which takes a mapping function.

Rename has the following properties: 
* `columns` object
	A map of columns to rename and their corresponding new names. Cannot be used with `fn`. 
* `fn` function 
    A function which takes a single string parameter (the old column name) and returns a string representing 
    the new column name. Cannot be used with `columns`.

Example usage:

Rename a single column: `rename(columns:{host: "server"})`

Rename all columns with `fn` parameter: `rename(fn: (col) => "{col}_new")`

#### Drop 

Drop will exclude specified columns from a returned data table. Columns to exclude can be specified either through a 
list, or a predicate function. 

Drop has the following properties:
* `columns` array of strings 
    An array of columns which should be excluded from the resulting table. Cannot be used with `fn`.
* `fn` function 
    A function which takes a column name as a parameter and returns a boolean indicating whether
    or not the column should be excluded from the resulting table. Cannot be used with `columns`.  

Example Usage:

Drop a list of columns: `drop(columns: ["host", "_measurement"])`

Drop all columns matching a predicate: `drop(fn: (col) => col =~ /usage*/)`

#### Keep 

Keep can be thought of as the inverse of drop. It will return a table containing only columns that are specified,
ignoring all others.

Keep has the following properties: 
* `columns` array of strings
    An array of columns that should be included in the resulting table. Cannot be used with `fn`.
* `fn` function
    A function which takes a column name as a parameter and returns a boolean indicating whether or not
    the column should be included in the resulting table. Cannot be used with `columns`. 

Example Usage:

Keep a list of columns: `keep(columns: ["_time", "_value"])`

Keep all columns matching a predicate: `keep(fn: (col) => col =~ /inodes*/)`


#### Set

Set assigns a static value to each record.
The key may modify and existing column or it may add a new column to the tables.
If the column that is modified is part of the group key, then the output tables will be regroup as needed.


Set has the following properties:

* `key` string
    key is the label for the column to set
* `value` string
    value is the string value to set


#### Sort

Sorts orders the records within each table.
One output table is produced for each input table.
The output tables will have the same schema as their corresponding input tables.

Sort has the following properties:

* `columns` list of strings
    List of columns used to sort; precedence from left to right.
    Default is `["_value"]`
* `desc` bool
    Sort results in descending order.


#### Group

Group groups records based on their values for specific columns.
It produces tables with new group keys based on the provided properties.

Group has the following properties:

*  `by` list of strings
    Group by these specific columns.
    Cannot be used with `except`.
*  `except` list of strings
    Group by all other column except this list of columns.
    Cannot be used with `by`.

Examples:

    group(by:["host"]) // group records by their "host" value
    group(except:["_time", "region", "_value"]) // group records by all other columns except for _time, region, and _value
    group(by:[]) // group all records into a single group
    group(except:[]) // group records into all unique groups

[IMPL#322](https://github.com/influxdata/platform/query/issues/322) Investigate always keeping all columns in group.

#### Window

Window groups records based on a time value.
New columns are added to uniquely identify each window and those columns are added to the group key of the output tables.

A single input record will be placed into zero or more output tables, depending on the specific windowing function.

Window has the following properties:

* `every` duration
    Duration of time between windows
    Defaults to `period`'s value
* `period` duration
    Duration of the windowed group
    Default to `every`'s value
* `start` time
    The time of the initial window group
* `round` duration
    Rounds a window's bounds to the nearest duration
    Defaults to `every`'s value
* `column` string
    Name of the time column to use. Defaults to `_time`.
* `startCol` string
    Name of the column containing the window start time. Defaults to `_start`.
* `stopCol` string
    Name of the column containing the window stop time. Defaults to `_stop`.

[IMPL#319](https://github.com/influxdata/platform/query/issues/319) Remove concept of Bounds from tables

#### Collate

[IMPL#323](https://github.com/influxdata/platform/query/issues/323) Add function that makes it easy to get all fields as columns given a set of tags.

#### Join

Join merges two or more input streams into a single output stream.
Input tables are matched on their group keys and then each of their records are joined into a single output table.
The output table group key will be the same as the input table.

The join operation compares values based on equality.

Join has the following properties:

* `tables` map of tables
    Map of tables to join. Currently only two tables are allowed.
* `on` array of strings
    List of columns on which to join the tables.
* `fn`
    Defines the function that merges the values of the tables.
    The function must defined to accept a single parameter.
    The parameter is an object where the value of each key is a corresponding record from the input streams.
    The return value must be an object which defines the output record structure.



#### Cumulative sum

Cumulative sum computes a running sum for non null records in the table.
The output table schema will be the same as the input table.

Cumulative sum has the following properties:

* `columns` list string
    columns is a list of columns on which to operate.

#### Derivative

Derivative computes the time based difference between subsequent non null records.

Derivative has the following properties:

* `unit` duration
    unit is the time duration to use for the result
* `nonNegative` bool
    nonNegative indicates if the derivative is allowed to be negative.
    If a value is encountered which is less than the previous value then it is assumed the previous value should have been a zero.
* `columns` list strings
    columns is a list of columns on which to compute the derivative
* `timeSrc` string
    timeSrc is the source column for the time values.
    Defaults to `_time`.

#### Difference

Difference computes the difference between subsequent non null records.

Difference has the following properties:

* `nonNegative` bool
    nonNegative indicates if the derivative is allowed to be negative.
    If a value is encountered which is less than the previous value then it is assumed the previous value should have been a zero.
* `columns` list strings
    columns is a list of columns on which to compute the difference.

#### Distinct

Distinct produces the unique values for a given column.

Distinct has the following properties:

* `column` string
    column the column on which to track unique values.


#### Shift

Shift add a fixed duration to time columns.
The output table schema is the same as the input table.

Shift has the following properties:

* `shift` duration
    shift is the amount to add to each time value.
    May be a negative duration.
* `columns` list of strings
    columns is the list of all columns that should be shifted.
    Defaults to `["_start", "_stop", "_time"]`

#### Type conversion operations

##### toBool

Convert a value to a bool.

Example: `from(bucket: "telegraf") |> filter(fn:(r) => r._measurement == "mem" and r._field == "used") |> toBool()`

The function `toBool` is defined as `toBool = (table=<-) => table |> map(fn:(r) => bool(v:r._value))`.
If you need to convert other columns use the `map` function directly with the `bool` function.

##### toInt

Convert a value to a int.

Example: `from(bucket: "telegraf") |> filter(fn:(r) => r._measurement == "mem" and r._field == "used") |> toInt()`

The function `toInt` is defined as `toInt = (table=<-) => table |> map(fn:(r) => int(v:r._value))`.
If you need to convert other columns use the `map` function directly with the `int` function.

##### toFloat

Convert a value to a float.

Example: `from(bucket: "telegraf") |> filter(fn:(r) => r._measurement == "mem" and r._field == "used") |> toFloat()`

The function `toFloat` is defined as `toFloat = (table=<-) => table |> map(fn:(r) => float(v:r._value))`.
If you need to convert other columns use the `map` function directly with the `float` function.

##### toDuration

Convert a value to a duration.

Example: `from(bucket: "telegraf") |> filter(fn:(r) => r._measurement == "mem" and r._field == "used") |> toDuration()`

The function `toDuration` is defined as `toDuration = (table=<-) => table |> map(fn:(r) => duration(v:r._value))`.
If you need to convert other columns use the `map` function directly with the `duration` function.

##### toString

Convert a value to a string.

Example: `from(bucket: "telegraf") |> filter(fn:(r) => r._measurement == "mem" and r._field == "used") |> toString()`

The function `toString` is defined as `toString = (table=<-) => table |> map(fn:(r) => string(v:r._value))`.
If you need to convert other columns use the `map` function directly with the `string` function.

##### toTime

Convert a value to a time.

Example: `from(bucket: "telegraf") |> filter(fn:(r) => r._measurement == "mem" and r._field == "used") |> toTime()`

The function `toTime` is defined as `toTime = (table=<-) => table |> map(fn:(r) => time(v:r._value))`.
If you need to convert other columns use the `map` function directly with the `time` function.

##### toUInt

Convert a value to a uint.

Example: `from(bucket: "telegraf") |> filter(fn:(r) => r._measurement == "mem" and r._field == "used") |> toUInt()`

The function `toUInt` is defined as `toUInt = (table=<-) => table |> map(fn:(r) => uint(v:r._value))`.
If you need to convert other columns use the `map` function directly with the `uint` function.


[IMPL#324](https://github.com/influxdata/platform/query/issues/324) Update specification around type conversion functions.


### Composite data types

A composite data type is a collection of primitive data types that together have a higher meaning.

#### Histogram data type

Histogram is a composite type that represents a discrete cumulative distribution.
Given a histogram with N buckets there will be N columns with the label `le_X` where `X` is replaced with the upper bucket boundary.

[IMPL#325](https://github.com/influxdata/platform/query/issues/325) Add support for a histogram composite data type.

### Triggers

A trigger is associated with a table and contains logic for when it should fire.
When a trigger fires its table is materialized.
Materializing a table makes it available for any down stream operations to consume.
Once a table is materialized it can no longer be modified.

Triggers can fire based on these inputs:

| Input                   | Description                                                                                       |
| -----                   | -----------                                                                                       |
| Current processing time | The current processing time is the system time when the trigger is being evaluated.               |
| Watermark time          | The watermark time is a time where it is expected that no data will arrive that is older than it. |
| Record count            | The number of records currently in the table.                                                     |
| Group key value         | The group key value of the table.                                                                 |

Additionally triggers can be _finished_, which means that they will never fire again.
Once a trigger is finished, its associated table is deleted.

Currently all tables use an _after watermark_ trigger which fires only once the watermark has exceeded the `_stop` value of the table and then is immediately finished.

Data sources are responsible for informing about updates to the watermark.

[IMPL#326](https://github.com/influxdata/platform/query/issues/326) Make trigger support not dependent on specific columns

### Execution model

A query specification defines what data and operations to perform.
The execution model reserves the right to perform those operations as efficiently as possible.
The execution model may rewrite the query in anyway it sees fit while maintaining correctness.

## Request and Response Formats

Included with the specification of the language and execution model, is a specification of how to submit queries and read their responses over HTTP.

### Request format

To submit a query for execution, make an HTTP POST request to the `/v1/query` endpoint.

The POST request may either submit parameters as the POST body or a subset of the parameters as URL query parameters.
The following parameters are supported:

| Parameter | Description                                                                                                                                       |
| --------- | -----------                                                                                                                                       |
| query     | Query is Flux text describing the query to run.  Only one of `query` or `spec` may be specified. This parameter may be passed as a URL parameter. |
| spec      | Spec is a query specification. Only one of `query` or `spec` may be specified.                                                                    |
| dialect   | Dialect is an object defining the options to use when encoding the response.                                                                      |


When using the POST body to submit the query the `Content-Type` HTTP header must contain the name of the request encoding being used.

Supported request content types:

* `application/json` - Use a JSON encoding of parameters.



Multiple response content types will be supported.
The desired response content type is specified using the `Accept` HTTP header on the request.
Each response content type will have its own dialect options.

Supported response encodings:

* `test/csv` - Corresponds with the MIME type specified in RFC 4180.
    Details on the encoding format are specified below.

If no `Accept` header is present it is assumed that `text/csv` was specified.
The HTTP header `Content-Type` of the response will specify the encoding of the response.

#### Examples requests

Make a request using a query string and URL query parameters:

```
POST /v1/query?query=%20from%28db%3A%22mydatabse%22%29%20%7C%3E%20last%28%29 HTTP/1.1
```

Make a request using a query string and the POST body as JSON:

```
POST /v1/query


{
    "query": "from(db:\"mydatabase\") |> last()"
}
```

Make a request using a query specification and the POST body as JSON:

```
POST /v1/query


{
    "spec": {
      "operations": [
        {
          "kind": "from",
          "id": "from0",
          "spec": {
            "db": "mydatabase"
          }
        },
        {
          "kind": "last",
          "id": "last1",
          "spec": {
            "column": ""
          }
        }
      ],
      "edges": [
        {
          "parent": "from0",
          "child": "last1"
        }
      ],
      "resources": {
        "priority": "high",
        "concurrency_quota": 0,
        "memory_bytes_quota": 0
      }
    }
}
```
Make a request using a query string and the POST body as JSON.
Dialect options are specified for the `text/csv` format.
See below for details on specific dialect options.

```
POST /v1/query


{
    "query": "from(db:\"mydatabase\") |> last()",
    "dialect" : {
        "header": true,
        "annotations": ["datatype"]
    }
}
```


### Response format

#### CSV

The result of a query is any number of named streams.
As a stream consists of multiple tables each table is encoded as CSV textual data.
CSV data should be encoded using UTF-8, and should be in Unicode Normal Form C as defined in [UAX15](https://www.w3.org/TR/2015/REC-tabular-data-model-20151217/#bib-UAX15).
Line endings must be CRLF as defined by the `text/csv` MIME type in RFC 4180

Each table may have the following rows:

* annotation rows - a set of rows describing properties about the columns of the table.
* header row - a single row that defines the column labels.
* record rows, a set of rows containing the record data, one record per row.

In addition to the columns on the tables themselves three additional columns may be added to the CSV table.

* annotation - Contains the name of an annotation.
    This column is optional, if it exists it is always the first column.
    The only valid values for the column are the list of supported annotations or an empty value.
* result - Contains the name of the result as specified by the query.
* table - Contains a unique ID for each table within a result.

Columns support the following annotations:

* datatype - a description of the type of data contained within the column.
* group - a boolean flag indicating if the column is part of the table's group key.
* default - a default value to be used for rows whose string value is the empty string.

##### Multiple tables

Multiple tables may be encoded into the same file or data stream.
The table column indicates the table a row belongs to.
All rows for a table must be contiguous.

It is possible that multiple tables in the same result do not share a common table scheme.
It is also possible that a table has no records.
In such cases an empty row delimits a new table boundary and new annotations and header rows follow.
The empty row acts like a delimiter between two independent CSV files that have been concatenated together.

In the case were a table has no rows the `default` annotation is used to provide the values of the group key.

##### Multiple results

Multiple results may be encoded into the same file or data stream.
An empty row always delimits separate results within the same file.
The empty row acts like a delimiter between two independent CSV files that have been concatenated together.

##### Annotations

Annotations rows are prefixed with a comment marker.
The first column contains the name of the annotation being defined.
The subsequent columns contain the value of the annotation for the respective columns.

The `datatype` annotation specifies the data types of the remaining columns.
The possible data types are:

| Datatype     | Flux type | Description                                                                          |
| --------     | --------- | -----------                                                                          |
| boolean      | bool      | a truth value, one of "true" or "false"                                              |
| unsignedLong | uint      | an unsigned 64-bit integer                                                           |
| long         | int       | a signed 64-bit integer                                                              |
| double       | float     | a IEEE-754 64-bit floating-point number                                              |
| string       | string    | a UTF-8 encoded string                                                               |
| base64Binary | bytes     | a base64 encoded sequence of bytes as defined in RFC 4648                            |
| dateTime     | time      | an instant in time, may be followed with a colon `:` and a description of the format |
| duration     | duration  | a length of time represented as an unsigned 64-bit integer number of nanoseconds     |

The `group` annotation specifies if the column is part of the table's group key.
Possible values are `true` or `false`.

The `default` annotation specifies a default value, if it exists, for each column.

In order to fully encode a table with its group key the `datatype`, `group` and `default` annotations must be used.

##### Errors

When an error occurs during execution a table will be returned with the first column label as `error` and the second column label as `reference`.
The error's properties are contained in the second row of the table.
The `error` column contains the error message and the `reference` column contains a unique reference code that can be used to get further information about the problem.

When an error occurs before any results are materialized then the HTTP status code will indicate an error and the error details will be encoded in the csv table.
When an error occurs after some results have already been sent to the client the error will be encoded as the next table and the rest of the results will be discarded.
In such a case the HTTP status code cannot be changed and will remain as 200 OK.

Example error encoding without annotations:

```
error,reference
Failed to parse query,897
```

##### Dialect options

The CSV response format support the following dialect options:


| Option        | Description                                                                                                                                             |
| ------        | -----------                                                                                                                                             |
| header        | Header is a boolean value, if true the header row is included, otherwise its is omitted. Defaults to true.                                              |
| delimiter     | Delimiter is a character to use as the delimiting value between columns.  Defaults to ",".                                                              |
| quoteChar     | QuoteChar is a character to use to quote values containing the delimiter. Defaults to `"`.                                                              |
| annotations   | Annotations is a list of annotations that should be encoded. If the list is empty the annotation column is omitted entirely. Defaults to an empty list. |
| commentPrefix | CommentPrefix is a string prefix to add to comment rows. Defaults to "#". Annotations are always comment rows.                                          |


##### Examples

For context the following example tables encode fictitious data in response to this query:

    from(db:"mydb")
        |> range(start:2018-05-08T20:50:00Z, stop:2018-05-08T20:51:00Z)
        |> group(by:["_start","_stop", "region", "host"])
        |> mean()
        |> group(by:["_start","_stop", "region"])
        |> yield(name:"mean")


Example encoding with of a single table with no annotations:

```
result,table,_start,_stop,_time,region,host,_value
mean,0,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:00Z,east,A,15.43
mean,0,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:20Z,east,B,59.25
mean,0,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:40Z,east,C,52.62
```


Example encoding with two tables in the same result with no annotations:

```
result,table,_start,_stop,_time,region,host,_value
mean,0,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:00Z,east,A,15.43
mean,0,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:20Z,east,B,59.25
mean,0,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:40Z,east,C,52.62
mean,1,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:00Z,west,A,62.73
mean,1,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:20Z,west,B,12.83
mean,1,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:40Z,west,C,51.62
```

Example encoding with two tables in the same result with no annotations and no header row:

```
mean,0,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:00Z,east,A,15.43
mean,0,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:20Z,east,B,59.25
mean,0,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:40Z,east,C,52.62
mean,1,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:00Z,west,A,62.73
mean,1,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:20Z,west,B,12.83
mean,1,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:40Z,west,C,51.62
```

Example encoding with two tables in the same result with the datatype annotation:

```
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,double
,result,table,_start,_stop,_time,region,host,_value
,mean,0,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:00Z,east,A,15.43
,mean,0,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:20Z,east,B,59.25
,mean,0,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:40Z,east,C,52.62
,mean,1,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:00Z,west,A,62.73
,mean,1,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:20Z,west,B,12.83
,mean,1,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:40Z,west,C,51.62
```

Example encoding with two tables in the same result with the datatype and group annotations:

```
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,double
#group,false,false,true,true,false,true,false,false
,result,table,_start,_stop,_time,region,host,_value
,mean,0,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:00Z,east,A,15.43
,mean,0,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:20Z,east,B,59.25
,mean,0,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:40Z,east,C,52.62
,mean,1,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:00Z,west,A,62.73
,mean,1,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:20Z,west,B,12.83
,mean,1,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:40Z,west,C,51.62
```

Example encoding with two tables with differing schemas in the same result with the datatype and group annotations:

```
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,double
#group,false,false,true,true,false,true,false,false
,result,table,_start,_stop,_time,region,host,_value
,mean,0,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:00Z,east,A,15.43
,mean,0,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:20Z,east,B,59.25
,mean,0,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:40Z,east,C,52.62

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,double
#group,false,false,true,true,false,true,false,false
,result,table,_start,_stop,_time,location,device,min,max
,mean,1,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:00Z,USA,5825,62.73,68.42
,mean,1,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:20Z,USA,2175,12.83,56.12
,mean,1,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:40Z,USA,6913,51.62,54.25
```

Example error encoding with the datatype annotation:

```
#datatype,string,long
,error,reference
,Failed to parse query,897
```

Example error encoding with after a valid table has already been encoded.

```
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,double
,result,table,_start,_stop,_time,region,host,_value
,mean,1,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:00Z,west,A,62.73
,mean,1,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:20Z,west,B,12.83
,mean,1,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:40Z,west,C,51.62

#datatype,string,long
,error,reference
,query terminated: reached maximum allowed memory limits,576
```

[IMPL#327](https://github.com/influxdata/platform/query/issues/327) Finalize csv encoding specification
