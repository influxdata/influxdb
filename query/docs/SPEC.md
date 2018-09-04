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
    empty  in      or   package

[IMPL#256](https://github.com/influxdata/platform/issues/256) Add in and empty operator support  
[IMPL#334](https://github.com/influxdata/platform/issues/334) Add "import" support  

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


[IMPL#255](https://github.com/influxdata/platform/issues/255) Allow numeric literal coercion.

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

[IMPL#254](https://github.com/influxdata/platform/issues/254) Parse float literals

#### Duration literals

A duration literal is a representation of a length of time.
It has an integer part and a duration unit part.
Multiple durations may be specified together and the resulting duration is the sum of each smaller part.
When several durations are specified together, larger units must appear before smaller ones, and there can be no repeated units.

    duration_lit  = { int_lit duration_unit } .
    duration_unit = "y" | "mo" | "w" | "d" | "h" | "m" | "s" | "ms" | "us" | "µs" | "ns" .

| Units    | Meaning                                 |
| -----    | -------                                 |
| y        | year (12 months)                        |
| mo       | month                                   |
| w        | week (7 days)                           |
| d        | day                                     |
| h        | hour (60 minutes)                       |
| m        | minute (60 seconds)                     |
| s        | second                                  |
| ms       | milliseconds (1 thousandth of a second) |
| us or µs | microseconds (1 millionth of a second)  |
| ns       | nanoseconds (1 billionth of a second)   |

Durations represent a length of time.
Lengths of time are dependent on specific instants in time they occur and as such, durations do not represent a fixed amount of time.
No amount of seconds is equal to a day, as days vary in their number of seconds.
No amount of days is equal to a month, as months vary in their number of days.
A duration consists of three basic time units: seconds, days and months.

Durations can be combined via addition and subtraction.
Durations can be multiplied by an integer value.
These operations are performed on each time unit independently.

Examples:

    1s
    10d
    1h15m // 1 hour and 15 minutes
    5w
    1mo5d // 1 month and 5 days

Durations can be added to date times to produce a new date time.
Adding and subtracting durations to date times normalizes the dates.
For example July 32 converts to August 1.

Addition and subtraction of durations to date times do not commute and are left associative.
Addition and subtraction of durations to date times applies months, days and seconds in that order and then normalizes.

Examples:

    2018-01-01T00:00:00Z + 1d       // 2018-01-02T00:00:00Z
    2018-01-01T00:00:00Z + 1mo      // 2018-02-01T00:00:00Z
    2018-01-01T00:00:00Z + 2mo30d   // 2018-03-30T00:00:00Z
    2018-01-01T00:00:00Z + 1mo30d   // 2018-03-02T00:00:00Z, February 30th is normalized to March 2 in 2018 since it is not a leap year.

    // Addition and subtraction of durations to date times does not commute
    2018-02-28T00:00:00Z + 1mo + 1d // 2018-03-29T00:00:00Z
    2018-02-28T00:00:00Z + 1d + 1mo // 2018-04-01T00:00:00Z
    2018-01-01T00:00:00Z + 3mo - 1d // 2018-02-28T00:00:00Z
    2018-01-01T00:00:00Z - 1d + 3mo // 2018-03-03T00:00:00Z, December 31st + 3m0 is February 31st which is normalized to March 3 in 2018.

    // Addition and subtraction of durations to date times applies months, days and seconds in that order.
    2018-02-28T00:00:00Z + 1mo + 1d // 2018-03-29T00:00:00Z
    2018-02-28T00:00:00Z + 1mo1d    // 2018-03-29T00:00:00Z
    2018-02-28T00:00:00Z + 1d + 1mo // 2018-04-01T00:00:00Z, explicit left associative add of 1d first changes the result

[IMPL#253](https://github.com/influxdata/platform/issues/253) Parse duration literals

#### Date and time literals

A date and time literal represents a specific moment in time.
It has a date part, a time part and a time offset part.
The format follows the RFC 3339 specification.
The time is optional, when it is omitted the time is assumed to be midnight for the default location.
The time_offset is optional, when it is omitted the location option is used to determine the offset.

    date_time_lit     = date [ "T" time ] .
    date              = year_lit "-" month "-" day .
    year              = decimal_digit decimal_digit decimal_digit decimal_digit .
    month             = decimal_digit decimal_digit .
    day               = decimal_digit decimal_digit .
    time              = hour ":" minute ":" second [ fractional_second ] [ time_offset ] .
    hour              = decimal_digit decimal_digit .
    minute            = decimal_digit decimal_digit .
    second            = decimal_digit decimal_digit .
    fractional_second = "."  { decimal_digit } .
    time_offset       = "Z" | ("+" | "-" ) hour ":" minute .

Examples:

    1952-01-25T12:35:51Z
    2018-08-15T13:36:23-07:00
    2009-10-15T09:00:00       // October 15th 2009 at 9 AM in the default location
    2018-01-01                // midnight on January 1st 2018 in the default location

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


[IMPL#252](https://github.com/influxdata/platform/issues/252) Parse string literals


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


[IMPL#248](https://github.com/influxdata/platform/issues/248) Add printf function

Interpolation example:

    n = 42
    "the answer is {n}" // the answer is 42
    "the answer is not {n+1}" // the answer is not 43
    "openinng curly bracket \{" // openinng curly bracket {
    "closing curly bracket \}" // closing curly bracket }

[IMPL#251](https://github.com/influxdata/platform/issues/251) Add string interpolation support


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


[IMPL#250](https://github.com/influxdata/platform/issues/250) Parse regular expression literals

### Variables

A variable holds a value.
A variable can only hold values defined by its type.

### Types

A type defines the set of values and operations on those values.
Types are never explicitly declared as part of the syntax.
Types are always inferred from the usage of the value.

[IMPL#249](https://github.com/influxdata/platform/issues/249) Specify type inference rules

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

Durations can be added to times to produce a new time.

Examples:

    2018-07-01T00:00:00Z + 1mo // 2018-08-01T00:00:00Z
    2018-07-01T00:00:00Z + 2y  // 2020-07-01T00:00:00Z
    2018-07-01T00:00:00Z + 5h  // 2018-07-01T05:00:00Z

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


[IMPL#249](https://github.com/influxdata/platform/issues/249) Specify type inference rules

#### Generator types

A _generator type_ represents a value that produces an unknown number of other values.
The generated values may be of any other type but must all be the same type.

[IMPL#XXX](https://github.com/influxdata/platform/query/issues/XXX) Implement generators

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
4. The scope of the name of an imported package is the file block of the file containing the import declaration.
5. The scope of an identifier denoting a function argument is the function body.
6. The scope of a variable assigned inside a function is the innermost containing block.

An identifier assigned in a block may be reassigned in an inner block with the exception of option identifiers.
While the identifier of the inner assignment is in scope, it denotes the entity assigned by the inner assignment.

Option identifiers have default assignments that are automatically defined in the _options block_.
Because the _options block_ is the top-level block of a Flux program, options are visible/available to any and all other blocks.

The package clause is not a assignment; the package name does not appear in any scope.
Its purpose is to identify the files belonging to the same package and to specify the default package name for import declarations.


[IMPL#247](https://github.com/influxdata/platform/issues/247) Add package/namespace support

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


[IMPL#246](https://github.com/influxdata/platform/issues/246) Update parser to use formal EBNF grammar.

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

### Program

A Flux program is a sequence of statements defined by

    Program = [PackageStatement] [ImportList] StatementList .
    ImportList = { ImportStatement } .

### Statements

A statement controls execution.

    Statement = OptionStatement | VarAssignment |
                ReturnStatement | ExpressionStatement | BlockStatment .

#### Package statement

    PackageStatement = "package" identifier .

A package statement defines a package block.
Package names must be valid Flux identifiers.
The package statement must be the first statement of every Flux source file.
If a file does not declare a package statement, all identifiers in that file will belong to the special _main_ package.

##### package main

The _main_ package is special for a few reasons:

1. It defines the entrypoint of a Flux program
2. It cannot be imported
3. All query specifications produced after evaluating the _main_ package are coerced into producing side effects

#### Import statement

    ImportStatement = "import" [identifier] `"` unicode_char { unicode_char } `"`.

Associated with every package is a package name and an import path.
The import statement takes a package's import path and brings all of the identifiers defined in that package into the current scope.
The import statment defines a namespace through which to access the imported identifiers.
By default the identifer of this namespace is the package name unless otherwise specified.
For example, given a variable `x` declared in package `foo`, importing `foo` and referencing `x` would look like this:

```
import "import/path/to/package/foo"

foo.x
```

Or this:

```
import bar "import/path/to/package/foo"

bar.x
```

A package's import path is always absolute.
Flux does not support relative imports.
Assigment into the namespace of an imported package is not allowed.
A package cannot access nor modify the identifiers belonging to the imported packages of its imported packages.
Every statement contained in an imported package is evaluated.

#### Option statements

Options specify a context in which a Flux query is to be run. They define variables
that describe how to execute a Flux query. For example, the following Flux script sets
the `task` option to schedule a query to run periodically every hour:

    option task = {
        name: "mean",
        every: 1h,
    }

    from(bucket:"metrics/autogen")
        |> range(start:-task.every)
        |> group(by:["level"])
        |> mean()
        |> yield(name:"mean")

All options are designed to be completely optional and have default values to be used when not specified.
Grammatically, an option statement is just a variable assignment preceded by the "option" keyword.

    OptionStatement = "option" VarAssignment

Below is a list of all options that are currently implemented in the Flux language:

* now
* task
* location

##### now

The `now` option is a function that returns a time value to be used as a proxy for the current system time.

    // Query should execute as if the below time is the current system time
    option now = () => 2006-01-02T15:04:05-07:00

##### task

The `task` option is used by a scheduler to schedule the execution of a Flux query.

    option task = {
        name: "foo",        // name is required
        every: 1h,          // task should be run at this interval
        delay: 10m,         // delay scheduling this task by this duration
        cron: "0 2 * * *",  // cron is a more sophisticated way to schedule. every and cron are mutually exclusive
        retry: 5,           // number of times to retry a failed query
    }

##### location

The `location` option is used to set the default time zone of all times in the script.
The location maps the UTC offset in use at that location for a given time.
The default value is set using the time zone of the running process.

    option location = fixedZone(offset:-5h) // set timezone to be 5 hours west of UTC
    option location = loadLocation(name:"America/Denver") // set location to be America/Denver

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

### Side Effects

Side effects can occur in two ways.

1. By reassigning builtin options
2. By calling a function that produces side effects

A function produces side effects when it is explicitly declared to have side effects or when it calls a function that itself produces side effects.

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
* to
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

##### Time constants

###### Days of the week

Days of the week are represented as integers in the range `[0-6]`.
The following builtin values are defined:

```
Sunday    = 0
Monday    = 1
Tuesday   = 2
Wednesday = 3
Thursday  = 4
Friday    = 5
Saturday  = 6
```

###### Months of the year

Months are represented as integers in the range `[1-12]`.
The following builtin values are defined:

```
January   = 1
February  = 2
March     = 3
April     = 4
May       = 5
June      = 6
July      = 7
August    = 8
September = 9
October   = 10
November  = 11
December  = 12
```

##### Time and date functions

These are builtin functions that all take a single `time` argument and return an integer.

* `second` - integer
    Second returns the second of the minute for the provided time in the range `[0-59]`.
* `minute` - integer
    Minute returns the minute of the hour for the provided time in the range `[0-59]`.
* `hour` - integer
    Hour returns the hour of the day for the provided time in the range `[0-59]`.
* `weekDay` - integer
    WeekDay returns the day of the week for the provided time in the range `[0-6]`.
* `monthDay` - integer
    MonthDay returns the day of the month for the provided time in the range `[1-31]`.
* `yearDay` - integer
    YearDay returns the day of the year for the provided time in the range `[1-366]`.
* `month` - integer
    Month returns the month of the year for the provided time in the range `[1-12]`.

##### System Time

The builtin function `systemTime` returns the current system time.
All calls to `systemTime` within a single evaluation of a Flux script return the same time.

[IMPL#XXX](https://github.com/influxdata/platform/query/issues/XXX) Make systemTime consistent for a single evaluation.

#### Intervals

Intervals is a function that produces a set of time intervals over an interval.
An interval is an object with `start` and `stop` properties that correspond to the inclusive start and exclusive stop times of the time interval.
The return value of `intervals` is another function that accepts `start` and `stop` time parameters and returns an interval generator.
The generator is then used to produce the set of intervals.
The `intervals` function is designed to be used with the `intervals` parameter of the `window` function.

Intervals has the following parameters:

* `every` duration
    Every is the duration between starts of each of the intervals
* `period` duration
    Period is the length of each interval.
    It can be negative, indicating the start and stop boundaries are reversed.
    Defaults to the value of the `every` duration.
* `offset` duration
    Offset is the offset duration relative to the location offset.
    It can be negative, indicating that the offset goes backwards in time.
    Defaults to zero.
* `filter` function
    Filter accepts an interval object and returns a boolean value.
    Each potential interval is passed to the filter function, when the function returns false, that interval is excluded from the set of intervals.
    Defaults to include all intervals.

Examples:

    intervals(every:1h)                        // 1 hour intervals
    intervals(every:1h, period:2h)             // 2 hour long intervals every 1 hour
    intervals(every:1h, period:2h, offset:30m) // 2 hour long intervals every 1 hour starting at 30m past the hour
    intervals(every:1w, offset:1d)             // 1 week intervals starting on Monday (by default weeks start on Sunday)
    intervals(every:1d, period:-1h)            // the hour from 11PM - 12AM every night
    intervals(every:1mo, period:-1d)           // the last day of each month

Examples using a predicate:

    // 1 day intervals excluding weekends
    intervals(
        every:1d,
        filter: (interval) => !(weekday(time: interval.start) in [Sunday, Saturday]),
    )
    // Work hours from 9AM - 5PM on work days.
    intervals(
        every:1d,
        period:8h,
        offset:9h,
        filter:(interval) => !(weekday(time: interval.start) in [Sunday, Saturday]),
    )


[IMPL#XXX](https://github.com/influxdata/platform/query/issues/XXX) Implement intervals function


##### Builtin Intervals

The following builtin intervals exist:

    // 1 second intervals starting at the 0th millisecond
    seconds = intervals(every:1s)
    // 1 minute intervals starting at the 0th second
    minutes = intervals(every:1m)
    // 1 hour intervals starting at the 0th minute
    hours = intervals(every:1h)
    // 1 day intervals starting at midnight
    days = intervals(every:1d)
    // 1 day intervals excluding Sundays and Saturdays
    weekdays = intervals(every:1d, filter: (interval) => weekday(time:interval.start) not in [Sunday, Saturday])
    // 1 day intervals including only Sundays and Saturdays
    weekdends = intervals(every:1d, filter: (interval) => weekday(time:interval.start) in [Sunday, Saturday])
    // 1 week intervals starting on Sunday
    weeks = intervals(every:1w)
    // 1 month interval starting on the 1st of each month
    months = intervals(every:1mo)
    // 3 month intervals starting in January on the 1st of each month.
    quarters = intervals(every:3mo)
    // 1 year intervals starting on the 1st of January
    years = intervals(every:1y)



#### FixedZone

FixedZone creates a location based on a fixed time offset from UTC.


FixedZone has the following parameters:

* offset duration
    Offset is the offset from UTC for the time zone.
    Offset must be less than 24h.
    Defaults to 0, which produces the UTC location.

Examples:

    fixedZone(offset:-5h) // time zone 5 hours west of UTC
    fixedZone(offset:4h30m) // time zone 4 and a half hours east of UTC

#### LoadLocation

LoadLoacation loads a locations from a time zone database.

LoadLocation has the following parameters:

* name string
    Name is the name of the location to load.
    The names correspond to names in the [IANA tzdb](https://www.iana.org/time-zones).

Examples:

    loadLocation(name:"America/Denver")
    loadLocation(name:"America/Chicago")
    loadLocation(name:"Africa/Tunis")

## Query engine

The execution of a query is separate and distinct from the execution of Flux the language.
The input into the query engine is a query specification.

The output of a Flux program is a query specification, which then may be passed into the query execution engine.

### Query specification

A query specification consists of a set of operations and a set of edges between those operations.
The operations and edges must form a directed acyclic graph (DAG).
A query specification produces side effects when at least one of its operations produces side effects.

#### Encoding

The query specification may be encoded in different formats.
An encoding must consist of three properties:

* operations -  a list of operations and their specification.
* edges - a list of edges declaring a parent child relation between operations.
* resources - an optional set of constraints on the resources the query can consume.

Each operation has three properties:

* kind - kind is the name of the operation to perform.
* id - an identifier for this operation, it must be unique per query specification.
* spec - a set of properties that specify details of the operation.
    These vary by the kind of operation.

JSON encoding is supported and the following is an example encoding of a query:

```
from(bucket:"mydatabase/autogen") |> last()
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


#### Stream

A stream represents a potentially unbounded dataset.
A stream grouped into individual tables.
Within a stream each table's group key value is unique.

#### Missing values

A record may be missing a value for a specific column.
Missing values are represented with a special _null_ value.
The _null_ value can be of any data type.


[IMPL#300](https://github.com/influxdata/platform/issues/300) Design how nulls behave

#### Operations

An operation defines a transformation on a stream.
All operations may consume a stream and always produce a new stream.

Most operations output one table for every table they receive from the input stream.

Operations that modify the group keys or values will need to regroup the tables in the output stream.

An operation produces side effects when it is constructed from a function that produces side effects.

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


From has the following properties:

* `bucket` string
    Bucket is the name of the bucket to query
* `bucketID` string
    BucketID is the string encoding of the ID of the bucket to query.

Example:

    from(bucket:"telegraf/autogen")
    from(bucketID:"0261d8287f4d6000")

#### Yield

Yield indicates that the stream received by the yield operation should be delivered as a result of the query.
A query may have multiple results, each identified by the name provided to yield.

Yield outputs the input stream unmodified.

Yield has the following properties:

* `name` string
    unique name to give to yielded results

Example:
`from(bucket: "telegraf/autogen") |> range(start: -5m) |> yield(name:"1")`

**Note:** The `yield` function produces side effects.

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

Example:
`from(bucket: "telegraf) |> range(start:-5m) |> covariance(columns: [/autogen"x", "y"])`

##### Count

Count is an aggregate operation.
For each aggregated column, it outputs the number of non null records as an integer.

Example:
`from(bucket: "telegraf/autogen") |> range(start: -5m) |> count()`

#### Duplicate 
Duplicate will duplicate a specified column in a table

Duplicate has the following properties:

* `column` string
	The column to duplicate
* `as` string
	The name that should be assigned to the duplicate column

Example usage:

Duplicate column `server` under the name `host`:
```
from(bucket: "telegraf/autogen")
	|> range(start:-5m)
	|> filter(fn: (r) => r._measurement == "cpu")
	|> duplicate(column: "host", as: "server")
```

##### Integral

Integral is an aggregate operation.
For each aggregate column, it outputs the area under the curve of non null records.
The curve is defined as function where the domain is the record times and the range is the record values.

Integral has the following properties:

* `unit` duration
    unit is the time duration to use when computing the integral

Example: 

```
from(bucket: "telegraf/autogen") 
    |> range(start: -5m) 
    |> filter(fn: (r) => r._measurement == "cpu" and r._field == "usage_system") 
    |> integral(unit:10s)
```

##### Mean

Mean is an aggregate operation.
For each aggregated column, it outputs the mean of the non null records as a float.

Example: 
```
from(bucket:"telegraf/autogen")
    |> filter(fn: (r) => r._measurement == "mem" AND
            r._field == "used_percent")
    |> range(start:-12h)
    |> window(every:10m)
    |> mean()
```

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

Example:
```
// Determine 99th percentile cpu system usage:
from(bucket: "telegraf/autogen")
	|> range(start: -5m)
	|> filter(fn: (r) => r._measurement == "cpu" and r._field == "usage_system")
	|> percentile(p: 0.99)
```

##### Skew

Skew is an aggregate operation.
For each aggregated column, it outputs the skew of the non null record as a float.

Example:
```
from(bucket: "telegraf/autogen") 
    |> range(start: -5m)
    |> filter(fn: (r) => r._measurement == "cpu" and r._field == "usage_system")
    |> skew()
```

##### Spread

Spread is an aggregate operation.
For each aggregated column, it outputs the difference between the min and max values.
The type of the output column depends on the type of input column: for input columns with type `uint` or `int`, the output is an `int`; for `float` input columns the output is a `float`.
All other input types are invalid.

Example:
```
from(bucket: "telegraf/autogen") 
    |> range(start: -5m)
    |> filter(fn: (r) => r._measurement == "cpu" and r._field == "usage_system")
    |> spread()
```
##### Stddev

Stddev is an aggregate operation.
For each aggregated column, it outputs the standard deviation of the non null record as a float.

Example:
```
from(bucket: "telegraf/autogen") 
    |> range(start: -5m)
    |> filter(fn: (r) => r._measurement == "cpu" and r._field == "usage_system")
    |> stddev()
```

##### Sum

Stddev is an aggregate operation.
For each aggregated column, it outputs the sum of the non null record.
The output column type is the same as the input column type.

Example:
```
from(bucket: "telegraf/autogen") 
    |> range(start: -5m)
    |> filter(fn: (r) => r._measurement == "cpu" and r._field == "usage_system")
    |> sum()
```

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

##### First

First is a selector operation.
First selects the first non null record from the input table.

Example:
`from(bucket:"telegraf/autogen") |> first()`

##### Last

Last is a selector operation.
Last selects the last non null record from the input table.

Example:
`from(bucket: "telegraf/autogen") |> last()`

##### Max

Max is a selector operation.
Max selects the maximum record from the input table.

Example:
```
from(bucket:"telegraf/autogen")
    |> range(start:-12h)
    |> filter(fn: (r) => r._measurement == "cpu" AND r._field == "usage_system")
    |> max()
```

##### Min

Min is a selector operation.
Min selects the minimum record from the input table.

Example: 

```
from(bucket:"telegraf/autogen")
    |> range(start:-12h)
    |> filter(fn: (r) => r._measurement == "cpu" AND r._field == "usage_system")
    |> min()
```

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

Example:

```
from(bucket:"telegraf/autogen")
    |> filter(fn: (r) => r._measurement == "cpu" AND
               r._field == "usage_system")
    |> range(start:-1d)
    |> sample(n: 5, pos: 1)
```


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

Example: 

```
from(bucket:"telegraf/autogen")
    |> range(start:-12h)
    |> filter(fn: (r) => r._measurement == "cpu" AND
                r._field == "usage_system" AND
                r.service == "app-server")
```

#### Histogram

Histogram approximates the cumulative distribution function of a dataset by counting data frequencies for a list of buckets.
A bucket is defined by an upper bound where all data points that are less than or equal to the bound are counted in the bucket.
The bucket counts are cumulative.

Each input table is converted into a single output table representing a single histogram.
The output table will have a the same group key as the input table.
The columns not part of the group key will be removed and an upper bound column and a count column will be added.

Histogram has the following properties:

* `column` string
    Column is the name of a column containing the input data values.
    The column type must be float.
    Defaults to `_value`.
* `upperBoundColumn` string
    UpperBoundColumn is the name of the column in which to store the histogram upper bounds.
    Defaults to `le`.
* `countColumn` string
    CountColumn is the name of the column in which to store the histogram counts.
    Defaults to `_value`.
* `buckets` array of floats
    Buckets is a list of upper bounds to use when computing the histogram frequencies.
    Buckets should contain a bucket whose bound is the maximum value of the data set, this value can be set to positive infinity if no maximum is known.
* `normalize` bool
    Normalize when true will convert the counts into frequencies values between 0 and 1.
    Normalized histograms cannot be aggregated by summing their counts.
    Defaults to `false`.


Example:

    histogram(buckets:linearBuckets(start:0.0,width:10.0,count:10))  // compute the histogram of the data using 10 buckets from 0,10,20,...,100

#### HistogramQuantile

HistogramQuantile approximates a quantile given an histogram that approximates the cumulative distribution of the dataset.
Each input table represents a single histogram.
The histogram tables must have two columns, a count column and an upper bound column.
The count is the number of values that are less than or equal to the upper bound value.
The table can have any number of records, each representing an entry in the histogram.
The counts must be monotonically increasing when sorted by upper bound.

Linear interpolation between the two closest bounds is used to compute the quantile.
If the either of the bounds used in interpolation are infinite, then the other finite bound is used and no interpolation is performed.

The output table will have a the same group key as the input table.
The columns not part of the group key will be removed and a single value column of type float will be added.
The count and upper bound columns must not be part of the group key.
The value column represents the value of the desired quantile from the histogram.

HistogramQuantile has the following properties:

* `quantile` float
    Quantile is a value between 0 and 1 indicating the desired quantile to compute.
* `countColumn` string
    CountColumn is the name of the column containing the histogram counts.
    The count column type must be float.
    Defaults to `_value`.
* `upperBoundColumn` string
    UpperBoundColumn is the name of the column containing the histogram upper bounds.
    The upper bound column type must be float.
    Defaults to `le`.
* `valueColumn` string
    ValueColumn is the name of the output column which will contain the computed quantile.
    Defaults to `_value`.
* `minValue` float
    MinValue is the assumed minumum value of the dataset.
    When the quantile falls below the lowest upper bound, interpolation is performed between
    minValue and the lowest upper bound.
    When minValue is equal to negative infinity, the lowest upper bound is used.
    Defaults to 0.

Example:

    histogramQuantile(quantile:0.9)  // compute the 90th quantile using histogram data.

#### LinearBuckets

LinearBuckets produces a list of linearly separated floats.

LinearBuckets has the following properties:

* `start` float
    Start is the first value in the returned list.
* `width` float
    Width is the distance between subsequent bucket values.
* `count` int
    Count is the number of buckets to create.
* `inifinity` bool
    Infinity when true adds an additional bucket with a value of positive infinity.
    Defaults to `true`.

#### LogrithmicBuckets

LogrithmicBuckets produces a list of exponentially separated floats.

LogrithmicBuckets has the following properties:

* `start` float
    Start is the first value in the returned bucket list.
* `factor` float
    Factor is the multiplier applied to each subsequent bucket.
* `count` int
    Count is the number of buckets to create.
* `inifinity` bool
    Infinity when true adds an additional bucket with a value of positive infinity.
    Defaults to `true`.

#### Limit

Limit caps the number of records in output tables to a fixed size n.
One output table is produced for each input table.
The output table will contain the first n records from the input table.
If the input table has less than n records all records will be output.

Limit has the following properties:

* `n` int
    The maximum number of records to output.

Example: `from(bucket: "telegraf/autogen") |> limit(n: 10)`

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

Example:
```
from(bucket:"telegraf/autogen")
    |> filter(fn: (r) => r._measurement == "cpu" AND
                r._field == "usage_system" AND
                r.service == "app-server")
    |> range(start:-12h)
    // Square the value
    |> map(fn: (r) => r._value * r._value)
```
Example (creating a new table):
```
from(bucket:"telegraf/autogen")
    |> filter(fn: (r) => r._measurement == "cpu" AND
                r._field == "usage_system" AND
                r.service == "app-server")
    |> range(start:-12h)
    // create a new table by copying each row into a new format
    |> map(fn: (r) => {_time: r._time, app_server: r._service})
```

#### Range

Range filters records based on provided time bounds.
Each input tables records are filtered to contain only records that exist within the time bounds.
Each input table's group key value is modified to fit within the time bounds.
Tables where all records exists outside the time bounds are filtered entirely.


[IMPL#244](https://github.com/influxdata/platform/issues/244) Update range to default to aligned window ranges.

Range has the following properties:

* `start` duration or timestamp
    Specifies the oldest time to be included in the results
* `stop` duration or timestamp
    Specifies the exclusive newest time to be included in the results.
    Defaults to the value of the `now` option time.

Example:
```
from(bucket:"telegraf/autogen")
    |> range(start:-12h, stop: -15m)
    |> filter(fn: (r) => r._measurement == "cpu" AND
               r._field == "usage_system")
```
Example:
```
from(bucket:"telegraf/autogen")
    |> range(start:2018-05-22T23:30:00Z, stop: 2018-05-23T00:00:00Z)
    |> filter(fn: (r) => r._measurement == "cpu" AND
               r._field == "usage_system")
```
#### Rename 

Rename will rename specified columns in a table. 
There are two variants: one which takes a map of old column names to new column names,
and one which takes a mapping function. 
If a column is renamed and is part of the group key, the column name in the group key will be updated.

Rename has the following properties: 
* `columns` object
	A map of columns to rename and their corresponding new names. Cannot be used with `fn`. 
* `fn` function 
    A function which takes a single string parameter (the old column name) and returns a string representing 
    the new column name. Cannot be used with `columns`.

Example usage:

Rename a single column: 
```
from(bucket: "telegraf/autogen")
    |> range(start: -5m)
    |> rename(columns:{host: "server"})
```
Rename all columns using `fn` parameter: 
```
from(bucket: "telegraf/autogen")
    |> range(start: -5m)
    |> rename(fn: (col) => "{col}_new")
```

#### Drop 

Drop will exclude specified columns from a table. Columns to exclude can be specified either through a 
list, or a predicate function. 
When a dropped column is part of the group key it will also be dropped from the key.

Drop has the following properties:
* `columns` array of strings 
    An array of columns which should be excluded from the resulting table. Cannot be used with `fn`.
* `fn` function 
    A function which takes a column name as a parameter and returns a boolean indicating whether
    or not the column should be excluded from the resulting table. Cannot be used with `columns`.  

Example Usage:

Drop a list of columns
```
from(bucket: "telegraf/autogen")
	|> range(start: -5m)
	|> drop(columns: ["host", "_measurement"])
```
Drop columns matching a predicate:
```
from(bucket: "telegraf/autogen")
    |> range(start: -5m)
    |> drop(fn: (col) => col =~ /usage*/)
```

#### Keep 

Keep is the inverse of drop. It will return a table containing only columns that are specified,
ignoring all others. 
Only columns in the group key that are also specified in `keep` will be kept in the resulting group key.

Keep has the following properties: 
* `columns` array of strings
    An array of columns that should be included in the resulting table. Cannot be used with `fn`.
* `fn` function
    A function which takes a column name as a parameter and returns a boolean indicating whether or not
    the column should be included in the resulting table. Cannot be used with `columns`. 

Example Usage:

Keep a list of columns: `keep(columns: ["_time", "_value"])`

Keep all columns matching a predicate: `keep(fn: (col) => col =~ /inodes*/)`

Keep a list of columns:
```
from(bucket: "telegraf/autogen")
    |> range(start: -5m)
    |> keep(columns: ["_time", "_value"])
```
Keep all columns matching a predicate:
```
from(bucket: "telegraf/autogen")
    |> range(start: -5m)
    |> keep(fn: (col) => col =~ /inodes*/) 
```

#### Set

Set assigns a static value to each record.
The key may modify and existing column or it may add a new column to the tables.
If the column that is modified is part of the group key, then the output tables will be regroup as needed.


Set has the following properties:

* `key` string
    key is the label for the column to set
* `value` string
    value is the string value to set

Example: 
```
from(bucket: "telegraf/autogen") |> set(key: "mykey", value: "myvalue")
```

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

Example:
```
from(bucket:"telegraf/autogen")
    |> filter(fn: (r) => r._measurement == "system" AND
               r._field == "uptime")
    |> range(start:-12h)
    |> sort(cols:["region", "host", "value"])
```
#### Group

Group groups records based on their values for specific columns.
It produces tables with new group keys based on the provided properties.

Group has the following properties:

*  `by` list of strings
    Group by these specific columns.
    Cannot be used with `except`.
*  `except` list of strings
    Group by all other columns except this list.
    Cannot be used with `by`.

Examples:
    group(by:["host"]) // group records by their "host" value
    group(except:["_time", "region", "_value"]) // group records by all other columns except for _time, region, and _value
    group(by:[]) // group all records into a single group
    group(except:[]) // group records into all unique groups

```
from(bucket: "telegraf/autogen") 
    |> range(start: -30m) 
    |> group(by: ["host", "_measurement"])
```
All records are grouped by the "host" and "_measurement" columns. The resulting group key would be ["host, "_measurement"]

```
from(bucket: "telegraf/autogen")
    |> range(start: -30m)
    |> group(except: ["_time"])
```
All records are grouped by the set of all columns in the table, excluding "_time". For example, if the table has columns ["_time", "host", "_measurement", "_field", "_value"] then the group key would be ["host", "_measurement", "_field", "_value"]


#### Window

Window groups records based on a time value.
New columns are added to uniquely identify each window and those columns are added to the group key of the output tables.

A single input record will be placed into zero or more output tables, depending on the specific windowing function.

Window has the following properties:

* `every` duration
    Duration of time between windows.
    Defaults to `period`'s value
    One of `every`, `period` or `intervals` must be provided.
* `period` duration
    Duration of the window.
    Period is the length of each interval.
    It can be negative, indicating the start and stop boundaries are reversed.
    Defaults to `every`'s value
    One of `every`, `period` or `intervals` must be provided.
* `offset` time
    The offset duration relative to the location offset.
    It can be negative, indicating that the offset goes backwards in time.
    The default aligns the window boundaries to line up with the `now` option time.
* `intervals` function that returns an interval generator
    A set of intervals to be used as the windows.
    One of `every`, `period` or `intervals` must be provided.
    When `intervals` is provided, `every`, `period`, and `offset` must be zero.
* `timeCol` string
    Name of the time column to use.
    Defaults to `_time`.
* `startCol` string
    Name of the column containing the window start time.
    Defaults to `_start`.
* `stopCol` string
    Name of the column containing the window stop time.
    Defaults to `_stop`.

Example: 
```
from(bucket:"telegraf/autogen")
    |> range(start:-12h)
    |> window(every:10m)
    |> max()
```

```
window(every:1h) // window the data into 1 hour intervals
window(intervals: intervals(every:1d, period:8h, offset:9h)) // window the data into 8 hour intervals starting at 9AM every day.
```

#### Pivot

Pivot collects values stored vertically (column-wise) in a table and aligns them horizontally (row-wise) into logical sets.  


Pivot has the following properties:

* `rowKey` array of strings
    List of columns used to uniquely identify a row for the output.
* `colKey` array of strings
    List of columns used to pivot values onto each row identified by the rowKey. 
* `valueCol` string
    Identifies the single column that contains the value to be moved around the pivot
    

The group key of the resulting table will be the same as the input tables, excluding the columns found in the colKey and valueCol. 
This is because these columns are not part of the resulting output table.  

Every input row should have a 1:1 mapping to a particular row + column in the output table, determined by its values for the rowKey and colKey.   
In the case where more than one value is identified for the same row+column pair in the output, the last value 
encountered in the set of table rows is taken as the result.

The output table will have columns based on the row key, plus the group key (minus any group key colums in the column key) 
plus new columns for each unique tuple of values identified by the column key.  
Any columns in the original table that are not referenced in the rowKey or the original table's group key will be dropped.  

The output is constructed as follows:
1. A new row is created for each unique value identified in the input by the rowKey parameter.
2. The initial set of columns for the new row is the row key unioned with the group key, but excluding the columns indicated by the colKey and the valueCol.
3. A set of value columns are added to the row for each unique value identified in the input by the columnKey parameter. 
The label is a concatenation of the valueCol string and the colKey values using '_' as a separator. 
4. For each rowKey, columnKey pair, the appropriate value is determined from the input table by the valueCol. 
If no value is found, the value is set to `null`.

[IMPL#353](https://github.com/influxdata/platform/issues/353) Null defined in spec but not implemented.  

#### FromRows

FromRows is a special application of pivot that will automatically align fields within each measurement that have the same time stamp.
Its definition is: 

```
  fromRows = (bucket) => from(bucket:bucket) |> pivot(rowKey:["_time"], colKey: ["_field"], valueCol: "_value")
```

Example: 

```
fromRows(bucket:"telegraf/autogen")
  |> range(start: 2018-05-22T19:53:26Z)
```

#### Join

Join merges two or more input streams, whose values are equal on a set of common columns, into a single output stream.
The resulting schema is the union of the input schemas, and the resulting group key is the union of the input group keys.

For example, given the following two streams of data:

* SF_Temperature

    | _time | _field | _value |
    | ----- | ------ | ------ |
    | 0001  | "temp" | 70 |
    | 0002  | "temp" | 75 |
    | 0003  | "temp" | 72 |

* NY_Temperature

    | _time | _field | _value |
    | ----- | ------ | ------ |
    | 0001  | "temp" | 55 |
    | 0002  | "temp" | 56 |
    | 0003  | "temp" | 55 |

And the following join query: `join(tables: {sf: SF_Temperature, ny: NY_Temperature}, on: ["_time", "_field"])`

The output will be:

| _time | _field | ny__value | sf__value |
| ----- | ------ |---------- | --------- |
| 0001  | "temp" | 55 | 70 |
| 0002  | "temp" | 56 | 75 |
| 0003  | "temp" | 55 | 72 |


##### options

The join operator accepts the following named parameters:

| Name | Type | Required | Default Value | Possible Values |
| ---- | ---- | -------- | ------- | ------ |
| tables    | map           | yes   | no default value - must be specified with every call | N/A |
| on        | string array  | no    | list of columns to join on | N/A |
| method    | string        | no    | inner | inner, cross, left, right, or outer |

* tables

    Map of tables (or streams) to join together. It is the one required parameter of the join.

* on

    An optional parameter for specifying a list of columns to join on.
    Defaults to the set of columns that are common to all of the input streams.

* method

    An optional parameter that specifies the type of join to be performed.
    When not specified, an inner join is performed.
    The **method** parameter may take on any one of the following values:

    * inner - inner join

    * cross - cross product

    * left - left outer join

    * right - right outer join

    * outer - full outer join

The **on** parameter and the **cross** method are mutually exclusive.


##### output schema

The column schema of the output stream is the union of the input schemas, and the same goes for the output group key.
Columns that must be renamed due to ambiguity (i.e. columns that occur in more than one input stream) are renamed
according to the template `<table>_<column>`.

Examples:

* SF_Temperature
* Group Key {"_field"}

    | _time | _field | _value |
    | ----- | ------ | ------ |
    | 0001  | "temp" | 70 |
    | 0002  | "temp" | 75 |
    | 0003  | "temp" | 72 |

* NY_Temperature
* Group Key {"_time", "_field"}

    | _time | _field | _value |
    | ----- | ------ | ------ |
    | 0001  | "temp" | 55 |
    | 0002  | "temp" | 56 |
    | 0003  | "temp" | 55 |

`join(tables: {sf: SF_Temperature, ny: NY_Temperature}, on: ["_time"])` produces:

* Group Key {"_time", "sf__field", "ny__field"}

    | _time | sf__field | sf__value | ny__field | ny__value |
    | ----- | ------ | ---------- | -------- |--------- |
    | 0001  | "temp" | 70 | "temp" | 55 |
    | 0002  | "temp" | 75 | "temp" | 56 |
    | 0003  | "temp" | 72 | "temp: | 55 |


#### Cumulative sum

Cumulative sum computes a running sum for non null records in the table.
The output table schema will be the same as the input table.

Cumulative sum has the following properties:

* `columns` list string
    columns is a list of columns on which to operate.

Example:
```
from(bucket: "telegraf/autogen")
    |> range(start: -5m)
    |> filter(fn: (r) => r._measurement == "disk" and r._field == "used_percent")
    |> cumulativeSum(columns: ["_value"])
```

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

```
from(bucket: "telegraf/autogen")
    |> range(start: -5m)
    |> filter(fn: (r) => r._measurement == "disk" and r._field == "used_percent")
    |> derivative(nonNegative: true, columns: ["used_percent"])
```

#### Difference

Difference computes the difference between subsequent non null records.

Difference has the following properties:

* `nonNegative` bool
    nonNegative indicates if the derivative is allowed to be negative.
    If a value is encountered which is less than the previous value then it is assumed the previous value should have been a zero.
* `columns` list strings
    columns is a list of columns on which to compute the difference.

```
from(bucket: "telegraf/autogen")
    |> range(start: -5m)
    |> filter(fn: (r) => r._measurement == "cpu" and r._field == "usage_user")
    |> difference()
```
#### Distinct

Distinct produces the unique values for a given column.

Distinct has the following properties:

* `column` string
    column the column on which to track unique values.

Example: 
```
from(bucket: "telegraf/autogen")
	|> range(start: -5m)
	|> filter(fn: (r) => r._measurement == "cpu")
	|> distinct(column: "host")
```

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
Example:
```
from(bucket: "telegraf/autogen")
	|> range(start: -5m)
	|> shift(shift: 1000h)
```


#### To

The To operation takes data from a stream and writes it to a bucket.
To has the following properties:

* `bucket` string  
    The bucket to which data will be written.
* `bucketID` string  
    The ID of the bucket to which data will be written.
* `org` string  
    The organization name of the above bucket.
* `orgID` string  
    The organization ID of the above bucket.
* `host` string  
    The remote host to write to. 
* `token` string  
    The authorization token to use when writing to a remote host.
* `timeColumn` string  
    The time column of the output.  
    **Default:** `"_time"`
* `tagColumns` list of strings  
    The tag columns of the output.  
    **Default:** All columns of type string, excluding all value columns and the `_field` column if present.
* `fieldFn` function(record) object  
    Function that takes a record from the input table and returns an object.  
    For each record from the input table `fieldFn` returns on object that maps output field key to output value.  
    **Default:** `(r) => ({ [r._field]: r._value })`

Either `bucket` or `bucketID` is required.
Both are mutually exclusive.
Similarly `org` and `orgID` are mutually exclusive and only required when writing to a remote host.
Both `host` and `token` are optional parameters, however if `host` is specified, `token` is required.


For example, given the following table:

| _time | _start | _stop | _measurement | _field | _value |
| ----- | ------ | ----- | ------------ | ------ | ------ |
| 0005  | 0000   | 0009  | "a"          | "temp" | 100.1  |
| 0006  | 0000   | 0009  | "a"          | "temp" | 99.3   |
| 0007  | 0000   | 0009  | "a"          | "temp" | 99.9   |

The default `to` operation `to(bucket:"my-bucket", org:"my-org")` is equivalent to writing the above data using the following line protocol:

```
_measurement=a temp=100.1 0005
_measurement=a temp=99.3 0006
_measurement=a temp=99.9 0007
```

For an example overriding `to`'s default settings, given the following table:

| _time | _start | _stop | tag1 | tag2 | hum | temp |
| ----- | ------ | ----- | ---- | ---- | ---- | ---- |
| 0005  | 0000   | 0009  | "a"  | "b"  | 55.3 | 100.1  |
| 0006  | 0000   | 0009  | "a"  | "b"  | 55.4 | 99.3   |
| 0007  | 0000   | 0009  | "a"  | "b"  | 55.5 | 99.9   |

The operation `to(bucket:"my-bucket", org:"my-org", tagColumns:["tag1"], fieldFn: (r) => return {"hum": r.hum, "temp": r.temp})` is equivalent to writing the above data using the following line protocol:

```
_tag1=a hum=55.3,temp=100.1 0005
_tag1=a hum=55.4,temp=99.3 0006
_tag1=a hum=55.5,temp=99.9 0007
```

**Note:** The `to` function produces side effects.

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


[IMPL#242](https://github.com/influxdata/platform/issues/242) Update specification around type conversion functions.


### Composite data types

A composite data type is a collection of primitive data types that together have a higher meaning.

#### Histogram data type

Histogram is a composite type that represents a discrete cumulative distribution.
Given a histogram with N buckets there will be N columns with the label `le_X` where `X` is replaced with the upper bucket boundary.

[IMPL#241](https://github.com/influxdata/platform/issues/241) Add support for a histogram composite data type.

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

[IMPL#240](https://github.com/influxdata/platform/issues/240) Make trigger support not dependent on specific columns

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
    "query": "from(bucket:\"mydatabase/autogen\") |> last()"
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
    "query": "from(bucket:\"mydatabase/autogen\") |> last()",
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

    from(bucket:"mydb/autogen")
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
