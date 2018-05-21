# IFQL Language

This document details the design of the IFQL langauage.
If you are looking for usage information on the langauage see the README.md.

# Overview

The IFQL langauage is used to construct query specifications.

# Syntax

The langauage syntax is defined by the ifql/platform/query.peg grammar.

## Keyword Arguments

IFQL uses keyword arguments for ALL arguments to ALL functions.
Keyword arguments enable iterative improvements to the langauage while remaining backwards compatible.

Since IFQL is functional in style it is important to note that the choice of keyword arguments means that many functional concepts that deal with positional arguments have to be mapped into a space where only keyword arguments exist.

### Default Arguments

Since all arguments are keyword arguments and there are no positional arguments it is possible for any argument to have a default value.
If an argument is not specified at call time, then if the argument has a default it is used, otherwise an error occurs.

## Abstract Syntax Tree

The abstract syntax tree (AST) of IFQL is closely modeled after the javascript AST.
Using the javascript AST provides a good foundation for organization and structure of the syntax tree.
Since IFQL is so similar to javascript this design works well.

# Semantics

The `semantic` package provides a graph structure that represents the meaning of an IFQL script.
An AST is converted into a semantic graph for use with other systems.
Using a semantic graph representation of the IFQL, enables highlevel meaning to be specified programatically.

For example since IFQL uses the javascript AST structures, arguments to a function are represented as a single positional argument that is always an object expression.
The semantic graph validates that the AST correctly follows these semantics, and use structures that are strongly typed for this expectation.

The semantic structures are to be designed to facilitate the interpretation and compilation of IFQL.

# Interpretation

IFQL is primarily an interpreted language.
The implementation of the IFQL interpreter can be found in the `interpreter` package.

# Compilation and Go Runtime

A subset of IFQL can be compiled into a runtime hosted in Go.
The subset consists of only pure functions.
Meaning a function defintion in IFQL can be compiled and then called repeatedly with different arguments.
The function must be pure, meaning it has no side effects.
Other language feature like imports etc are not supported.

This runtime is entirely not portable.
The runtime consists of Go types that have been constructed based on the IFQL function being compiled.
Those types are not serializable and cannot be transported to other systems or environments.
This design is intended to limit the scope under which compilation must be supported.

# Features

This sections details various features of the language.

## Functions

IFQL supports defining functions.

Example:

```
add = (a,b) => a + b

add(a:1, b:2) // 3
```

Functions can be assigned to identifiers and can call other functions.
Functions are first class types within IFQL.

## Scoping

IFQL uses lexical scoping.
Scoping boundaries occur at functions.

Example:

```
x = 5
addX = (a) => a + x

add(a:1) // 6
```

The `x` referred to in the `addX` function is the same as is defined in the toplevel scope.

Scope names can be changed for more specific scopes.

Example:

```
x = 5

add = (x,y) => x + y

add(x:1,y:2) // 3
```

In this example the `x = 5` definition is unused, as the `add` function defines it own local identifier `x` as a parameter.

