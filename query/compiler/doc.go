// The compiler package provides a compiler and Go runtime for a subset of the Flux language.
// Only pure functions are supported by the compiler.
// A function is compiled and then may be called repeatedly with different arguments.
// The function must be pure meaning it has no side effects. Other language features are not supported.
//
// This runtime is not portable by design. The runtime consists of Go types that have been constructed based on the Flux function being compiled.
// Those types are not serializable and cannot be transported to other systems or environments.
// This design is intended to limit the scope under which compilation must be supported.
package compiler
