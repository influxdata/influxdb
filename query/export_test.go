package query

// This file aliases unexported compilation internals to exported names so they
// are visible to test code in package query_test without widening the
// production API (test-only files are excluded from regular builds).

// CompiledStatement exposes compiledStatement so query_test can construct one
// via NewCompilerForTesting and drive its unexported methods.
type CompiledStatement = compiledStatement

// NewCompilerForTesting exposes newCompiler.
func NewCompilerForTesting(opt CompileOptions) *CompiledStatement {
	return newCompiler(opt)
}

// CompileTimeDimension exposes (*compiledStatement).compileTimeDimension.
var CompileTimeDimension = (*compiledStatement).compileTimeDimension

// Exported aliases for the compilation sentinel errors.
var (
	ErrOnlyTimeAndDatePartDimensions = errOnlyTimeAndDatePartDimensions
	ErrTimeDimensionArgCount         = errTimeDimensionArgCount
	ErrTimeDimensionDurationArg      = errTimeDimensionDurationArg
	ErrMultipleTimeDimensions        = errMultipleTimeDimensions
	ErrTimeOffsetFunctionMustBeNow   = errTimeOffsetFunctionMustBeNow
	ErrTimeOffsetNowNoArgs           = errTimeOffsetNowNoArgs
	ErrInvalidTimeOffset             = errInvalidTimeOffset
	ErrAtLeastOneNonTimeField        = errAtLeastOneNonTimeField
	ErrMixedMultipleSelectors        = errMixedMultipleSelectors
	ErrDatePartRequiresAggregate     = errDatePartRequiresAggregate
	ErrDatePartSingleAggregate       = errDatePartSingleAggregate
	ErrDatePartFillPrevious          = errDatePartFillPrevious
	ErrDatePartFillLinear            = errDatePartFillLinear
	ErrDatePartFillValue             = errDatePartFillValue
	ErrDatePartFillNull              = errDatePartFillNull
	ErrDatePartSubqueryCondition     = errDatePartSubqueryCondition
)
