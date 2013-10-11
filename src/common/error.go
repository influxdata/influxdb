package common

const (
	WrongNumberOfArguments = iota
	InvalidArgument
	InternalError
)

type QueryError struct {
	ErrorCode int
	ErrorMsg  string
}

func (self *QueryError) Error() string {
	return self.ErrorMsg
}

func NewQueryError(code int, msg string) *QueryError {
	return &QueryError{code, msg}
}
