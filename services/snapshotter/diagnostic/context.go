package diagnostic

type Context interface {
	Starting()
	Closed()
	AcceptError(err error)
	Error(err error)
}
