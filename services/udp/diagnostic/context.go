package diagnostic

type Context interface {
	Started(bindAddress string)
	Closed()
	CreateInternalStorageFailure(db string, err error)
	PointWriterError(database string, err error)
	ParseError(err error)
	ReadFromError(err error)
}
