package chronograf

// Transformer will transform the `Response` data.
type Transformer interface {
	Transform(Response) (Response, error)
}
