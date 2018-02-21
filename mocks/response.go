package mocks

// NewResponse returns a mocked chronograf.Response
func NewResponse(res string, err error) *Response {
	return &Response{
		res: res,
		err: err,
	}
}

// Response is a mocked chronograf.Response
type Response struct {
	res string
	err error
}

// MarshalJSON returns the res and err as the fake response.
func (r *Response) MarshalJSON() ([]byte, error) {
	return []byte(r.res), r.err
}
