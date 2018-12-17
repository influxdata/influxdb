package mock

type PagingFilter struct {
	Name string
	Type []string
}

func (f PagingFilter) QueryParams() map[string][]string {
	qp := map[string][]string{}
	qp["name"] = []string{f.Name}
	qp["type"] = f.Type
	return qp
}
