package check

// Response is a result of a collection of health checks.
type Response struct {
	Name    string    `json:"name"`
	Status  Status    `json:"status"`
	Message string    `json:"message,omitempty"`
	Checks  Responses `json:"checks,omitempty"`
}

// HasCheck verifies whether the receiving Response has a check with the given name or not.
func (r *Response) HasCheck(name string) bool {
	found := false
	for _, check := range r.Checks {
		if check.Name == name {
			found = true
			break
		}
	}
	return found
}

// Responses is a sortable collection of Response objects.
type Responses []Response

func (r Responses) Len() int { return len(r) }

// Less defines the order in which responses are sorted.
//
// Failing responses are always sorted before passing responses. Responses with
// the same status are then sorted according to the name of the check.
func (r Responses) Less(i, j int) bool {
	if r[i].Status == r[j].Status {
		return r[i].Name < r[j].Name
	}
	return r[i].Status < r[j].Status
}

func (r Responses) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
