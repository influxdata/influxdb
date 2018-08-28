package platform

// Status defines if a resource is active or inactive.
type Status string

const (
	// Active status means that the resource can be used.
	Active Status = "active"
	// Inactive status means that the resource cannot be used.
	Inactive Status = "inactive"
)
