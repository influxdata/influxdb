package uuid

import uuid "github.com/satori/go.uuid"

// V4 implements chronograf.ID
type V4 struct{}

// Generate creates a UUID v4 string
func (i *V4) Generate() (string, error) {
	return uuid.NewV4().String(), nil
}
