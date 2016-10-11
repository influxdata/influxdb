package uuid

import uuid "github.com/satori/go.uuid"

type V4 struct{}

func (i *V4) Generate() (string, error) {
	return uuid.NewV4().String(), nil
}
