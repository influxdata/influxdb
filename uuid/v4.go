package uuid

import uuid "github.com/satori/go.uuid"

type V4 struct {
	u *uuid.UUID
}

func (i *V4) Generate() (string, error) {
	if i.u == nil {
		u := uuid.NewV4()
		i.u = &u
	}
	return i.u.String(), nil
}
