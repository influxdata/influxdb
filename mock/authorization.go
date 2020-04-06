package mock

import (
	platform "github.com/influxdata/influxdb/v2"
)

// Authorization is an Authorizer that always allows everything
type Authorization struct {
}

func (Authorization) Allowed(p platform.Permission) bool {
	return true
}

func (Authorization) Identifier() platform.ID {
	return mustID("beefdeaddeadbeef")
}

func (Authorization) GetUserID() platform.ID {
	return mustID("deadbeefbeefdead")
}

func (Authorization) Kind() string {
	return "mock-authorizer"
}

func mustID(str string) platform.ID {
	id, err := platform.IDFromString(str)
	if err != nil {
		panic(err)
	}
	return *id
}
