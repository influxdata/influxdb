package storage

import (
	"errors"

	"google.golang.org/protobuf/types/known/anypb"
)

func GetReadSource(any *anypb.Any) (*ReadSource, error) {
	if any == nil {
		return nil, errors.New("reque")
	}
	var source ReadSource
	if err := any.UnmarshalTo(&source); err != nil {
		return nil, err
	}
	return &source, nil
}
