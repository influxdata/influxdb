package proto

import (
	"encoding/json"

	platform "github.com/influxdata/influxdb"
	"go.uber.org/zap"
)

//go:generate env GO111MODULE=on go run github.com/kevinburke/go-bindata/go-bindata -o bin_gen.go -ignore Makefile|README|go -pkg proto .

// Load returns a list of all protos from within the release binary.
func Load(logger *zap.Logger) ([]*platform.Proto, error) {
	logger = logger.With(zap.String("service", "proto"))
	names := AssetNames()
	protos := make([]*platform.Proto, 0, len(names))
	for _, name := range names {
		b, err := Asset(name)
		if err != nil {
			logger.Info("error unable to load asset", zap.String("asset", name), zap.Error(err))
			return nil, err
		}

		proto := &platform.Proto{}
		if err := json.Unmarshal(b, proto); err != nil {
			logger.Info("error unmarshalling asset into proto", zap.String("asset", name), zap.Error(err))
		}

		protos = append(protos, proto)
	}

	return protos, nil
}
