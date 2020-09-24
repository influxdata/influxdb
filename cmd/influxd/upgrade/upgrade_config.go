package upgrade

import (
	"errors"

	"go.uber.org/zap"
)

// Backups existing config file and updates it with upgraded config.
func upgradeConfig(configFile string, targetOptions optionsV2, log *zap.Logger) (*configV1, error) {
	return nil, errors.New("not implemented")
}
