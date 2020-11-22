package upgrade

// Configuration file upgrade implementation.
// The strategy is to transform only those entries for which rule exists.

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
	"go.uber.org/zap"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

// configMapRules is a map of transformation rules
var configMapRules = map[string]string{
	"reporting-disabled":                                   "reporting-disabled",
	"data.dir":                                             "engine-path",
	"data.wal-fsync-delay":                                 "storage-wal-fsync-delay",
	"data.validate-keys":                                   "storage-validate-keys",
	"data.cache-max-memory-size":                           "storage-cache-max-memory-size",
	"data.cache-snapshot-memory-size":                      "storage-cache-snapshot-memory-size",
	"data.cache-snapshot-write-cold-duration":              "storage-cache-snapshot-write-cold-duration",
	"data.compact-full-write-cold-duration":                "storage-compact-full-write-cold-duration",
	"data.compact-throughput-burst":                        "storage-compact-throughput-burst",
	"data.max-concurrent-compactions":                      "storage-max-concurrent-compactions",
	"data.max-index-log-file-size":                         "storage-max-index-log-file-size",
	"data.series-id-set-cache-size":                        "storage-series-id-set-cache-size",
	"data.series-file-max-concurrent-snapshot-compactions": "storage-series-file-max-concurrent-snapshot-compactions",
	"data.tsm-use-madv-willneed":                           "storage-tsm-use-madv-willneed",
	"retention.check-interval":                             "storage-retention-check-interval",
	"shard-precreation.check-interval":                     "storage-shard-precreator-check-interval",
	"shard-precreation.advance-period":                     "storage-shard-precreator-advance-period",
	"coordinator.max-concurrent-queries":                   "query-concurrency",
	"coordinator.max-select-point":                         "influxql-max-select-point",
	"coordinator.max-select-series":                        "influxql-max-select-series",
	"coordinator.max-select-buckets":                       "influxql-max-select-buckets",
	"logging.level":                                        "log-level",
	"http.bind-address":                                    "http-bind-address",
	"http.https-certificate":                               "tls-cert",
	"http.https-private-key":                               "tls-key",
}

// configValueTransforms is a map from 2.x config keys to transformation functions
// that should run on the 1.x values before they're written into the 2.x config.
var configValueTransforms = map[string]func(interface{}) interface{}{
	// Transform config values of 0 into 10 (the new default).
	// query-concurrency used to accept 0 as a representation of infinity,
	// but the 2.x controller now forces a positive value to be chosen
	// for the parameter.
	"query-concurrency": func(v interface{}) interface{} {
		ret := v
		if i, ok := v.(int64); ok && i == 0 {
			ret = 10
		}
		return ret
	},
}

func loadV1Config(configFile string) (*configV1, *map[string]interface{}, error) {
	_, err := os.Stat(configFile)
	if err != nil {
		return nil, nil, fmt.Errorf("1.x config file '%s' does not exist", configFile)
	}

	// load 1.x config content into byte array
	bs, err := load(configFile)
	if err != nil {
		return nil, nil, err
	}

	// parse it into simplified v1 config used as return value
	var configV1 configV1
	_, err = toml.Decode(string(bs), &configV1)
	if err != nil {
		return nil, nil, err
	}

	// parse into a generic config map
	var cAny map[string]interface{}
	_, err = toml.Decode(string(bs), &cAny)
	if err != nil {
		return nil, nil, err
	}

	return &configV1, &cAny, nil
}

func load(path string) ([]byte, error) {
	bs, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// From master-1.x/cmd/influxd/run/config.go:
	// Handle any potential Byte-Order-Marks that may be in the config file.
	// This is for Windows compatibility only.
	// See https://github.com/influxdata/telegraf/issues/1378 and
	// https://github.com/influxdata/influxdb/issues/8965.
	bom := unicode.BOMOverride(transform.Nop)
	bs, _, err = transform.Bytes(bom, bs)

	return bs, err
}

// upgradeConfig upgrades existing 1.x configuration file to 2.x influxdb.toml file.
func upgradeConfig(v1Config map[string]interface{}, targetOptions optionsV2, log *zap.Logger) error {
	// create and initialize helper
	cu := &configUpgrader{
		rules:           configMapRules,
		valueTransforms: configValueTransforms,
		log:             log,
	}

	// rewrite config options from V1 to V2 paths
	cTransformed := cu.transform(v1Config)

	// update new config with upgrade command options
	cu.updateV2Config(cTransformed, targetOptions)

	// write the ugpraded config to disk
	return cu.save(cTransformed, targetOptions.configPath)
}

// configUpgrader is a helper used by `upgrade-config` command.
type configUpgrader struct {
	rules           map[string]string
	valueTransforms map[string]func(interface{}) interface{}
	log             *zap.Logger
}

func (cu *configUpgrader) updateV2Config(config map[string]interface{}, targetOptions optionsV2) {
	if targetOptions.enginePath != "" {
		config["engine-path"] = targetOptions.enginePath
	}
	if targetOptions.boltPath != "" {
		config["bolt-path"] = targetOptions.boltPath
	}
}

func (cu *configUpgrader) save(config map[string]interface{}, path string) error {
	// Open the target file, creating parent directories if needed.
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	outFile, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer outFile.Close()

	// Encode the config directly into the file as TOML.
	return toml.NewEncoder(outFile).Encode(&config)
}

// Credits: @rogpeppe (Roger Peppe)

func (cu *configUpgrader) transform(x map[string]interface{}) map[string]interface{} {
	res := make(map[string]interface{})
	for old, new := range cu.rules {
		val, ok := cu.lookup(x, old)
		if ok {
			if transform, ok := cu.valueTransforms[new]; ok {
				val = transform(val)
			}
			res[new] = val
		}
	}

	return res
}

func (cu *configUpgrader) lookup(x map[string]interface{}, path string) (interface{}, bool) {
	for {
		elem := path
		rest := ""
		if i := strings.Index(path, "."); i != -1 {
			elem, rest = path[0:i], path[i+1:]
		}
		val, ok := x[elem]
		if rest == "" {
			return val, ok
		}
		child, ok := val.(map[string]interface{})
		if !ok {
			return nil, false
		}
		path, x = rest, child
	}
}
