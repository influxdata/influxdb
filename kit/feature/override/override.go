package override

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/influxdata/influxdb/v2/kit/feature"
)

// Flagger can override default flag values.
type Flagger struct {
	overrides map[string]string
	byKey     feature.ByKeyFn
}

// Make a Flagger that returns defaults with any overrides parsed from the string.
func Make(overrides map[string]string, byKey feature.ByKeyFn) (Flagger, error) {
	if byKey == nil {
		byKey = feature.ByKey
	}

	// Check all provided override keys correspond to an existing Flag.
	var missing []string
	for k := range overrides {
		if _, found := byKey(k); !found {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return Flagger{}, fmt.Errorf("configured overrides for non-existent flags: %s", strings.Join(missing, ","))
	}

	return Flagger{
		overrides: overrides,
		byKey:     byKey,
	}, nil
}

// Flags returns a map of default values with overrides applied. It never returns an error.
func (f Flagger) Flags(_ context.Context, flags ...feature.Flag) (map[string]interface{}, error) {
	if len(flags) == 0 {
		flags = feature.Flags()
	}

	m := make(map[string]interface{}, len(flags))
	for _, flag := range flags {
		if s, overridden := f.overrides[flag.Key()]; overridden {
			iface, err := f.coerce(s, flag)
			if err != nil {
				return nil, err
			}
			m[flag.Key()] = iface
		} else {
			m[flag.Key()] = flag.Default()
		}
	}

	return m, nil
}

func (f Flagger) coerce(s string, flag feature.Flag) (iface interface{}, err error) {
	if base, ok := flag.(feature.Base); ok {
		flag, _ = f.byKey(base.Key())
	}

	switch flag.Default().(type) {
	case bool:
		iface, err = strconv.ParseBool(s)
	case int32:
		iface, err = strconv.Atoi(s)
	case float64:
		iface, err = strconv.ParseFloat(s, 64)
	default:
		iface = s
	}

	if err != nil {
		return nil, fmt.Errorf("coercing string %q based on flag type %T: %v", s, flag, err)
	}
	return
}
