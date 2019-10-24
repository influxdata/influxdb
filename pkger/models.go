package pkger

import (
	"time"

	"github.com/influxdata/influxdb"
)

const (
	kindUnknown   kind = ""
	kindBucket    kind = "bucket"
	kindLabel     kind = "label"
	kindDashboard kind = "dashboard"
	kindPackage   kind = "package"
)

type kind string

func (k kind) String() string {
	switch k {
	case kindBucket, kindLabel, kindDashboard, kindPackage:
		return string(k)
	default:
		return "unknown"
	}
}

// Metadata is the pkg metadata. This data describes the user
// defined identifiers.
type Metadata struct {
	Description string `yaml:"description" json:"description"`
	Name        string `yaml:"pkgName" json:"pkgName"`
	Version     string `yaml:"pkgVersion" json:"pkgVersion"`
}

// Summary is a definition of all the resources that have or
// will be created from a pkg.
type Summary struct {
	Buckets []struct {
		influxdb.Bucket
		Associations []influxdb.Label
	}

	Labels []struct {
		influxdb.Label
	}
}

type (
	bucket struct {
		ID              influxdb.ID
		OrgID           influxdb.ID
		Description     string
		Name            string
		RetentionPeriod time.Duration
	}

	label struct {
		ID          influxdb.ID
		OrgID       influxdb.ID
		Name        string
		Color       string
		Description string
	}
)
