package generate

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type CleanLevel int

const (
	// CleanLevelNone will not remove any data files.
	CleanLevelNone CleanLevel = iota

	// CleanLevelTSM will only remove TSM data files.
	CleanLevelTSM

	// CleanLevelAll will remove all TSM and index data files.
	CleanLevelAll
)

func (i CleanLevel) String() string {
	switch i {
	case CleanLevelNone:
		return "none"
	case CleanLevelTSM:
		return "tsm"
	case CleanLevelAll:
		return "all"
	default:
		return strconv.FormatInt(int64(i), 10)
	}
}

func (i *CleanLevel) Set(v string) error {
	switch v {
	case "none":
		*i = CleanLevelNone
	case "tsm":
		*i = CleanLevelTSM
	case "all":
		*i = CleanLevelAll
	default:
		return fmt.Errorf("invalid clean level %q, must be none, tsm or all", v)
	}
	return nil
}

func (i CleanLevel) Type() string {
	return "clean-level"
}

type StorageSpec struct {
	Organization string
	Bucket       string
	StartTime    string
	EndTime      string
	Clean        CleanLevel
}

func (a *StorageSpec) AddFlags(cmd *cobra.Command, fs *pflag.FlagSet) {
	fs.StringVar(&a.Organization, "org", "", "Name of organization")
	cmd.MarkFlagRequired("org")
	fs.StringVar(&a.Bucket, "bucket", "", "Name of bucket")
	cmd.MarkFlagRequired("bucket")
	start := time.Now().UTC().AddDate(0, 0, -7).Truncate(24 * time.Hour)
	fs.StringVar(&a.StartTime, "start-time", start.Format(time.RFC3339), "Start time")
	fs.StringVar(&a.EndTime, "end-time", start.AddDate(0, 0, 7).Format(time.RFC3339), "End time")
	fs.Var(&a.Clean, "clean", "Clean time series data files (none, tsm or all)")
}

func (a *StorageSpec) Plan() (*StoragePlan, error) {
	plan := &StoragePlan{
		Organization: a.Organization,
		Bucket:       a.Bucket,
		Clean:        a.Clean,
	}

	if a.StartTime != "" {
		if t, err := time.Parse(time.RFC3339, a.StartTime); err != nil {
			return nil, err
		} else {
			plan.StartTime = t.UTC()
		}
	}

	if a.EndTime != "" {
		if t, err := time.Parse(time.RFC3339, a.EndTime); err != nil {
			return nil, err
		} else {
			plan.EndTime = t.UTC()
		}
	}

	return plan, nil
}

type TagCardinalities []int

func (t TagCardinalities) String() string {
	s := make([]string, 0, len(t))
	for i := 0; i < len(t); i++ {
		s = append(s, strconv.Itoa(t[i]))
	}
	return fmt.Sprintf("[%s]", strings.Join(s, ","))
}

func (t TagCardinalities) Cardinality() int {
	n := 1
	for i := range t {
		n *= t[i]
	}
	return n
}

func (t *TagCardinalities) Set(tags string) error {
	*t = (*t)[:0]
	for _, s := range strings.Split(tags, ",") {
		v, err := strconv.Atoi(s)
		if err != nil {
			return fmt.Errorf("cannot parse tag cardinality: %s", s)
		}
		*t = append(*t, v)
	}
	return nil
}

func (t *TagCardinalities) Type() string {
	return "tags"
}

type SchemaSpec struct {
	Tags            TagCardinalities
	PointsPerSeries int
}

func (s *SchemaSpec) AddFlags(cmd *cobra.Command, fs *pflag.FlagSet) {
	s.Tags = []int{10, 10, 10}
	fs.Var(&s.Tags, "t", "Tag cardinality")
	fs.IntVar(&s.PointsPerSeries, "p", 100, "Points per series")
}

func (s *SchemaSpec) Plan(sp *StoragePlan) (*SchemaPlan, error) {
	return &SchemaPlan{
		StoragePlan:     sp,
		Tags:            s.Tags,
		PointsPerSeries: s.PointsPerSeries,
	}, nil
}
