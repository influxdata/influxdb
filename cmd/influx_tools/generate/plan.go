package generate

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/influxdata/influxdb/cmd/influx_tools/internal/errlist"
	"github.com/influxdata/influxdb/cmd/influx_tools/server"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/pkg/errors"
)

type StoragePlan struct {
	Database      string
	Retention     string
	ReplicaN      int
	StartTime     time.Time
	ShardCount    int
	ShardDuration time.Duration
	DatabasePath  string

	info   *meta.DatabaseInfo
	groups []meta.ShardGroupInfo
}

func (p *StoragePlan) String() string {
	sb := new(strings.Builder)
	p.PrintPlan(sb)
	return sb.String()
}

func (p *StoragePlan) PrintPlan(w io.Writer) {
	tw := tabwriter.NewWriter(w, 25, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "Data Path\t%s\n", p.ShardPath())
	fmt.Fprintf(tw, "Shard Count\t%d\n", p.ShardCount)
	fmt.Fprintf(tw, "Database\t%s/%s (Shard duration: %s)\n", p.Database, p.Retention, p.ShardDuration)
	fmt.Fprintf(tw, "Start time\t%s\n", p.StartTime)
	fmt.Fprintf(tw, "End time\t%s\n", p.EndTime())
	tw.Flush()
}

func (p *StoragePlan) ShardPath() string {
	return filepath.Join(p.DatabasePath, p.Retention)
}

// TimeSpan returns the total duration for which the data set.
func (p *StoragePlan) TimeSpan() time.Duration {
	return p.ShardDuration * time.Duration(p.ShardCount)
}

func (p *StoragePlan) EndTime() time.Time {
	return p.StartTime.Add(p.TimeSpan())
}

func (p *StoragePlan) InitMetadata(client server.MetaClient) (err error) {
	if err = client.DropDatabase(p.Database); err != nil {
		return err
	}

	rp := meta.RetentionPolicySpec{
		Name:               p.Retention,
		ShardGroupDuration: p.ShardDuration,
		ReplicaN:           &p.ReplicaN,
	}
	info, err := client.CreateDatabaseWithRetentionPolicy(p.Database, &rp)
	if err != nil {
		return err
	}

	return p.createShardGroupMetadata(client, info.DefaultRetentionPolicy)
}

// InitFileSystem initializes the file system structure, cleaning up
// existing files and re-creating the appropriate shard directories.
func (p *StoragePlan) InitFileSystem(client server.MetaClient) error {
	var err error
	if err = os.RemoveAll(p.DatabasePath); err != nil {
		return err
	}

	minT, maxT := p.TimeRange()

	groups, err := client.NodeShardGroupsByTimeRange(p.Database, p.Retention, minT, maxT)
	if err != nil {
		return err
	}

	p.groups = groups

	for i := 0; i < len(groups); i++ {
		sgi := &groups[i]
		if len(sgi.Shards) > 1 {
			return fmt.Errorf("multiple shards for the same owner %v", sgi.Shards[0].Owners)
		}

		if err = os.MkdirAll(filepath.Join(p.ShardPath(), strconv.Itoa(int(sgi.Shards[0].ID))), 0777); err != nil {
			return err
		}
	}

	p.info = client.Database(p.Database)

	return nil
}

// NodeShardGroups returns ShardGroupInfo with Shards limited to the current node
func (p *StoragePlan) NodeShardGroups() []meta.ShardGroupInfo {
	return p.groups
}

func (p *StoragePlan) ShardGroups() []meta.ShardGroupInfo {
	return p.info.RetentionPolicy(p.info.DefaultRetentionPolicy).ShardGroups
}

func (p *StoragePlan) createShardGroupMetadata(client server.MetaClient, rp string) error {
	ts := p.StartTime.Truncate(p.ShardDuration).UTC()

	var err error
	groups := make([]*meta.ShardGroupInfo, p.ShardCount)
	for i := 0; i < p.ShardCount; i++ {
		groups[i], err = client.CreateShardGroup(p.Database, rp, ts)
		if err != nil {
			return err
		}
		ts = ts.Add(p.ShardDuration)
	}

	return nil
}

func (p *StoragePlan) TimeRange() (start, end time.Time) {
	start = p.StartTime.Truncate(p.ShardDuration).UTC()
	end = start.Add(time.Duration(p.ShardDuration.Nanoseconds() * int64(p.ShardCount)))
	return start, end
}

func (p *StoragePlan) Validate() error {
	// build default values
	def := &planDefaults{}
	WalkPlan(def, p)

	// validate
	val := &planValidator{}
	WalkPlan(val, p)
	return val.Err()
}

type Visitor interface {
	Visit(node Node) Visitor
}

type Node interface{ node() }

func (*StoragePlan) node() {}

func WalkPlan(v Visitor, node Node) {
	if v = v.Visit(node); v == nil {
		return
	}

	switch n := node.(type) {
	case *StoragePlan:

	default:
		panic(fmt.Sprintf("WalkConfig: unexpected node type %T", n))
	}
}

type planValidator struct {
	errs errlist.ErrorList
}

func (v *planValidator) Visit(node Node) Visitor {
	switch n := node.(type) {
	case *StoragePlan:
		if n.DatabasePath == "" {
			v.errs.Add(errors.New("missing DataPath"))
		}

		if n.StartTime.Add(n.TimeSpan()).After(time.Now()) {
			v.errs.Add(fmt.Errorf("start time must be ≤ %s", time.Now().Truncate(n.ShardDuration).UTC().Add(-n.TimeSpan())))
		}
	}

	return v
}
func (v *planValidator) Err() error {
	return v.errs.Err()
}

type planDefaults struct{}

func (v *planDefaults) Visit(node Node) Visitor {
	switch n := node.(type) {
	case *StoragePlan:
		if n.DatabasePath == "" {
			n.DatabasePath = "${HOME}/.influxdb/data"
		}
		if n.Database == "" {
			n.Database = "db"
		}
		if n.Retention == "" {
			n.Retention = "autogen"
		}
		if n.ShardDuration == 0 {
			n.ShardDuration = 24 * time.Hour
		}
		if n.ShardCount == 0 {
			n.ShardCount = 1
		}
		if n.StartTime.IsZero() {
			n.StartTime = time.Now().Truncate(n.ShardDuration).Add(-n.TimeSpan())
		}
	}

	return v
}

type SchemaPlan struct {
	StoragePlan             *StoragePlan
	Tags                    TagCardinalities
	PointsPerSeriesPerShard int
}

func (p *SchemaPlan) String() string {
	sb := new(strings.Builder)
	p.PrintPlan(sb)
	return sb.String()
}

func (p *SchemaPlan) PrintPlan(w io.Writer) {
	tw := tabwriter.NewWriter(w, 25, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "Tag cardinalities\t%s\n", p.Tags)
	fmt.Fprintf(tw, "Points per series per shard\t%d\n", p.PointsPerSeriesPerShard)
	fmt.Fprintf(tw, "Total points per shard\t%d\n", p.Tags.Cardinality()*p.PointsPerSeriesPerShard)
	fmt.Fprintf(tw, "Total series\t%d\n", p.Tags.Cardinality())
	fmt.Fprintf(tw, "Total points\t%d\n", p.Tags.Cardinality()*p.StoragePlan.ShardCount*p.PointsPerSeriesPerShard)
	_ = tw.Flush()
}
