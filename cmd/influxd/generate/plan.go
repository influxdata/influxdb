package generate

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"
)

type StoragePlan struct {
	Organization string
	Bucket       string
	StartTime    time.Time
	EndTime      time.Time
	Clean        CleanLevel
	Path         string
}

func (p *StoragePlan) String() string {
	sb := new(strings.Builder)
	p.PrintPlan(sb)
	return sb.String()
}

func (p *StoragePlan) PrintPlan(w io.Writer) {
	tw := tabwriter.NewWriter(w, 25, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "Organization\t%s\n", p.Organization)
	fmt.Fprintf(tw, "Bucket\t%s\n", p.Bucket)
	fmt.Fprintf(tw, "Start time\t%s\n", p.StartTime)
	fmt.Fprintf(tw, "End time\t%s\n", p.EndTime)
	fmt.Fprintf(tw, "Clean data\t%s\n", p.Clean)
	tw.Flush()
}

// TimeSpan returns the total duration for which the data set.
func (p *StoragePlan) TimeSpan() time.Duration {
	return p.EndTime.Sub(p.StartTime)
}

type SchemaPlan struct {
	StoragePlan     *StoragePlan
	Tags            TagCardinalities
	PointsPerSeries int
}

func (p *SchemaPlan) String() string {
	sb := new(strings.Builder)
	p.PrintPlan(sb)
	return sb.String()
}

func (p *SchemaPlan) PrintPlan(w io.Writer) {
	tw := tabwriter.NewWriter(w, 25, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "Tag cardinalities\t%s\n", p.Tags)
	fmt.Fprintf(tw, "Points per series\t%d\n", p.PointsPerSeries)
	fmt.Fprintf(tw, "Total points\t%d\n", p.Tags.Cardinality()*p.PointsPerSeries)
	fmt.Fprintf(tw, "Total series\t%d\n", p.Tags.Cardinality())
	_ = tw.Flush()
}
