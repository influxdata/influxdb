package table

import (
	"fmt"
	"strings"

	"github.com/andreyvit/diff"
	"github.com/influxdata/flux"
)

// Diff will perform a diff between two table iterators.
// This will sort the tables within the table iterators and produce
// a diff of the full output.
func Diff(want, got flux.TableIterator, opts ...DiffOption) string {
	if want == nil {
		want = Iterator{}
	}

	var wantS string
	if wantT, err := Sort(want); err != nil {
		wantS = fmt.Sprintf("table error: %s\n", err)
	} else {
		var sb strings.Builder
		if err := wantT.Do(func(table flux.Table) error {
			sb.WriteString(Stringify(table))
			return nil
		}); err != nil {
			_, _ = fmt.Fprintf(&sb, "table error: %s\n", err)
		}
		wantS = sb.String()
	}

	if got == nil {
		got = Iterator{}
	}

	var gotS string
	if gotT, err := Sort(got); err != nil {
		gotS = fmt.Sprintf("table error: %s\n", err)
	} else {
		var sb strings.Builder
		if err := gotT.Do(func(table flux.Table) error {
			sb.WriteString(Stringify(table))
			return nil
		}); err != nil {
			_, _ = fmt.Fprintf(&sb, "table error: %s\n", err)
		}
		gotS = sb.String()
	}

	differ := newDiffer(opts...)
	return differ.diff(wantS, gotS)
}

type differ struct {
	ctx *[2]int
}

func newDiffer(opts ...DiffOption) (d differ) {
	for _, opt := range diffDefaultOptions {
		opt.apply(&d)
	}
	for _, opt := range opts {
		opt.apply(&d)
	}
	return d
}

func (d differ) diff(want, got string) string {
	lines := diff.LineDiffAsLines(want, got)
	if d.ctx == nil {
		return strings.Join(lines, "\n")
	}

	difflines := make([]string, 0, len(lines))
OUTER:
	for {
		for i := 0; i < len(lines); i++ {
			if lines[i][0] == ' ' {
				continue
			}

			// This is the start of a diff section. Store this location.
			start := i - (*d.ctx)[0]
			if start < 0 {
				start = 0
			}

			// Find the end of this section.
			for ; i < len(lines); i++ {
				if lines[i][0] == ' ' {
					break
				}
			}

			// Look n points in the future and, if they are
			// not part of a diff or don't overrun the number
			// of lines, include them.
			stop := i

			for n := (*d.ctx)[1]; n > 0; n-- {
				if stop+1 >= len(lines) || lines[stop+1][0] != ' ' {
					break
				}
				stop++
			}

			difflines = append(difflines, lines[start:stop]...)
			lines = lines[stop:]
			continue OUTER
		}
		return strings.Join(difflines, "\n")
	}
}

type DiffOption interface {
	apply(*differ)
}

type diffOptionFn func(d *differ)

func (opt diffOptionFn) apply(d *differ) {
	opt(d)
}

var diffDefaultOptions = []DiffOption{
	DiffContext(3),
}

func DiffContext(n int) DiffOption {
	return diffOptionFn(func(d *differ) {
		if n < 0 {
			d.ctx = nil
		}
		ctx := [2]int{n, n}
		d.ctx = &ctx
	})
}
