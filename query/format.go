package query

import (
	"encoding/json"
	"fmt"
)

// TODO(nathanielc): Add better options for formatting plans as Graphviz dot format.
type FormatOption func(*formatter)

func Formatted(q *Spec, opts ...FormatOption) fmt.Formatter {
	f := formatter{
		q: q,
	}
	for _, o := range opts {
		o(&f)
	}
	return f
}

func FmtJSON(f *formatter) { f.json = true }

type formatter struct {
	q    *Spec
	json bool
}

func (f formatter) Format(fs fmt.State, c rune) {
	if c == 'v' && fs.Flag('#') {
		fmt.Fprintf(fs, "%#v", f.q)
		return
	}
	if f.json {
		f.formatJSON(fs)
	} else {
		f.formatDAG(fs)
	}
}
func (f formatter) formatJSON(fs fmt.State) {
	e := json.NewEncoder(fs)
	e.SetIndent("", "  ")
	e.Encode(f.q)
}

func (f formatter) formatDAG(fs fmt.State) {
	fmt.Fprint(fs, "digraph QuerySpec {\n")
	_ = f.q.Walk(func(o *Operation) error {
		fmt.Fprintf(fs, "%s[kind=%q];\n", o.ID, o.Spec.Kind())
		for _, child := range f.q.Children(o.ID) {
			fmt.Fprintf(fs, "%s->%s;\n", o.ID, child.ID)
		}
		return nil
	})
	fmt.Fprintln(fs, "}")
}
