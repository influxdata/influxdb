package plan

import "fmt"

// TODO(nathanielc): Add better options for formatting plans as Graphviz dot format.
type FormatOption func(*formatter)

func Formatted(p PlanReader, opts ...FormatOption) fmt.Formatter {
	f := formatter{
		p: p,
	}
	for _, o := range opts {
		o(&f)
	}
	return f
}

func UseIDs() FormatOption {
	return func(f *formatter) {
		f.useIDs = true
	}
}

type PlanReader interface {
	Do(func(*Procedure))
	lookup(id ProcedureID) *Procedure
}

type formatter struct {
	p      PlanReader
	useIDs bool
}

func (f formatter) Format(fs fmt.State, c rune) {
	if c == 'v' && fs.Flag('#') {
		fmt.Fprintf(fs, "%#v", f.p)
		return
	}
	f.format(fs)
}

func (f formatter) format(fs fmt.State) {
	fmt.Fprint(fs, "digraph PlanSpec {\n")
	f.p.Do(func(pr *Procedure) {
		if f.useIDs {
			fmt.Fprintf(fs, "%s[kind=%q];\n", pr.ID, pr.Spec.Kind())
		} else {
			fmt.Fprintf(fs, "%s[id=%q];\n", pr.Spec.Kind(), pr.ID)
		}
		for _, child := range pr.Children {
			if f.useIDs {
				fmt.Fprintf(fs, "%s->%s;\n", pr.ID, child)
			} else {
				c := f.p.lookup(child)
				if c != nil {
					fmt.Fprintf(fs, "%s->%s;\n", pr.Spec.Kind(), c.Spec.Kind())
				} else {
					fmt.Fprintf(fs, "%s->%s;\n", pr.Spec.Kind(), child)
				}
			}
		}
	})
	fmt.Fprintln(fs, "}")
}
