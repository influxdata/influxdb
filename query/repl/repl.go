package repl

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"

	"io/ioutil"

	"path/filepath"

	prompt "github.com/c-bata/go-prompt"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/control"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/interpreter"
	"github.com/influxdata/platform/query/parser"
	"github.com/influxdata/platform/query/semantic"
	"github.com/influxdata/platform/query/values"
	"github.com/pkg/errors"
)

type REPL struct {
	orgID platform.ID

	scope        *interpreter.Scope
	declarations semantic.DeclarationScope
	c            *control.Controller

	cancelMu   sync.Mutex
	cancelFunc context.CancelFunc
}

func addBuiltIn(script string, scope *interpreter.Scope, declarations semantic.DeclarationScope) error {
	astProg, err := parser.NewAST(script)
	if err != nil {
		return errors.Wrap(err, "failed to parse builtin")
	}
	semProg, err := semantic.New(astProg, declarations)
	if err != nil {
		return errors.Wrap(err, "failed to create semantic graph for builtin")
	}

	if _, err := interpreter.Eval(semProg, scope); err != nil {
		return errors.Wrap(err, "failed to evaluate builtin")
	}
	return nil
}

func New(c *control.Controller, orgID platform.ID) *REPL {
	scope, declarations := query.BuiltIns()
	interpScope := interpreter.NewScopeWithValues(scope)
	addBuiltIn("run = () => yield(table:_)", interpScope, declarations)
	return &REPL{
		orgID:        orgID,
		scope:        interpScope,
		declarations: declarations,
		c:            c,
	}
}

func (r *REPL) Run() {
	p := prompt.New(
		r.input,
		r.completer,
		prompt.OptionPrefix("> "),
		prompt.OptionTitle("flux"),
	)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT)
	go func() {
		for range sigs {
			r.cancel()
		}
	}()
	p.Run()
}

func (r *REPL) cancel() {
	r.cancelMu.Lock()
	defer r.cancelMu.Unlock()
	if r.cancelFunc != nil {
		r.cancelFunc()
		r.cancelFunc = nil
	}
}

func (r *REPL) setCancel(cf context.CancelFunc) {
	r.cancelMu.Lock()
	defer r.cancelMu.Unlock()
	r.cancelFunc = cf
}
func (r *REPL) clearCancel() {
	r.setCancel(nil)
}

func (r *REPL) completer(d prompt.Document) []prompt.Suggest {
	names := r.scope.Names()
	sort.Strings(names)

	s := make([]prompt.Suggest, 0, len(names))
	for _, n := range names {
		if n == "_" || !strings.HasPrefix(n, "_") {
			s = append(s, prompt.Suggest{Text: n})
		}
	}
	if d.Text == "" || strings.HasPrefix(d.Text, "@") {
		root := "./" + strings.TrimPrefix(d.Text, "@")
		fluxFiles, err := getFluxFiles(root)
		if err == nil {
			for _, fName := range fluxFiles {
				s = append(s, prompt.Suggest{Text: "@" + fName})
			}
		}
		dirs, err := getDirs(root)
		if err == nil {
			for _, fName := range dirs {
				s = append(s, prompt.Suggest{Text: "@" + fName + string(os.PathSeparator)})
			}
		}
	}

	return prompt.FilterHasPrefix(s, d.GetWordBeforeCursor(), true)
}

func (r *REPL) Input(t string) error {
	_, err := r.executeLine(t, false)
	return err
}

// input processes a line of input and prints the result.
func (r *REPL) input(t string) {
	v, err := r.executeLine(t, true)
	if err != nil {
		fmt.Println("Error:", err)
	} else if v != nil {
		fmt.Println(v)
	}
}

// executeLine processes a line of input.
// If the input evaluates to a valid value, that value is returned.
func (r *REPL) executeLine(t string, expectYield bool) (values.Value, error) {
	if t == "" {
		return nil, nil
	}

	if t[0] == '@' {
		q, err := LoadQuery(t)
		if err != nil {
			return nil, err
		}
		t = q
	}

	astProg, err := parser.NewAST(t)
	if err != nil {
		return nil, err
	}

	semProg, err := semantic.New(astProg, r.declarations)
	if err != nil {
		return nil, err
	}

	if _, err := interpreter.Eval(semProg, r.scope); err != nil {
		return nil, err
	}

	v := r.scope.Return()

	// Check for yield and execute query
	if v.Type() == query.TableObjectType {
		t := v.(query.TableObject)
		if !expectYield || (expectYield && t.Kind == functions.YieldKind) {
			spec := t.ToSpec()
			// Do query
			return nil, r.doQuery(spec)
		}
	}

	r.scope.Set("_", v)

	// Print value
	if v.Type() != semantic.Invalid {
		return v, nil
	}
	return nil, nil
}

func (r *REPL) doQuery(spec *query.Spec) error {
	// Setup cancel context
	ctx, cancelFunc := context.WithCancel(context.Background())
	r.setCancel(cancelFunc)
	defer cancelFunc()
	defer r.clearCancel()

	q, err := r.c.Query(ctx, r.orgID, spec)
	if err != nil {
		return err
	}
	defer q.Done()

	results, ok := <-q.Ready()
	if !ok {
		err := q.Err()
		return err
	}

	names := make([]string, 0, len(results))
	for name := range results {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		r := results[name]
		blocks := r.Blocks()
		fmt.Println("Result:", name)
		err := blocks.Do(func(b query.Block) error {
			_, err := execute.NewFormatter(b, nil).WriteTo(os.Stdout)
			return err
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func getFluxFiles(path string) ([]string, error) {
	return filepath.Glob(path + "*.flux")
}

func getDirs(path string) ([]string, error) {
	dir := filepath.Dir(path)
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	dirs := make([]string, 0, len(files))
	for _, f := range files {
		if f.IsDir() {
			dirs = append(dirs, filepath.Join(dir, f.Name()))
		}
	}
	return dirs, nil
}

// LoadQuery returns the Flux query q, except for two special cases:
// if q is exactly "-", the query will be read from stdin;
// and if the first character of q is "@",
// the @ prefix is removed and the contents of the file specified by the rest of q are returned.
func LoadQuery(q string) (string, error) {
	if q == "-" {
		data, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			return "", err
		}
		return string(data), nil
	}

	if len(q) > 0 && q[0] == '@' {
		data, err := ioutil.ReadFile(q[1:])
		if err != nil {
			return "", err
		}

		return string(data), nil
	}

	return q, nil
}
