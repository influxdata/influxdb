package functions

import (
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/compiler"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/interpreter"
	"github.com/influxdata/platform/query/plan"
	"github.com/influxdata/platform/query/semantic"
	"github.com/influxdata/platform/query/values"
	"github.com/pkg/errors"
)

const JoinKind = "join"
const MergeJoinKind = "merge-join"

type JoinOpSpec struct {
	// On is a list of tags on which to join.
	On []string `json:"on"`
	// Fn is a function accepting a single parameter.
	// The parameter is map if records for each of the parent operations.
	Fn *semantic.FunctionExpression `json:"fn"`
	// TableNames are the names to give to each parent when populating the parameter for the function.
	// The first parent is referenced by the first name and so forth.
	// TODO(nathanielc): Change this to a map of parent operation IDs to names.
	// Then make it possible for the transformation to map operation IDs to parent IDs.
	TableNames map[query.OperationID]string `json:"table_names"`
	// tableNames maps each TableObject being joined to the parameter that holds it.
	tableNames map[*query.TableObject]string
}

type params struct {
	vars []string
	vals []*query.TableObject
}

type joinParams params

func newJoinParams(capacity int) *joinParams {
	params := &joinParams{
		vars: make([]string, 0, capacity),
		vals: make([]*query.TableObject, 0, capacity),
	}
	return params
}

func (params *joinParams) add(newVar string, newVal *query.TableObject) {
	params.vars = append(params.vars, newVar)
	params.vals = append(params.vals, newVal)
}

// joinParams implements the Sort interface in order
// to build the query spec in a consistent manner.
func (params *joinParams) Len() int {
	return len(params.vals)
}
func (params *joinParams) Swap(i, j int) {
	params.vars[i], params.vars[j] = params.vars[j], params.vars[i]
	params.vals[i], params.vals[j] = params.vals[j], params.vals[i]
}
func (params *joinParams) Less(i, j int) bool {
	return params.vars[i] < params.vars[j]
}

var joinSignature = semantic.FunctionSignature{
	Params: map[string]semantic.Type{
		"tables": semantic.Object,
		"fn":     semantic.Function,
		"on":     semantic.NewArrayType(semantic.String),
	},
	ReturnType:   query.TableObjectType,
	PipeArgument: "tables",
}

func init() {
	query.RegisterFunction(JoinKind, createJoinOpSpec, joinSignature)
	query.RegisterOpSpec(JoinKind, newJoinOp)
	//TODO(nathanielc): Allow for other types of join implementations
	plan.RegisterProcedureSpec(MergeJoinKind, newMergeJoinProcedure, JoinKind)
	execute.RegisterTransformation(MergeJoinKind, createMergeJoinTransformation)
}

func createJoinOpSpec(args query.Arguments, a *query.Administration) (query.OperationSpec, error) {
	f, err := args.GetRequiredFunction("fn")
	if err != nil {
		return nil, err
	}
	fn, err := interpreter.ResolveFunction(f)
	if err != nil {
		return nil, err
	}
	spec := &JoinOpSpec{
		Fn:         fn,
		TableNames: make(map[query.OperationID]string),
		tableNames: make(map[*query.TableObject]string),
	}

	if array, ok, err := args.GetArray("on", semantic.String); err != nil {
		return nil, err
	} else if ok {
		spec.On, err = interpreter.ToStringArray(array)
		if err != nil {
			return nil, err
		}
	}

	if m, ok, err := args.GetObject("tables"); err != nil {
		return nil, err
	} else if ok {
		var err error
		joinParams := newJoinParams(m.Len())
		m.Range(func(k string, t values.Value) {
			if err != nil {
				return
			}
			if t.Type().Kind() != semantic.Object {
				err = fmt.Errorf("value for key %q in tables must be an object: got %v", k, t.Type().Kind())
				return
			}
			if t.Type() != query.TableObjectType {
				err = fmt.Errorf("value for key %q in tables must be an table object: got %v", k, t.Type())
				return
			}
			p := t.(*query.TableObject)
			joinParams.add(k /*parameter*/, p /*argument*/)
			spec.tableNames[p] = k
		})
		if err != nil {
			return nil, err
		}
		// Add parents in a consistent manner by sorting
		// based on their corresponding function parameter.
		sort.Sort(joinParams)
		for _, p := range joinParams.vals {
			a.AddParent(p)
		}
	}

	return spec, nil
}

func (t *JoinOpSpec) IDer(ider query.IDer) {
	for p, k := range t.tableNames {
		t.TableNames[ider.ID(p)] = k
	}
}

func newJoinOp() query.OperationSpec {
	return new(JoinOpSpec)
}

func (s *JoinOpSpec) Kind() query.OperationKind {
	return JoinKind
}

type MergeJoinProcedureSpec struct {
	On         []string                     `json:"keys"`
	Fn         *semantic.FunctionExpression `json:"f"`
	TableNames map[plan.ProcedureID]string  `json:"table_names"`
}

func newMergeJoinProcedure(qs query.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*JoinOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}

	tableNames := make(map[plan.ProcedureID]string, len(spec.TableNames))
	for qid, name := range spec.TableNames {
		pid := pa.ConvertID(qid)
		tableNames[pid] = name
	}

	p := &MergeJoinProcedureSpec{
		On:         spec.On,
		Fn:         spec.Fn,
		TableNames: tableNames,
	}
	sort.Strings(p.On)
	return p, nil
}

func (s *MergeJoinProcedureSpec) Kind() plan.ProcedureKind {
	return MergeJoinKind
}
func (s *MergeJoinProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(MergeJoinProcedureSpec)

	ns.On = make([]string, len(s.On))
	copy(ns.On, s.On)

	ns.Fn = s.Fn.Copy().(*semantic.FunctionExpression)

	return ns
}

func (s *MergeJoinProcedureSpec) ParentChanged(old, new plan.ProcedureID) {
	if v, ok := s.TableNames[old]; ok {
		delete(s.TableNames, old)
		s.TableNames[new] = v
	}
}

func createMergeJoinTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*MergeJoinProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	parents := a.Parents()
	if len(parents) != 2 {
		//TODO(nathanielc): Support n-way joins
		return nil, nil, errors.New("joins currently must only have two parents")
	}

	tableNames := make(map[execute.DatasetID]string, len(s.TableNames))
	for pid, name := range s.TableNames {
		id := a.ConvertID(pid)
		tableNames[id] = name
	}
	leftName := tableNames[parents[0]]
	rightName := tableNames[parents[1]]

	joinFn, err := NewRowJoinFunction(s.Fn, parents, tableNames)
	if err != nil {
		return nil, nil, errors.Wrap(err, "invalid expression")
	}
	cache := NewMergeJoinCache(joinFn, a.Allocator(), leftName, rightName, s.On)
	d := execute.NewDataset(id, mode, cache)
	t := NewMergeJoinTransformation(d, cache, s, parents, tableNames)
	return t, d, nil
}

type mergeJoinTransformation struct {
	parents []execute.DatasetID

	mu sync.Mutex

	d     execute.Dataset
	cache MergeJoinCache

	leftID, rightID     execute.DatasetID
	leftName, rightName string

	parentState map[execute.DatasetID]*mergeJoinParentState

	keys []string
}

func NewMergeJoinTransformation(d execute.Dataset, cache MergeJoinCache, spec *MergeJoinProcedureSpec, parents []execute.DatasetID, tableNames map[execute.DatasetID]string) *mergeJoinTransformation {
	t := &mergeJoinTransformation{
		d:         d,
		cache:     cache,
		keys:      spec.On,
		leftID:    parents[0],
		rightID:   parents[1],
		leftName:  tableNames[parents[0]],
		rightName: tableNames[parents[1]],
	}
	t.parentState = make(map[execute.DatasetID]*mergeJoinParentState)
	for _, id := range parents {
		t.parentState[id] = new(mergeJoinParentState)
	}
	return t
}

type mergeJoinParentState struct {
	mark       execute.Time
	processing execute.Time
	finished   bool
}

func (t *mergeJoinTransformation) RetractTable(id execute.DatasetID, key query.GroupKey) error {
	panic("not implemented")
}

func (t *mergeJoinTransformation) Process(id execute.DatasetID, tbl query.Table) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	tables := t.cache.Tables(tbl.Key())

	var references []string
	var table execute.TableBuilder
	switch id {
	case t.leftID:
		table = tables.left
		references = tables.joinFn.references[t.leftName]
	case t.rightID:
		table = tables.right
		references = tables.joinFn.references[t.rightName]
	}

	// Add columns to table
	labels := unionStrs(t.keys, references)
	colMap := make([]int, len(labels))
	for _, label := range labels {
		tableIdx := execute.ColIdx(label, tbl.Cols())
		if tableIdx < 0 {
			return fmt.Errorf("no column %q exists", label)
		}
		// Only add the column if it does not already exist
		builderIdx := execute.ColIdx(label, table.Cols())
		if builderIdx < 0 {
			c := tbl.Cols()[tableIdx]
			builderIdx = table.AddCol(c)
		}
		colMap[builderIdx] = tableIdx
	}

	execute.AppendTable(tbl, table, colMap)
	return nil
}

func unionStrs(as, bs []string) []string {
	u := make([]string, len(bs), len(as)+len(bs))
	copy(u, bs)
	for _, a := range as {
		found := false
		for _, b := range bs {
			if a == b {
				found = true
				break
			}
		}
		if !found {
			u = append(u, a)
		}
	}
	return u
}

func (t *mergeJoinTransformation) UpdateWatermark(id execute.DatasetID, mark execute.Time) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.parentState[id].mark = mark

	min := execute.Time(math.MaxInt64)
	for _, state := range t.parentState {
		if state.mark < min {
			min = state.mark
		}
	}

	return t.d.UpdateWatermark(min)
}

func (t *mergeJoinTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.parentState[id].processing = pt

	min := execute.Time(math.MaxInt64)
	for _, state := range t.parentState {
		if state.processing < min {
			min = state.processing
		}
	}

	return t.d.UpdateProcessingTime(min)
}

func (t *mergeJoinTransformation) Finish(id execute.DatasetID, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err != nil {
		t.d.Finish(err)
	}

	t.parentState[id].finished = true
	finished := true
	for _, state := range t.parentState {
		finished = finished && state.finished
	}

	if finished {
		t.d.Finish(nil)
	}
}

type MergeJoinCache interface {
	Tables(query.GroupKey) *joinTables
}

type mergeJoinCache struct {
	data  *execute.GroupLookup
	alloc *execute.Allocator

	keys []string
	on   map[string]bool

	leftName, rightName string

	triggerSpec query.TriggerSpec

	joinFn *joinFunc
}

func NewMergeJoinCache(joinFn *joinFunc, a *execute.Allocator, leftName, rightName string, keys []string) *mergeJoinCache {
	on := make(map[string]bool, len(keys))
	for _, k := range keys {
		on[k] = true
	}
	return &mergeJoinCache{
		data:      execute.NewGroupLookup(),
		keys:      keys,
		on:        on,
		joinFn:    joinFn,
		alloc:     a,
		leftName:  leftName,
		rightName: rightName,
	}
}

func (c *mergeJoinCache) Table(key query.GroupKey) (query.Table, error) {
	t, ok := c.lookup(key)
	if !ok {
		return nil, errors.New("table not found")
	}
	return t.Join()
}

func (c *mergeJoinCache) ForEach(f func(query.GroupKey)) {
	c.data.Range(func(key query.GroupKey, value interface{}) {
		f(key)
	})
}

func (c *mergeJoinCache) ForEachWithContext(f func(query.GroupKey, execute.Trigger, execute.TableContext)) {
	c.data.Range(func(key query.GroupKey, value interface{}) {
		tables := value.(*joinTables)
		bc := execute.TableContext{
			Key:   key,
			Count: tables.Size(),
		}
		f(key, tables.trigger, bc)
	})
}

func (c *mergeJoinCache) DiscardTable(key query.GroupKey) {
	t, ok := c.lookup(key)
	if ok {
		t.ClearData()
	}
}

func (c *mergeJoinCache) ExpireTable(key query.GroupKey) {
	v, ok := c.data.Delete(key)
	if ok {
		v.(*joinTables).ClearData()
	}
}

func (c *mergeJoinCache) SetTriggerSpec(spec query.TriggerSpec) {
	c.triggerSpec = spec
}

func (c *mergeJoinCache) lookup(key query.GroupKey) (*joinTables, bool) {
	v, ok := c.data.Lookup(key)
	if !ok {
		return nil, false
	}
	return v.(*joinTables), true
}

func (c *mergeJoinCache) Tables(key query.GroupKey) *joinTables {
	tables, ok := c.lookup(key)
	if !ok {
		tables = &joinTables{
			keys:      c.keys,
			key:       key,
			on:        c.on,
			alloc:     c.alloc,
			left:      execute.NewColListTableBuilder(key, c.alloc),
			right:     execute.NewColListTableBuilder(key, c.alloc),
			leftName:  c.leftName,
			rightName: c.rightName,
			trigger:   execute.NewTriggerFromSpec(c.triggerSpec),
			joinFn:    c.joinFn,
		}
		c.data.Set(key, tables)
	}
	return tables
}

type joinTables struct {
	keys []string
	on   map[string]bool
	key  query.GroupKey

	alloc *execute.Allocator

	left, right         *execute.ColListTableBuilder
	leftName, rightName string

	trigger execute.Trigger

	joinFn *joinFunc
}

func (t *joinTables) Size() int {
	return t.left.NRows() + t.right.NRows()
}

func (t *joinTables) ClearData() {
	t.left = execute.NewColListTableBuilder(t.key, t.alloc)
	t.right = execute.NewColListTableBuilder(t.key, t.alloc)
}

// Join performs a sort-merge join
func (t *joinTables) Join() (query.Table, error) {
	// First prepare the join function
	left := t.left.RawTable()
	right := t.right.RawTable()
	err := t.joinFn.Prepare(map[string]*execute.ColListTable{
		t.leftName:  left,
		t.rightName: right,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to prepare join function")
	}
	// Create a builder for the result of the join
	builder := execute.NewColListTableBuilder(t.key, t.alloc)

	// Add columns from function in sorted order
	properties := t.joinFn.Type().Properties()
	keys := make([]string, 0, len(properties))
	for k := range properties {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		builder.AddCol(query.ColMeta{
			Label: k,
			Type:  execute.ConvertFromKind(properties[k].Kind()),
		})
	}

	// Now that all columns have been added, keep a reference.
	bCols := builder.Cols()

	// Determine sort order for the joining tables
	sortOrder := make([]string, len(t.keys))
	for i, label := range t.keys {
		sortOrder[i] = label
	}
	// Sort input tables
	t.left.Sort(sortOrder, false)
	t.right.Sort(sortOrder, false)

	var (
		leftSet, rightSet subset
		leftKey, rightKey query.GroupKey
	)

	rows := map[string]int{
		t.leftName:  -1,
		t.rightName: -1,
	}

	leftSet, leftKey = t.advance(leftSet.Stop, left)
	rightSet, rightKey = t.advance(rightSet.Stop, right)
	for !leftSet.Empty() && !rightSet.Empty() {
		if leftKey.Equal(rightKey) {
			// Inner join
			for l := leftSet.Start; l < leftSet.Stop; l++ {
				for r := rightSet.Start; r < rightSet.Stop; r++ {
					// Evaluate expression and add to table
					rows[t.leftName] = l
					rows[t.rightName] = r
					m, err := t.joinFn.Eval(rows)
					if err != nil {
						return nil, errors.Wrap(err, "failed to evaluate join function")
					}
					for j, c := range bCols {
						v, _ := m.Get(c.Label)
						execute.AppendValue(builder, j, v)
					}
				}
			}
			leftSet, leftKey = t.advance(leftSet.Stop, left)
			rightSet, rightKey = t.advance(rightSet.Stop, right)
		} else if leftKey.Less(rightKey) {
			leftSet, leftKey = t.advance(leftSet.Stop, left)
		} else {
			rightSet, rightKey = t.advance(rightSet.Stop, right)
		}
	}
	return builder.Table()
}

func (t *joinTables) advance(offset int, table *execute.ColListTable) (subset, query.GroupKey) {
	if n := table.NRows(); n == offset {
		return subset{Start: n, Stop: n}, nil
	}
	start := offset
	key := execute.GroupKeyForRowOn(start, table, t.on)
	s := subset{Start: start}
	offset++
	for offset < table.NRows() && equalRowKeys(start, offset, table, t.on) {
		offset++
	}
	s.Stop = offset
	return s, key
}

type subset struct {
	Start int
	Stop  int
}

func (s subset) Empty() bool {
	return s.Start == s.Stop
}

func equalRowKeys(x, y int, table *execute.ColListTable, on map[string]bool) bool {
	for j, c := range table.Cols() {
		if !on[c.Label] {
			continue
		}
		switch c.Type {
		case query.TBool:
			if xv, yv := table.Bools(j)[x], table.Bools(j)[y]; xv != yv {
				return false
			}
		case query.TInt:
			if xv, yv := table.Ints(j)[x], table.Ints(j)[y]; xv != yv {
				return false
			}
		case query.TUInt:
			if xv, yv := table.UInts(j)[x], table.UInts(j)[y]; xv != yv {
				return false
			}
		case query.TFloat:
			if xv, yv := table.Floats(j)[x], table.Floats(j)[y]; xv != yv {
				return false
			}
		case query.TString:
			if xv, yv := table.Strings(j)[x], table.Strings(j)[y]; xv != yv {
				return false
			}
		case query.TTime:
			if xv, yv := table.Times(j)[x], table.Times(j)[y]; xv != yv {
				return false
			}
		default:
			execute.PanicUnknownType(c.Type)
		}
	}
	return true
}

type joinFunc struct {
	fn               *semantic.FunctionExpression
	compilationCache *compiler.CompilationCache
	scope            compiler.Scope

	preparedFn compiler.Func

	recordName string
	record     *execute.Record

	recordCols map[tableCol]int
	references map[string][]string

	isWrap  bool
	wrapObj *execute.Record

	tableData map[string]*execute.ColListTable
}

type tableCol struct {
	table, col string
}

func NewRowJoinFunction(fn *semantic.FunctionExpression, parentIDs []execute.DatasetID, tableNames map[execute.DatasetID]string) (*joinFunc, error) {
	if len(fn.Params) != 1 {
		return nil, errors.New("join function should only have one parameter for the map of tables")
	}
	scope, decls := query.BuiltIns()
	return &joinFunc{
		compilationCache: compiler.NewCompilationCache(fn, scope, decls),
		scope:            make(compiler.Scope, 1),
		references:       findTableReferences(fn),
		recordCols:       make(map[tableCol]int),
		recordName:       fn.Params[0].Key.Name,
	}, nil
}

func (f *joinFunc) Prepare(tables map[string]*execute.ColListTable) error {
	f.tableData = tables
	propertyTypes := make(map[string]semantic.Type, len(f.references))
	// Prepare types and recordcols
	for tbl, b := range tables {
		cols := b.Cols()
		tblPropertyTypes := make(map[string]semantic.Type, len(f.references[tbl]))
		for _, r := range f.references[tbl] {
			j := execute.ColIdx(r, cols)
			if j < 0 {
				return fmt.Errorf("function references unknown column %q of table %q", r, tbl)
			}
			c := cols[j]
			f.recordCols[tableCol{table: tbl, col: c.Label}] = j
			tblPropertyTypes[r] = execute.ConvertToKind(c.Type)
		}
		propertyTypes[tbl] = semantic.NewObjectType(tblPropertyTypes)
	}
	f.record = execute.NewRecord(semantic.NewObjectType(propertyTypes))
	for tbl := range tables {
		f.record.Set(tbl, execute.NewRecord(propertyTypes[tbl]))
	}
	// Compile fn for given types
	fn, err := f.compilationCache.Compile(map[string]semantic.Type{
		f.recordName: f.record.Type(),
	})
	if err != nil {
		return err
	}
	f.preparedFn = fn

	k := f.preparedFn.Type().Kind()
	f.isWrap = k != semantic.Object
	if f.isWrap {
		f.wrapObj = execute.NewRecord(semantic.NewObjectType(map[string]semantic.Type{
			execute.DefaultValueColLabel: f.preparedFn.Type(),
		}))
	}
	return nil
}

func (f *joinFunc) Type() semantic.Type {
	if f.isWrap {
		return f.wrapObj.Type()
	}
	return f.preparedFn.Type()
}

func (f *joinFunc) Eval(rows map[string]int) (values.Object, error) {
	for tbl, references := range f.references {
		row := rows[tbl]
		data := f.tableData[tbl]
		obj, _ := f.record.Get(tbl)
		o := obj.(*execute.Record)
		for _, r := range references {
			o.Set(r, execute.ValueForRow(row, f.recordCols[tableCol{table: tbl, col: r}], data))
		}
	}
	f.scope[f.recordName] = f.record

	v, err := f.preparedFn.Eval(f.scope)
	if err != nil {
		return nil, err
	}
	if f.isWrap {
		f.wrapObj.Set(execute.DefaultValueColLabel, v)
		return f.wrapObj, nil
	}
	return v.Object(), nil
}

func findTableReferences(fn *semantic.FunctionExpression) map[string][]string {
	v := &tableReferenceVisitor{
		record: fn.Params[0].Key.Name,
		refs:   make(map[string][]string),
	}
	semantic.Walk(v, fn)
	return v.refs
}

type tableReferenceVisitor struct {
	record string
	refs   map[string][]string
}

func (c *tableReferenceVisitor) Visit(node semantic.Node) semantic.Visitor {
	if col, ok := node.(*semantic.MemberExpression); ok {
		if table, ok := col.Object.(*semantic.MemberExpression); ok {
			if record, ok := table.Object.(*semantic.IdentifierExpression); ok && record.Name == c.record {
				c.refs[table.Property] = append(c.refs[table.Property], col.Property)
				return nil
			}
		}
	}
	return c
}

func (c *tableReferenceVisitor) Done() {}
