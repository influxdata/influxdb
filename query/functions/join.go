package functions

import (
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/interpreter"
	"github.com/influxdata/platform/query/plan"
	"github.com/influxdata/platform/query/semantic"
	"github.com/influxdata/platform/query/values"
	"github.com/pkg/errors"
)

const JoinKind = "join"
const MergeJoinKind = "merge-join"

// All supported join types in Flux
var methods map[string]bool = map[string]bool{
	"inner": true,
}

type JoinOpSpec struct {
	// On is a list of tags on which to join.
	On []string `json:"on"`
	// TableNames are the names to give to each parent when populating the parameter for the function.
	// The first parent is referenced by the first name and so forth.
	// TODO(nathanielc): Change this to a map of parent operation IDs to names.
	// Then make it possible for the transformation to map operation IDs to parent IDs.
	TableNames map[query.OperationID]string `json:"tableNames"`
	// Method is a the type of join to perform
	Method string `json:"method"`
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
		"on":     semantic.NewArrayType(semantic.String),
		"method": semantic.String,
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
	spec := &JoinOpSpec{
		TableNames: make(map[query.OperationID]string),
		tableNames: make(map[*query.TableObject]string),
	}

	// On specifies the columns to join on. If 'on' is not present in the arguments
	// to join, the default value will be set when the join tables are processed.
	// Specifically when the schema of the output table is able to be constructed.
	if array, ok, err := args.GetArray("on", semantic.String); err != nil {
		return nil, err
	} else if ok {
		spec.On, err = interpreter.ToStringArray(array)
		if err != nil {
			return nil, err
		}
	}

	// Method is an optional parameter that when not specified defaults to
	// the inner join type.
	if joinType, ok, err := args.GetString("method"); err != nil {
		return nil, err
	} else if ok && !methods[joinType] {
		return nil, fmt.Errorf("%s is not a valid join type", joinType)
	} else if ok && methods[joinType] {
		spec.Method = joinType
	} else {
		spec.Method = "inner"
	}

	// It is not valid to specify a list of 'on' columns for a cross product
	if spec.Method == "cross" && spec.On != nil {
		return nil, errors.New("cross product and 'on' are mutually exclusive")
	}

	tables, err := args.GetRequiredObject("tables")
	if err != nil {
		return nil, err
	}

	joinParams := newJoinParams(tables.Len())
	tables.Range(func(k string, t values.Value) {
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
		joinParams.add(k, p)
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
	On         []string                    `json:"keys"`
	TableNames map[plan.ProcedureID]string `json:"table_names"`
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

	cache := NewMergeJoinCache(a.Allocator(), parents, tableNames, s.On)
	d := execute.NewDataset(id, mode, cache)
	t := NewMergeJoinTransformation(d, cache, s, parents, tableNames)
	return t, d, nil
}

type mergeJoinTransformation struct {
	parents []execute.DatasetID

	mu sync.Mutex

	d     execute.Dataset
	cache *MergeJoinCache

	leftID, rightID     execute.DatasetID
	leftName, rightName string

	parentState map[execute.DatasetID]*mergeJoinParentState

	keys []string
}

func NewMergeJoinTransformation(d execute.Dataset, cache *MergeJoinCache, spec *MergeJoinProcedureSpec, parents []execute.DatasetID, tableNames map[execute.DatasetID]string) *mergeJoinTransformation {
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

// Process processes a table from an incoming data stream.
// It adds the table to an internal buffer and stores any output
// group keys that can be constructed as a result of the new addition.
func (t *mergeJoinTransformation) Process(id execute.DatasetID, tbl query.Table) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.cache.insertIntoBuffer(id, tbl)

	// Check if enough data sources have been seen to produce an output schema
	if !t.cache.isBufferEmpty(t.leftID) && !t.cache.isBufferEmpty(t.rightID) && !t.cache.postJoinSchemaBuilt() {
		t.cache.buildPostJoinSchema()
	}

	// Register any new output group keys that can be constructed from the new table
	t.cache.registerKey(id, tbl.Key())
	return nil
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

// MergeJoinCache implements execute.DataCache
// This is where the all the tables to be joined are stored.
//
// buffers:         Buffers to hold the tables for each incoming stream.
//
// postJoinKeys:    The post-join group keys for all joined tables.
//                  These group keys are constructed and stored as soon
//                  as a table is consumed by the join operator, but prior
//                  to actually joining the data.
//
// reverseLookup:   Each output group key that is stored is mapped to its
//                  corresponding pre-join group keys. These pre-join group
//                  keys are then used to retrieve their correspoinding
//                  tables from the buffers.
//
// tables:          All output tables are materialized and stored in this
//                  map before being sent to downstream operators.
type MergeJoinCache struct {
	leftID  execute.DatasetID
	rightID execute.DatasetID

	names   map[execute.DatasetID]string
	schemas map[execute.DatasetID]schema
	buffers map[execute.DatasetID]*streamBuffer

	on           map[string]bool
	intersection map[string]bool

	schema    schema
	colIndex  map[query.ColMeta]int
	schemaMap map[tableCol]query.ColMeta

	postJoinKeys  *execute.GroupLookup
	reverseLookup map[query.GroupKey]preJoinGroupKeys

	tables      map[query.GroupKey]query.Table
	alloc       *execute.Allocator
	triggerSpec query.TriggerSpec
}

type streamBuffer struct {
	data     map[query.GroupKey]*execute.ColListTableBuilder
	consumed map[values.Value]int
	ready    map[values.Value]bool
	stale    map[query.GroupKey]bool
	last     values.Value
	alloc    *execute.Allocator
}

func newStreamBuffer(alloc *execute.Allocator) *streamBuffer {
	return &streamBuffer{
		data:     make(map[query.GroupKey]*execute.ColListTableBuilder),
		consumed: make(map[values.Value]int),
		ready:    make(map[values.Value]bool),
		stale:    make(map[query.GroupKey]bool),
		alloc:    alloc,
	}
}

func (buf *streamBuffer) table(key query.GroupKey) *execute.ColListTableBuilder {
	return buf.data[key]
}

func (buf *streamBuffer) insert(table query.Table) {
	// Construct a new table builder with same schema as input table
	builder := execute.NewColListTableBuilder(table.Key(), buf.alloc)
	execute.AddTableCols(table, builder)

	builderColumnsToTableColumns := make([]int, len(builder.Cols()))
	for i := range builder.Cols() {
		builderColumnsToTableColumns[i] = i
	}

	// Append the input table to this builder
	execute.AppendTable(table, builder, builderColumnsToTableColumns)

	// Insert this table into the buffer
	buf.data[table.Key()] = builder

	if len(table.Key().Cols()) > 0 {
		leftKeyValue := table.Key().Value(0)

		tablesConsumed := buf.consumed[leftKeyValue]
		buf.consumed[leftKeyValue] = tablesConsumed + 1

		if buf.last == nil {
			buf.last = leftKeyValue
		}

		if !buf.last.Equal(leftKeyValue) {
			buf.ready[buf.last] = true
			buf.last = leftKeyValue
		}
	}
}

func (buf *streamBuffer) expire(key query.GroupKey) {
	if !buf.stale[key] && len(key.Cols()) > 0 {
		leftKeyValue := key.Value(0)
		consumedTables := buf.consumed[leftKeyValue]
		buf.consumed[leftKeyValue] = consumedTables - 1
		buf.stale[key] = true
	}
}

func (buf *streamBuffer) evict(key query.GroupKey) {
	if builder, ok := buf.data[key]; ok {
		builder.ClearData()
		delete(buf.data, key)
	}
}

func (buf *streamBuffer) clear(f func(query.GroupKey) bool) {
	for key := range buf.stale {
		if f(key) {
			buf.evict(key)
			delete(buf.stale, key)
		}
	}
}

func (buf *streamBuffer) iterate(f func(query.GroupKey)) {
	for key := range buf.data {
		f(key)
	}
}

type tableCol struct {
	table, col string
}

type preJoinGroupKeys struct {
	left, right query.GroupKey
}

type schema struct {
	key     []query.ColMeta
	columns []query.ColMeta
}

func (s schema) Len() int {
	return len(s.columns)

}
func (s schema) Less(i int, j int) bool {
	return s.columns[i].Label < s.columns[j].Label
}

func (s schema) Swap(i int, j int) {
	s.columns[i], s.columns[j] = s.columns[j], s.columns[i]
}

// NewMergeJoinCache constructs a new instance of a MergeJoinCache
func NewMergeJoinCache(alloc *execute.Allocator, datasetIDs []execute.DatasetID, tableNames map[execute.DatasetID]string, key []string) *MergeJoinCache {
	// Join currently only accepts two data sources(streams) as input
	if len(datasetIDs) != 2 {
		panic("Join only accepts two data sources")
	}

	names := make(map[execute.DatasetID]string, len(datasetIDs))
	schemas := make(map[execute.DatasetID]schema, len(datasetIDs))
	buffers := make(map[execute.DatasetID]*streamBuffer, len(datasetIDs))

	for _, datasetID := range datasetIDs {
		names[datasetID] = tableNames[datasetID]
		buffers[datasetID] = newStreamBuffer(alloc)
	}

	on := make(map[string]bool, len(key))
	intersection := make(map[string]bool, len(key))

	for _, k := range key {
		on[k] = true
		intersection[k] = true
	}

	return &MergeJoinCache{
		on:            on,
		intersection:  intersection,
		leftID:        datasetIDs[0],
		rightID:       datasetIDs[1],
		names:         names,
		schemas:       schemas,
		buffers:       buffers,
		reverseLookup: make(map[query.GroupKey]preJoinGroupKeys),
		postJoinKeys:  execute.NewGroupLookup(),
		tables:        make(map[query.GroupKey]query.Table),
		alloc:         alloc,
	}
}

// Table joins the two tables associated with a single output group key and returns the resulting table
func (c *MergeJoinCache) Table(key query.GroupKey) (query.Table, error) {
	preJoinGroupKeys, ok := c.reverseLookup[key]

	if !ok {
		return nil, fmt.Errorf("No table exists with group key: %v", key)
	}

	if _, ok := c.tables[key]; !ok {

		left := c.buffers[c.leftID].table(preJoinGroupKeys.left)
		if left == nil {
			return nil, fmt.Errorf("No table in left join buffer with key: %v", key)
		}

		right := c.buffers[c.rightID].table(preJoinGroupKeys.right)
		if left == nil {
			return nil, fmt.Errorf("No table in right join buffer with key: %v", key)
		}

		table, err := c.join(left, right)
		if err != nil {
			return nil, fmt.Errorf("Table with group key (%v) could not be fetched", key)
		}

		c.tables[key] = table
	}
	return c.tables[key], nil
}

// ForEach iterates over each table in the output stream
func (c *MergeJoinCache) ForEach(f func(query.GroupKey)) {
	c.postJoinKeys.Range(func(key query.GroupKey, value interface{}) {

		if _, ok := c.tables[key]; !ok {

			preJoinGroupKeys := c.reverseLookup[key]

			leftKey := preJoinGroupKeys.left
			rightKey := preJoinGroupKeys.right

			leftBuilder := c.buffers[c.leftID].table(leftKey)
			rightBuilder := c.buffers[c.rightID].table(rightKey)

			table, err := c.join(leftBuilder, rightBuilder)
			if err != nil || table.Empty() {
				c.DiscardTable(key)
				return
			}

			c.tables[key] = table
		}
		f(key)
	})
}

// ForEachWithContext iterates over each table in the output stream
func (c *MergeJoinCache) ForEachWithContext(f func(query.GroupKey, execute.Trigger, execute.TableContext)) {
	trigger := execute.NewTriggerFromSpec(c.triggerSpec)

	c.postJoinKeys.Range(func(key query.GroupKey, value interface{}) {

		preJoinGroupKeys := c.reverseLookup[key]

		leftKey := preJoinGroupKeys.left
		rightKey := preJoinGroupKeys.right

		leftBuilder := c.buffers[c.leftID].table(leftKey)
		rightBuilder := c.buffers[c.rightID].table(rightKey)

		if _, ok := c.tables[key]; !ok {

			table, err := c.join(leftBuilder, rightBuilder)

			if err != nil || table.Empty() {
				c.DiscardTable(key)
				return
			}

			c.tables[key] = table
		}

		leftsize := leftBuilder.NRows()
		rightsize := rightBuilder.NRows()

		ctx := execute.TableContext{
			Key:   key,
			Count: leftsize + rightsize,
		}

		f(key, trigger, ctx)
	})
}

// DiscardTable removes a table from the output buffer
func (c *MergeJoinCache) DiscardTable(key query.GroupKey) {
	delete(c.tables, key)
}

// ExpireTable removes the a key from the set of postJoinKeys.
// ExpireTable will be called after the table associated with key has already
// been materialized. As a result, it cannot not be materialized again. Each
// buffer is cleared of any stale data that arises as a result of this process.
func (c *MergeJoinCache) ExpireTable(key query.GroupKey) {
	// Remove this group key from the cache
	c.postJoinKeys.Delete(key)
	delete(c.tables, key)

	// Clear any stale data
	preJoinGroupKeys := c.reverseLookup[key]

	leftBuffer := c.buffers[c.leftID]
	rightBuffer := c.buffers[c.rightID]

	leftBuffer.expire(preJoinGroupKeys.left)
	rightBuffer.expire(preJoinGroupKeys.right)

	if c.canEvictTables() {

		leftBuffer.clear(func(key query.GroupKey) bool {
			return rightBuffer.ready[key.Value(0)] &&
				rightBuffer.consumed[key.Value(0)] == 0
		})

		rightBuffer.clear(func(key query.GroupKey) bool {
			return leftBuffer.ready[key.Value(0)] &&
				leftBuffer.consumed[key.Value(0)] == 0
		})
	}
}

// SetTriggerSpec sets the trigger rule for this cache
func (c *MergeJoinCache) SetTriggerSpec(spec query.TriggerSpec) {
	c.triggerSpec = spec
}

// Currently tables are the smallest unit of data that can be evicted from the join's internal
// buffers. This is the rule that specifies whether a data cache can early evict tables.
func (c *MergeJoinCache) canEvictTables() bool {
	leftKey := c.schemas[c.leftID].key
	rightKey := c.schemas[c.rightID].key
	return len(leftKey) > 0 && len(rightKey) > 0 &&
		leftKey[0].Label == rightKey[0].Label && c.on[leftKey[0].Label]
}

// insertIntoBuffer adds the rows of an incoming table to one of the Join's internal buffers
func (c *MergeJoinCache) insertIntoBuffer(id execute.DatasetID, tbl query.Table) {
	// Initialize schema if tbl is first from its stream
	if _, ok := c.schemas[id]; !ok {

		c.schemas[id] = schema{
			key:     make([]query.ColMeta, len(tbl.Key().Cols())),
			columns: make([]query.ColMeta, len(tbl.Cols())),
		}

		for j, column := range tbl.Cols() {
			c.schemas[id].columns[j] = column
		}

		intersection := make(map[string]bool, len(c.intersection))

		for j, column := range tbl.Key().Cols() {
			c.schemas[id].key[j] = column

			if c.intersection[column.Label] {
				intersection[column.Label] = true
			}
		}

		c.intersection = intersection
	}
	c.buffers[id].insert(tbl)
}

// registerKey takes a group key from the input stream associated with id and joins
// it with all other group keys from the opposing input stream. If it is determined
// that two group keys will not join (due to having different values on a join column)
// they are skipped.
func (c *MergeJoinCache) registerKey(id execute.DatasetID, key query.GroupKey) {
	var empty struct{}
	switch id {

	case c.leftID:

		c.buffers[c.rightID].iterate(func(groupKey query.GroupKey) {

			keys := map[execute.DatasetID]query.GroupKey{
				c.leftID:  key,
				c.rightID: groupKey,
			}

			for k := range c.intersection {
				if !key.LabelValue(k).Equal(groupKey.LabelValue(k)) {
					return
				}
			}

			outputGroupKey := c.postJoinGroupKey(keys)
			c.postJoinKeys.Set(outputGroupKey, empty)

			c.reverseLookup[outputGroupKey] = preJoinGroupKeys{
				left:  key,
				right: groupKey,
			}
		})

	case c.rightID:

		c.buffers[c.leftID].iterate(func(groupKey query.GroupKey) {

			keys := map[execute.DatasetID]query.GroupKey{
				c.leftID:  groupKey,
				c.rightID: key,
			}

			for k := range c.intersection {
				if !key.LabelValue(k).Equal(groupKey.LabelValue(k)) {
					return
				}
			}

			outputGroupKey := c.postJoinGroupKey(keys)
			c.postJoinKeys.Set(outputGroupKey, empty)

			c.reverseLookup[outputGroupKey] = preJoinGroupKeys{
				left:  groupKey,
				right: key,
			}
		})
	}
}

func (c *MergeJoinCache) isBufferEmpty(id execute.DatasetID) bool {
	return len(c.buffers[id].data) == 0
}

func (c *MergeJoinCache) postJoinSchemaBuilt() bool {
	return c.schemaMap != nil
}

func (c *MergeJoinCache) buildPostJoinSchema() {
	left := c.schemas[c.leftID].columns
	right := c.schemas[c.rightID].columns

	// Find column names shared between the two tables
	shared := make(map[string]bool, len(left))
	for _, leftColumn := range left {
		for _, rightColumn := range right {

			if leftColumn.Label == rightColumn.Label {
				shared[leftColumn.Label] = true
				break
			}
		}
	}

	if len(c.on) == 0 {
		c.on = shared
	}

	ncols := len(left) + len(right)

	c.schema = schema{
		columns: make([]query.ColMeta, 0, ncols-len(c.on)),
		key:     make([]query.ColMeta, 0, ncols-len(c.on)),
	}

	c.colIndex = make(map[query.ColMeta]int, ncols-len(c.on))
	c.schemaMap = make(map[tableCol]query.ColMeta, ncols)
	added := make(map[string]bool, ncols-len(c.on))

	// Build schema for output table
	addColumnsToSchema(c.names[c.leftID], left, added, shared, c.on, &c.schema, c.schemaMap)
	addColumnsToSchema(c.names[c.rightID], right, added, shared, c.on, &c.schema, c.schemaMap)

	// Give schema an order
	sort.Sort(c.schema)
	for j, column := range c.schema.columns {
		c.colIndex[column] = j
	}
}

func (c *MergeJoinCache) join(left, right *execute.ColListTableBuilder) (query.Table, error) {
	// Determine sort order for the joining tables
	on := make([]string, len(c.on))

	for k := range c.on {
		on = append(on, k)
	}

	// Sort input tables
	left.Sort(on, false)
	right.Sort(on, false)

	var leftSet, rightSet subset
	var leftKey, rightKey query.GroupKey

	leftTable, rightTable := left.RawTable(), right.RawTable()
	leftSet, leftKey = c.advance(leftSet.Stop, leftTable)
	rightSet, rightKey = c.advance(rightSet.Stop, rightTable)

	keys := map[execute.DatasetID]query.GroupKey{
		c.leftID:  left.Key(),
		c.rightID: right.Key(),
	}

	// Instantiate a builder for the output table
	groupKey := c.postJoinGroupKey(keys)
	builder := execute.NewColListTableBuilder(groupKey, c.alloc)

	for _, column := range c.schema.columns {
		builder.AddCol(column)
	}

	// Perform sort merge join
	for !leftSet.Empty() && !rightSet.Empty() {
		if leftKey.Equal(rightKey) {

			for l := leftSet.Start; l < leftSet.Stop; l++ {
				for r := rightSet.Start; r < rightSet.Stop; r++ {

					leftRecord := leftTable.GetRow(l)
					rightRecord := rightTable.GetRow(r)

					leftRecord.Range(func(columnName string, columnVal values.Value) {
						column := tableCol{
							table: c.names[c.leftID],
							col:   columnName,
						}
						newColumn := c.schemaMap[column]
						newColumnIdx := c.colIndex[newColumn]
						execute.AppendValue(builder, newColumnIdx, columnVal)
					})

					rightRecord.Range(func(columnName string, columnVal values.Value) {
						column := tableCol{
							table: c.names[c.rightID],
							col:   columnName,
						}
						newColumn := c.schemaMap[column]
						newColumnIdx := c.colIndex[newColumn]

						// No need to append value if column is part of the join key.
						// Because value already appended when iterating over left record.
						if !c.on[newColumn.Label] {
							execute.AppendValue(builder, newColumnIdx, columnVal)
						}
					})
				}
			}
			leftSet, leftKey = c.advance(leftSet.Stop, leftTable)
			rightSet, rightKey = c.advance(rightSet.Stop, rightTable)
		} else if leftKey.Less(rightKey) {
			leftSet, leftKey = c.advance(leftSet.Stop, leftTable)
		} else {
			rightSet, rightKey = c.advance(rightSet.Stop, rightTable)
		}
	}

	return builder.Table()
}

// postJoinGroupKey produces a new group key value from a left and a right group key value
func (c *MergeJoinCache) postJoinGroupKey(keys map[execute.DatasetID]query.GroupKey) query.GroupKey {
	key := groupKey{
		cols: make([]query.ColMeta, 0, len(keys)*5),
		vals: make([]values.Value, 0, len(keys)*5),
	}

	added := make(map[string]bool, len(keys)*5)

	for id, groupKey := range keys {
		for j, column := range groupKey.Cols() {

			tableAndColumn := tableCol{
				table: c.names[id],
				col:   column.Label,
			}

			colMeta := c.schemaMap[tableAndColumn]

			if !added[colMeta.Label] {
				key.cols = append(key.cols, colMeta)
				key.vals = append(key.vals, groupKey.Value(j))
			}

			added[colMeta.Label] = true
		}
	}

	// Table columns are always sorted so need
	// to sort the group key for consistency
	sort.Sort(key)
	return execute.NewGroupKey(key.cols, key.vals)
}

// advance advances the row pointer of a sorted table that is being joined
func (c *MergeJoinCache) advance(offset int, table query.ColReader) (subset, query.GroupKey) {
	if n := table.Len(); n == offset {
		return subset{Start: n, Stop: n}, nil
	}
	start := offset
	key := execute.GroupKeyForRowOn(start, table, c.on)
	sequence := subset{Start: start}
	offset++
	for offset < table.Len() && equalRowKeys(start, offset, table, c.on) {
		offset++
	}
	sequence.Stop = offset
	return sequence, key
}

type subset struct {
	Start int
	Stop  int
}

func (s subset) Empty() bool {
	return s.Start == s.Stop
}

// equalRowKeys determines whether two rows of a table are equal on the set of columns defined by on
func equalRowKeys(x, y int, table query.ColReader, on map[string]bool) bool {
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

func addColumnsToSchema(name string, columns []query.ColMeta, added, shared, on map[string]bool, schema *schema, schemaMap map[tableCol]query.ColMeta) {
	for _, column := range columns {

		tableAndColumn := tableCol{
			table: name,
			col:   column.Label,
		}

		newLabel := renameColumn(tableAndColumn, shared, on)
		newColumn := query.ColMeta{
			Label: newLabel,
			Type:  column.Type,
		}

		schemaMap[tableAndColumn] = newColumn

		if !added[newLabel] {
			schema.columns = append(schema.columns, newColumn)
		}

		added[newLabel] = true
	}
}

func renameColumn(col tableCol, share, on map[string]bool) string {
	columnName := col.col

	if share[columnName] && !on[columnName] {
		return fmt.Sprintf("%s_%s", col.table, columnName)
	}
	return columnName
}

type groupKey struct {
	cols []query.ColMeta
	vals []values.Value
}

func (k groupKey) Len() int {
	return len(k.cols)
}

func (k groupKey) Less(i, j int) bool {
	return k.cols[i].Label < k.cols[j].Label
}

func (k groupKey) Swap(i, j int) {
	k.cols[i], k.cols[j] = k.cols[j], k.cols[i]
	k.vals[i], k.vals[j] = k.vals[j], k.vals[i]
}
