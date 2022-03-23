package tsm1

import (
	"bytes"
	"fmt"
	"regexp"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"google.golang.org/protobuf/proto"
)

// Predicate is something that can match on a series key.
type Predicate interface {
	Clone() influxdb.Predicate
	Matches(key []byte) bool
	Marshal() ([]byte, error)
}

const ( // Enumeration of all predicate versions we support unmarshalling.
	predicateVersionZero = '\x00'
)

// UnmarshalPredicate takes stored predicate bytes from a Marshal call and returns a Predicate.
func UnmarshalPredicate(data []byte) (Predicate, error) {
	if len(data) == 0 {
		return nil, nil
	} else if data[0] != predicateVersionZero {
		return nil, fmt.Errorf("unknown tag byte: %x", data[0])
	}

	pred := new(datatypes.Predicate)
	if err := proto.Unmarshal(data[1:], pred); err != nil {
		return nil, err
	}
	return NewProtobufPredicate(pred)
}

//
// Design
//

// Predicates lazily evaluate with memoization so that we can walk a series key
// by the tags without parsing them into a structure and allocating. Each node
// in a predicate tree keeps a cache if it has enough information to have a
// definite value. The predicate state keeps track of all of the tag key/value
// pairs passed to it, and has a reset function to start over for a new series key.
//
// For example, imagine a query like
//
//	("tag1" == "val1" AND "tag2" == "val2") OR "tag3" == "val3"
//
// The state would have tag values set on it like
//
//	state.Set("tag1", "val1")        => NeedMore
//	state.Set("tag2", "not-val2")    => NeedMore
//	state.Set("tag3", "val3")        => True
//
// where after the first Set, the AND and OR clauses are both NeedMore, after
// the second Set, the AND clause is False and the OR clause is NeedMore, and
// after the last Set, the AND clause is still False, and the OR clause is True.
//
// Fast resetting is achieved by having each cache maintain a pointer to the state
// and both having a generation number. When the state resets, it bumps the generation
// number, and when the value is set in the cache, it is set with the current generation
// of the state. When querying the cache, it checks if the generation still matches.

//
// Protobuf Implementation
//

// NewProtobufPredicate returns a Predicate that matches based on the comparison structure
// described by the incoming protobuf.
func NewProtobufPredicate(pred *datatypes.Predicate) (Predicate, error) {
	// Walk the predicate to collect the tag refs
	locs := make(map[string]int)
	walkPredicateNodes(pred.Root, func(node *datatypes.Node) {
		if node.GetNodeType() == datatypes.Node_TypeTagRef {
			switch value := node.GetValue().(type) {
			case *datatypes.Node_TagRefValue:
				// Only add to the matcher locations the first time we encounter
				// the tag key reference. This prevents problems with redundant
				// predicates like:
				//
				//   foo = a AND foo = b
				//   foo = c AND foo = d
				if _, ok := locs[value.TagRefValue]; !ok {
					locs[value.TagRefValue] = len(locs)
				}
			}
		}
	})

	// Construct the shared state and root predicate node.
	state := newPredicateState(locs)
	root, err := buildPredicateNode(state, pred.Root)
	if err != nil {
		return nil, err
	}

	return &predicateMatcher{
		pred:  pred,
		state: state,
		root:  root,
	}, nil
}

// predicateMatcher implements Predicate for a protobuf.
type predicateMatcher struct {
	pred  *datatypes.Predicate
	state *predicateState
	root  predicateNode
}

// Clone returns a deep copy of p's state and root node.
//
// It is not safe to modify p.pred on the returned clone.
func (p *predicateMatcher) Clone() influxdb.Predicate {
	state := p.state.Clone()
	return &predicateMatcher{
		pred:  p.pred,
		state: state,
		root:  p.root.Clone(state),
	}
}

// Matches checks if the key matches the predicate by feeding individual tags into the
// state and returning as soon as the root node has a definite answer.
func (p *predicateMatcher) Matches(key []byte) bool {
	p.state.Reset()

	// Extract the series from the composite key
	key, _ = SeriesAndFieldFromCompositeKey(key)

	// Determine which popping algorithm to use. If there are no escape characters
	// we can use the quicker method that only works in that case.
	popTag := predicatePopTag
	if bytes.IndexByte(key, '\\') != -1 {
		popTag = predicatePopTagEscape
	}

	// Feed tag pairs into the state and update until we have a definite response.
	var tag, value []byte
	for len(key) > 0 {
		tag, value, key = popTag(key)
		if tag == nil || !p.state.Set(tag, value) {
			continue
		}
		resp := p.root.Update()
		if resp == predicateResponse_true {
			return true
		} else if resp == predicateResponse_false {
			return false
		}
	}

	// If it always needed more then it didn't match. For example, consider if
	// the predicate matches `tag1=val1` but tag1 is not present in the key.
	return false
}

// Marshal returns a buffer representing the protobuf predicate.
func (p *predicateMatcher) Marshal() ([]byte, error) {
	// Prefix it with the version byte so that we can change in the future if necessary
	buf, err := proto.Marshal(p.pred)
	return append([]byte{predicateVersionZero}, buf...), err
}

// walkPredicateNodes recursively calls the function for each node.
func walkPredicateNodes(node *datatypes.Node, fn func(node *datatypes.Node)) {
	fn(node)
	for _, ch := range node.Children {
		walkPredicateNodes(ch, fn)
	}
}

// buildPredicateNode takes a protobuf node and converts it into a predicateNode. It is strict
// in what it accepts.
func buildPredicateNode(state *predicateState, node *datatypes.Node) (predicateNode, error) {
	switch node.GetNodeType() {
	case datatypes.Node_TypeComparisonExpression:
		children := node.GetChildren()
		if len(children) != 2 {
			return nil, fmt.Errorf("invalid number of children for logical expression: %v", len(children))
		}
		left, right := children[0], children[1]

		comp := &predicateNodeComparison{
			predicateCache: newPredicateCache(state),
			comp:           node.GetComparison(),
		}

		// Fill in the left side of the comparison
		switch left.GetNodeType() {
		// Tag refs look up the location of the tag in the state
		case datatypes.Node_TypeTagRef:
			idx, ok := state.locs[left.GetTagRefValue()]
			if !ok {
				return nil, fmt.Errorf("invalid tag ref in comparison: %v", left.GetTagRefValue())
			}
			comp.leftIndex = idx

		// Left literals are only allowed to be strings
		case datatypes.Node_TypeLiteral:
			lit, ok := left.GetValue().(*datatypes.Node_StringValue)
			if !ok {
				return nil, fmt.Errorf("invalid left literal in comparison: %v", left.GetValue())
			}
			comp.leftLiteral = []byte(lit.StringValue)

		default:
			return nil, fmt.Errorf("invalid left node in comparison: %v", left.GetNodeType())
		}

		// Fill in the right side of the comparison
		switch right.GetNodeType() {
		// Tag refs look up the location of the tag in the state
		case datatypes.Node_TypeTagRef:
			idx, ok := state.locs[right.GetTagRefValue()]
			if !ok {
				return nil, fmt.Errorf("invalid tag ref in comparison: %v", right.GetTagRefValue())
			}
			comp.rightIndex = idx

		// Right literals are allowed to be regexes as well as strings
		case datatypes.Node_TypeLiteral:
			switch lit := right.GetValue().(type) {
			case *datatypes.Node_StringValue:
				comp.rightLiteral = []byte(lit.StringValue)

			case *datatypes.Node_RegexValue:
				reg, err := regexp.Compile(lit.RegexValue)
				if err != nil {
					return nil, err
				}
				comp.rightReg = reg

			default:
				return nil, fmt.Errorf("invalid right literal in comparison: %v", right.GetValue())
			}

		default:
			return nil, fmt.Errorf("invalid right node in comparison: %v", right.GetNodeType())
		}

		// Ensure that a regex is set on the right if and only if the comparison is a regex
		if comp.rightReg == nil {
			if comp.comp == datatypes.Node_ComparisonRegex || comp.comp == datatypes.Node_ComparisonNotRegex {
				return nil, fmt.Errorf("invalid comparison involving regex: %v", node)
			}
		} else {
			if comp.comp != datatypes.Node_ComparisonRegex && comp.comp != datatypes.Node_ComparisonNotRegex {
				return nil, fmt.Errorf("invalid comparison not against regex: %v", node)
			}
		}

		return comp, nil

	case datatypes.Node_TypeLogicalExpression:
		children := node.GetChildren()
		if len(children) != 2 {
			return nil, fmt.Errorf("invalid number of children for logical expression: %v", len(children))
		}

		left, err := buildPredicateNode(state, children[0])
		if err != nil {
			return nil, err
		}
		right, err := buildPredicateNode(state, children[1])
		if err != nil {
			return nil, err
		}

		switch node.GetLogical() {
		case datatypes.Node_LogicalAnd:
			return &predicateNodeAnd{
				predicateCache: newPredicateCache(state),
				left:           left,
				right:          right,
			}, nil

		case datatypes.Node_LogicalOr:
			return &predicateNodeOr{
				predicateCache: newPredicateCache(state),
				left:           left,
				right:          right,
			}, nil

		default:
			return nil, fmt.Errorf("unknown logical type: %v", node.GetLogical())
		}

	default:
		return nil, fmt.Errorf("unsupported predicate type: %v", node.GetNodeType())
	}
}

//
// Predicate Responses
//

type predicateResponse uint8

const (
	predicateResponse_needMore predicateResponse = iota
	predicateResponse_true
	predicateResponse_false
)

//
// Predicate State
//

// predicateState keeps track of tag key=>value mappings with cheap methods
// to reset to a blank state.
type predicateState struct {
	gen    uint64
	locs   map[string]int
	values [][]byte
}

// newPredicateState creates a predicateState given a map of keys to indexes into an
// an array.
func newPredicateState(locs map[string]int) *predicateState {
	return &predicateState{
		gen:    1, // so that caches start out unfilled since they start at 0
		locs:   locs,
		values: make([][]byte, len(locs)),
	}
}

// Clone returns a deep copy of p.
func (p *predicateState) Clone() *predicateState {
	q := &predicateState{
		gen:    p.gen,
		locs:   make(map[string]int, len(p.locs)),
		values: make([][]byte, len(p.values)),
	}

	for k, v := range p.locs {
		q.locs[k] = v
	}
	copy(q.values, p.values)

	return q
}

// Reset clears any set values for the state.
func (p *predicateState) Reset() {
	p.gen++

	for i := range p.values {
		p.values[i] = nil
	}
}

// Set sets the key to be the value and returns true if the key is part of the considered
// set of keys.
func (p *predicateState) Set(key, value []byte) bool {
	i, ok := p.locs[string(key)]
	if ok {
		p.values[i] = value
	}
	return ok
}

//
// Predicate Cache
//

// predicateCache interacts with the predicateState to keep determined responses
// memoized until the state has been Reset to avoid recomputing nodes.
type predicateCache struct {
	state *predicateState
	gen   uint64
	resp  predicateResponse
}

// newPredicateCache constructs a predicateCache for the provided state.
func newPredicateCache(state *predicateState) predicateCache {
	return predicateCache{
		state: state,
		gen:   0,
		resp:  predicateResponse_needMore,
	}
}

// Clone returns a deep copy of p.
func (p *predicateCache) Clone(state *predicateState) *predicateCache {
	if state == nil {
		state = p.state.Clone()
	}
	return &predicateCache{
		state: state,
		gen:   p.gen,
		resp:  p.resp,
	}
}

// Cached returns the cached response and a boolean indicating if it is valid.
func (p *predicateCache) Cached() (predicateResponse, bool) {
	return p.resp, p.gen == p.state.gen
}

// Store sets the cache to the provided response until the state is Reset.
func (p *predicateCache) Store(resp predicateResponse) {
	p.gen = p.state.gen
	p.resp = resp
}

//
// Predicate Nodes
//

// predicateNode is the interface that any parts of a predicate tree implement.
type predicateNode interface {
	// Update informs the node that the state has been updated and asks it to return
	// a response.
	Update() predicateResponse

	// Clone returns a deep copy of the node.
	Clone(state *predicateState) predicateNode
}

// predicateNodeAnd combines two predicate nodes with an And.
type predicateNodeAnd struct {
	predicateCache
	left, right predicateNode
}

// Clone returns a deep copy of p.
func (p *predicateNodeAnd) Clone(state *predicateState) predicateNode {
	return &predicateNodeAnd{
		predicateCache: *p.predicateCache.Clone(state),
		left:           p.left.Clone(state),
		right:          p.right.Clone(state),
	}
}

// Update checks if both of the left and right nodes are true. If either is false
// then the node is definitely false. Otherwise, it needs more information.
func (p *predicateNodeAnd) Update() predicateResponse {
	if resp, ok := p.Cached(); ok {
		return resp
	}

	left := p.left.Update()
	if left == predicateResponse_false {
		p.Store(predicateResponse_false)
		return predicateResponse_false
	} else if left == predicateResponse_needMore {
		return predicateResponse_needMore
	}

	right := p.right.Update()
	if right == predicateResponse_false {
		p.Store(predicateResponse_false)
		return predicateResponse_false
	} else if right == predicateResponse_needMore {
		return predicateResponse_needMore
	}

	return predicateResponse_true
}

// predicateNodeOr combines two predicate nodes with an Or.
type predicateNodeOr struct {
	predicateCache
	left, right predicateNode
}

// Clone returns a deep copy of p.
func (p *predicateNodeOr) Clone(state *predicateState) predicateNode {
	return &predicateNodeOr{
		predicateCache: *p.predicateCache.Clone(state),
		left:           p.left.Clone(state),
		right:          p.right.Clone(state),
	}
}

// Update checks if either the left and right nodes are true. If both nodes
// are false, then the node is definitely false. Otherwise, it needs more information.
func (p *predicateNodeOr) Update() predicateResponse {
	if resp, ok := p.Cached(); ok {
		return resp
	}

	left := p.left.Update()
	if left == predicateResponse_true {
		p.Store(predicateResponse_true)
		return predicateResponse_true
	}

	right := p.right.Update()
	if right == predicateResponse_true {
		p.Store(predicateResponse_true)
		return predicateResponse_true
	}

	if left == predicateResponse_false && right == predicateResponse_false {
		p.Store(predicateResponse_false)
		return predicateResponse_false
	}

	return predicateResponse_needMore
}

// predicateNodeComparison compares values of tags.
type predicateNodeComparison struct {
	predicateCache
	comp         datatypes.Node_Comparison
	rightReg     *regexp.Regexp
	leftLiteral  []byte
	rightLiteral []byte
	leftIndex    int
	rightIndex   int
}

// Clone returns a deep copy of p.
func (p *predicateNodeComparison) Clone(state *predicateState) predicateNode {
	q := &predicateNodeComparison{
		predicateCache: *p.predicateCache.Clone(state),
		comp:           p.comp,
		rightReg:       p.rightReg,
		leftIndex:      p.leftIndex,
		rightIndex:     p.rightIndex,
	}

	if p.leftLiteral != nil {
		q.leftLiteral = make([]byte, len(p.leftLiteral))
		copy(q.leftLiteral, p.leftLiteral)
	}
	if p.rightLiteral != nil {
		q.rightLiteral = make([]byte, len(p.rightLiteral))
		copy(q.rightLiteral, p.rightLiteral)
	}
	return q
}

// Update checks if both sides of the comparison are determined, and if so, evaluates
// the comparison to a determined truth value.
func (p *predicateNodeComparison) Update() predicateResponse {
	if resp, ok := p.Cached(); ok {
		return resp
	}

	left := p.leftLiteral
	if left == nil {
		left = p.state.values[p.leftIndex]
		if left == nil {
			return predicateResponse_needMore
		}
	}

	right := p.rightLiteral
	if right == nil && p.rightReg == nil {
		right = p.state.values[p.rightIndex]
		if right == nil {
			return predicateResponse_needMore
		}
	}

	if predicateEval(p.comp, left, right, p.rightReg) {
		p.Store(predicateResponse_true)
		return predicateResponse_true
	} else {
		p.Store(predicateResponse_false)
		return predicateResponse_false
	}
}

// predicateEval is a helper to do the appropriate comparison depending on which comparison
// enumeration value was passed.
func predicateEval(comp datatypes.Node_Comparison, left, right []byte, rightReg *regexp.Regexp) bool {
	switch comp {
	case datatypes.Node_ComparisonEqual:
		return string(left) == string(right)
	case datatypes.Node_ComparisonNotEqual:
		return string(left) != string(right)
	case datatypes.Node_ComparisonStartsWith:
		return bytes.HasPrefix(left, right)
	case datatypes.Node_ComparisonLess:
		return string(left) < string(right)
	case datatypes.Node_ComparisonLessEqual:
		return string(left) <= string(right)
	case datatypes.Node_ComparisonGreater:
		return string(left) > string(right)
	case datatypes.Node_ComparisonGreaterEqual:
		return string(left) >= string(right)
	case datatypes.Node_ComparisonRegex:
		return rightReg.Match(left)
	case datatypes.Node_ComparisonNotRegex:
		return !rightReg.Match(left)
	}
	return false
}

//
// Popping Tags
//

// The models package has some of this logic as well, but doesn't export ways to get
// at individual tags one at a time. In the common, no escape characters case, popping
// the first tag off of a series key takes around ~10ns.

// predicatePopTag pops a tag=value pair from the front of series, returning the
// remainder in rest. it assumes there are no escaped characters in the series.
func predicatePopTag(series []byte) (tag, value []byte, rest []byte) {
	// find the first ','
	series, rest, _ = bytes.Cut(series, []byte(","))

	// find the first '='
	tag, value, _ = bytes.Cut(series, []byte("="))

	return tag, value, rest
}

// predicatePopTagEscape pops a tag=value pair from the front of series, returning the
// remainder in rest. it assumes there are possibly/likely escaped characters in the series.
func predicatePopTagEscape(series []byte) (tag, value []byte, rest []byte) {
	// find the first unescaped ','
	for j := uint(0); j < uint(len(series)); {
		i := bytes.IndexByte(series[j:], ',')
		if i < 0 {
			break // this is the last tag pair
		}

		ui := uint(i) + j                   // make index relative to full series slice
		if ui > 0 && series[ui-1] == '\\' { // the comma is escaped
			j = ui + 1
			continue
		}

		series, rest = series[:ui], series[ui+1:]
		break
	}

	// find the first unescaped '='
	for j := uint(0); j < uint(len(series)); {
		i := bytes.IndexByte(series[j:], '=')
		if i < 0 {
			break // there is no tag value
		}
		ui := uint(i) + j                   // make index relative to full series slice
		if ui > 0 && series[ui-1] == '\\' { // the equals is escaped
			j = ui + 1
			continue
		}

		tag, value = series[:ui], series[ui+1:]
		break
	}

	// sad time: it's possible this tag/value has escaped characters, so we have to
	// find an unescape them. since the byte slice may refer to read-only memory, we
	// can't do this in place, so we make copies.
	if bytes.IndexByte(tag, '\\') != -1 {
		unescapedTag := make([]byte, 0, len(tag))
		for i, c := range tag {
			if c == '\\' && i+1 < len(tag) {
				if c := tag[i+1]; c == ',' || c == ' ' || c == '=' {
					continue
				}
			}
			unescapedTag = append(unescapedTag, c)
		}
		tag = unescapedTag
	}

	if bytes.IndexByte(value, '\\') != -1 {
		unescapedValue := make([]byte, 0, len(value))
		for i, c := range value {
			if c == '\\' && i+1 < len(value) {
				if c := value[i+1]; c == ',' || c == ' ' || c == '=' {
					continue
				}
			}
			unescapedValue = append(unescapedValue, c)
		}
		value = unescapedValue
	}

	return tag, value, rest
}
