package query

import (
	"context"

	"github.com/influxdata/influxql"
)

type subqueryBuilder struct {
	ic   IteratorCreator
	stmt *influxql.SelectStatement
}

// buildAuxIterator constructs an auxiliary Iterator from a subquery.
func (b *subqueryBuilder) buildAuxIterator(ctx context.Context, opt IteratorOptions) (Iterator, error) {
	// Map the desired auxiliary fields from the substatement.
	indexes := b.mapAuxFields(opt.Aux)

	subOpt, err := newIteratorOptionsSubstatement(ctx, b.stmt, opt)
	if err != nil {
		return nil, err
	}

	cur, err := buildCursor(ctx, b.stmt, b.ic, subOpt)
	if err != nil {
		return nil, err
	}

	// Filter the cursor by a condition if one was given.
	if opt.Condition != nil {
		cur = newFilterCursor(cur, opt.Condition)
	}

	// Construct the iterators for the subquery.
	itr := NewIteratorMapper(cur, nil, indexes, subOpt)
	if len(opt.GetDimensions()) != len(subOpt.GetDimensions()) {
		itr = NewTagSubsetIterator(itr, opt)
	}
	return itr, nil
}

func (b *subqueryBuilder) mapAuxFields(auxFields []influxql.VarRef) []IteratorMap {
	indexes := make([]IteratorMap, len(auxFields))
	for i, name := range auxFields {
		m := b.mapAuxField(&name)
		if m == nil {
			// If this field doesn't map to anything, use the NullMap so it
			// shows up as null.
			m = NullMap{}
		}
		indexes[i] = m
	}
	return indexes
}

func (b *subqueryBuilder) mapAuxField(name *influxql.VarRef) IteratorMap {
	offset := 0
	for i, f := range b.stmt.Fields {
		if f.Name() == name.Val {
			return FieldMap{
				Index: i + offset,
				// Cast the result of the field into the desired type.
				Type: name.Type,
			}
		} else if call, ok := f.Expr.(*influxql.Call); ok && (call.Name == "top" || call.Name == "bottom") {
			// We may match one of the arguments in "top" or "bottom".
			if len(call.Args) > 2 {
				for j, arg := range call.Args[1 : len(call.Args)-1] {
					if arg, ok := arg.(*influxql.VarRef); ok && arg.Val == name.Val {
						return FieldMap{
							Index: i + j + 1,
							Type:  influxql.String,
						}
					}
				}
				// Increment the offset so we have the correct index for later fields.
				offset += len(call.Args) - 2
			}
		}
	}

	// Unable to find this in the list of fields.
	// Look within the dimensions and create a field if we find it.
	for _, d := range b.stmt.Dimensions {
		if d, ok := d.Expr.(*influxql.VarRef); ok && name.Val == d.Val {
			return TagMap(d.Val)
		}
	}

	// Unable to find any matches.
	return nil
}

func (b *subqueryBuilder) buildVarRefIterator(ctx context.Context, expr *influxql.VarRef, opt IteratorOptions) (Iterator, error) {
	// Look for the field or tag that is driving this query.
	driver := b.mapAuxField(expr)
	if driver == nil {
		// Exit immediately if there is no driver. If there is no driver, there
		// are no results. Period.
		return nil, nil
	}

	// Map the auxiliary fields to their index in the subquery.
	indexes := b.mapAuxFields(opt.Aux)
	subOpt, err := newIteratorOptionsSubstatement(ctx, b.stmt, opt)
	if err != nil {
		return nil, err
	}

	cur, err := buildCursor(ctx, b.stmt, b.ic, subOpt)
	if err != nil {
		return nil, err
	}

	// Filter the cursor by a condition if one was given.
	if opt.Condition != nil {
		cur = newFilterCursor(cur, opt.Condition)
	}

	// Construct the iterators for the subquery.
	itr := NewIteratorMapper(cur, driver, indexes, subOpt)
	if len(opt.GetDimensions()) != len(subOpt.GetDimensions()) {
		itr = NewTagSubsetIterator(itr, opt)
	}
	return itr, nil
}
