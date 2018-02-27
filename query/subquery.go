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
	// Retrieve a list of fields needed for conditions.
	auxFields := opt.Aux
	conds := influxql.ExprNames(opt.Condition)
	if len(conds) > 0 {
		auxFields = make([]influxql.VarRef, len(opt.Aux)+len(conds))
		copy(auxFields, opt.Aux)
		copy(auxFields[len(opt.Aux):], conds)
	}

	// Map the desired auxiliary fields from the substatement.
	indexes := b.mapAuxFields(auxFields)
	subOpt, err := newIteratorOptionsSubstatement(ctx, b.stmt, opt)
	if err != nil {
		return nil, err
	}
	subOpt.Aux = auxFields

	itrs, err := buildIterators(ctx, b.stmt, b.ic, subOpt)
	if err != nil {
		return nil, err
	}

	// Construct the iterators for the subquery.
	input := NewIteratorMapper(itrs, nil, indexes, subOpt)
	// If there is a condition, filter it now.
	if opt.Condition != nil {
		input = NewFilterIterator(input, opt.Condition, subOpt)
	}
	return input, nil
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
			return FieldMap(i + offset)
		} else if call, ok := f.Expr.(*influxql.Call); ok && (call.Name == "top" || call.Name == "bottom") {
			// We may match one of the arguments in "top" or "bottom".
			if len(call.Args) > 2 {
				for j, arg := range call.Args[1 : len(call.Args)-1] {
					if arg, ok := arg.(*influxql.VarRef); ok && arg.Val == name.Val {
						return FieldMap(i + j + 1)
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

	// Determine necessary auxiliary fields for this query.
	auxFields := opt.Aux
	conds := influxql.ExprNames(opt.Condition)
	if len(conds) > 0 && len(opt.Aux) > 0 {
		// Combine the auxiliary fields requested with the ones in the condition.
		auxFields = make([]influxql.VarRef, len(opt.Aux)+len(conds))
		copy(auxFields, opt.Aux)
		copy(auxFields[len(opt.Aux):], conds)
	} else if len(conds) > 0 {
		// Set the auxiliary fields to what is in the condition since we have
		// requested none in the query itself.
		auxFields = conds
	}

	// Map the auxiliary fields to their index in the subquery.
	indexes := b.mapAuxFields(auxFields)
	subOpt, err := newIteratorOptionsSubstatement(ctx, b.stmt, opt)
	if err != nil {
		return nil, err
	}
	subOpt.Aux = auxFields

	itrs, err := buildIterators(ctx, b.stmt, b.ic, subOpt)
	if err != nil {
		return nil, err
	}

	// Construct the iterators for the subquery.
	input := NewIteratorMapper(itrs, driver, indexes, subOpt)
	// If there is a condition, filter it now.
	if opt.Condition != nil {
		input = NewFilterIterator(input, opt.Condition, subOpt)
	}
	return input, nil
}
