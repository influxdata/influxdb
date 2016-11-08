package tsi1

/*
import (
	"github.com/influxdata/influxdb/influxql"
)

// TagSets returns the unique tag sets that exist for the given tag keys. This
// is used to determine what composite series will be created by a group by.
//
// i.e. "group by region" should return: {"region":"uswest"},
// {"region":"useast"} or region, service returns {"region": "uswest",
// "service": "redis"}, {"region": "uswest", "service": "mysql"}, etc...
//
// This will also populate the TagSet objects with the series IDs that match
// each tagset and any influx filter expression that goes with the series TODO:
// this shouldn't be exported. However, until tx.go and the engine get
// refactored into tsdb, we need it.
func (m *Measurement) TagSets(dimensions []string, condition influxql.Expr) ([]*influxql.TagSet, error) {
	m.mu.RLock()

	// TODO(benbjohnson):
	// Iterators are needed at the series id level and the series level. The
	// series id will allow us to union faster. We can't intersect at the
	// series id level because that could remove series which would intersect
	// at a higher cross-file level.
	//
	// - IndexFile.SeriesIteratorByExpr(condition)
	// - LogFile.SeriesIteratorByExpr(condition)
	//
	// - UnionSeriesIterators()
	// - IntersectSeriesIterators()
	// - unionSeriesIDIterators()

	// TODO(benbjohnson):
	// Create series iterator based on condition. If condition is time-only
	// the return all measurement series ids. Otherwise walk condition and merge
	// series via walkWhereForSeriesIds()/idsForExpr().

	// get the unique set of series ids and the filters that should be applied to each
	ids, filters, err := m.filters(condition)
	if err != nil {
		m.mu.RUnlock()
		return nil, err
	}

	// TODO(benbjohnson):
	// Iterate over each series and build tagsets with dimensions.
	// Limit and offset as needed.

	// For every series, get the tag values for the requested tag keys i.e. dimensions. This is the
	// TagSet for that series. Series with the same TagSet are then grouped together, because for the
	// purpose of GROUP BY they are part of the same composite series.
	tagSets := make(map[string]*influxql.TagSet, 64)
	for _, id := range ids {
		s := m.seriesByID[id]
		tags := make(map[string]string, len(dimensions))

		// Build the TagSet for this series.
		for _, dim := range dimensions {
			tags[dim] = s.Tags.GetString(dim)
		}
		// Convert the TagSet to a string, so it can be added to a map allowing TagSets to be handled
		// as a set.
		tagsAsKey := MarshalTags(tags)
		tagSet, ok := tagSets[string(tagsAsKey)]
		if !ok {
			// This TagSet is new, create a new entry for it.
			tagSet = &influxql.TagSet{
				Tags: tags,
				Key:  tagsAsKey,
			}
		}
		// Associate the series and filter with the Tagset.
		tagSet.AddFilter(m.seriesByID[id].Key, filters[id])

		// Ensure it's back in the map.
		tagSets[string(tagsAsKey)] = tagSet
	}
	// Release the lock while we sort all the tags
	m.mu.RUnlock()

	// Sort the series in each tag set.
	for _, t := range tagSets {
		sort.Sort(t)
	}

	// The TagSets have been created, as a map of TagSets. Just send
	// the values back as a slice, sorting for consistency.
	sortedTagsSets := make([]*influxql.TagSet, 0, len(tagSets))
	for _, v := range tagSets {
		sortedTagsSets = append(sortedTagsSets, v)
	}
	sort.Sort(byTagKey(sortedTagsSets))

	return sortedTagsSets, nil
}

// filters walks the where clause of a select statement and returns a map with all series ids
// matching the where clause and any filter expression that should be applied to each
func (m *Measurement) filters(condition influxql.Expr) ([]uint64, map[uint64]influxql.Expr, error) {
	if condition == nil || influxql.OnlyTimeExpr(condition) {
		return m.seriesIDs, nil, nil
	}
	return m.walkWhereForSeriesIds(condition)
}

// walkWhereForSeriesIds recursively walks the WHERE clause and returns an
// ordered set of series IDs and a map from those series IDs to filter
// expressions that should be used to limit points returned in the final query
// result.
func (m *Measurement) walkWhereForSeriesIds(expr influxql.Expr) (SeriesIDs, FilterExprs, error) {
	switch n := expr.(type) {
	case *influxql.BinaryExpr:
		switch n.Op {
		case influxql.EQ, influxql.NEQ, influxql.LT, influxql.LTE, influxql.GT, influxql.GTE, influxql.EQREGEX, influxql.NEQREGEX:
			// Get the series IDs and filter expression for the tag or field comparison.
			ids, expr, err := m.idsForExpr(n)
			if err != nil {
				return nil, nil, err
			}

			if len(ids) == 0 {
				return ids, nil, nil
			}

			// If the expression is a boolean literal that is true, ignore it.
			if b, ok := expr.(*influxql.BooleanLiteral); ok && b.Val {
				expr = nil
			}

			var filters FilterExprs
			if expr != nil {
				filters = make(FilterExprs, len(ids))
				for _, id := range ids {
					filters[id] = expr
				}
			}

			return ids, filters, nil
		case influxql.AND, influxql.OR:
			// Get the series IDs and filter expressions for the LHS.
			lids, lfilters, err := m.walkWhereForSeriesIds(n.LHS)
			if err != nil {
				return nil, nil, err
			}

			// Get the series IDs and filter expressions for the RHS.
			rids, rfilters, err := m.walkWhereForSeriesIds(n.RHS)
			if err != nil {
				return nil, nil, err
			}

			// Combine the series IDs from the LHS and RHS.
			if n.Op == influxql.AND {
				ids, filters := intersectSeriesFilters(lids, rids, lfilters, rfilters)
				return ids, filters, nil
			} else {
				ids, filters := unionSeriesFilters(lids, rids, lfilters, rfilters)
				return ids, filters, nil
			}
		}

		ids, _, err := m.idsForExpr(n)
		return ids, nil, err
	case *influxql.ParenExpr:
		// walk down the tree
		return m.walkWhereForSeriesIds(n.Expr)
	default:
		return nil, nil, nil
	}
}

// intersectSeriesFilters performs an intersection for two sets of ids and filter expressions.
func intersectSeriesFilters(lids, rids SeriesIDs, lfilters, rfilters FilterExprs) (SeriesIDs, FilterExprs) {
	// We only want to allocate a slice and map of the smaller size.
	var ids []uint64
	if len(lids) > len(rids) {
		ids = make([]uint64, 0, len(rids))
	} else {
		ids = make([]uint64, 0, len(lids))
	}

	var filters FilterExprs
	if len(lfilters) > len(rfilters) {
		filters = make(FilterExprs, len(rfilters))
	} else {
		filters = make(FilterExprs, len(lfilters))
	}

	// They're in sorted order so advance the counter as needed.
	// This is, don't run comparisons against lower values that we've already passed.
	for len(lids) > 0 && len(rids) > 0 {
		lid, rid := lids[0], rids[0]
		if lid == rid {
			ids = append(ids, lid)

			var expr influxql.Expr
			lfilter := lfilters[lid]
			rfilter := rfilters[rid]

			if lfilter != nil && rfilter != nil {
				be := &influxql.BinaryExpr{
					Op:  influxql.AND,
					LHS: lfilter,
					RHS: rfilter,
				}
				expr = influxql.Reduce(be, nil)
			} else if lfilter != nil {
				expr = lfilter
			} else if rfilter != nil {
				expr = rfilter
			}

			if expr != nil {
				filters[lid] = expr
			}
			lids, rids = lids[1:], rids[1:]
		} else if lid < rid {
			lids = lids[1:]
		} else {
			rids = rids[1:]
		}
	}
	return ids, filters
}

// unionSeriesFilters performs a union for two sets of ids and filter expressions.
func unionSeriesFilters(lids, rids SeriesIDs, lfilters, rfilters FilterExprs) (SeriesIDs, FilterExprs) {
	ids := make([]uint64, 0, len(lids)+len(rids))

	// Setup the filters with the smallest size since we will discard filters
	// that do not have a match on the other side.
	var filters FilterExprs
	if len(lfilters) < len(rfilters) {
		filters = make(FilterExprs, len(lfilters))
	} else {
		filters = make(FilterExprs, len(rfilters))
	}

	for len(lids) > 0 && len(rids) > 0 {
		lid, rid := lids[0], rids[0]
		if lid == rid {
			ids = append(ids, lid)

			// If one side does not have a filter, then the series has been
			// included on one side of the OR with no condition. Eliminate the
			// filter in this case.
			var expr influxql.Expr
			lfilter := lfilters[lid]
			rfilter := rfilters[rid]
			if lfilter != nil && rfilter != nil {
				be := &influxql.BinaryExpr{
					Op:  influxql.OR,
					LHS: lfilter,
					RHS: rfilter,
				}
				expr = influxql.Reduce(be, nil)
			}

			if expr != nil {
				filters[lid] = expr
			}
			lids, rids = lids[1:], rids[1:]
		} else if lid < rid {
			ids = append(ids, lid)

			filter := lfilters[lid]
			if filter != nil {
				filters[lid] = filter
			}
			lids = lids[1:]
		} else {
			ids = append(ids, rid)

			filter := rfilters[rid]
			if filter != nil {
				filters[rid] = filter
			}
			rids = rids[1:]
		}
	}

	// Now append the remainder.
	if len(lids) > 0 {
		for i := 0; i < len(lids); i++ {
			ids = append(ids, lids[i])

			filter := lfilters[lids[i]]
			if filter != nil {
				filters[lids[i]] = filter
			}
		}
	} else if len(rids) > 0 {
		for i := 0; i < len(rids); i++ {
			ids = append(ids, rids[i])

			filter := rfilters[rids[i]]
			if filter != nil {
				filters[rids[i]] = filter
			}
		}
	}
	return ids, filters
}

// idsForExpr will return a collection of series ids and a filter expression that should
// be used to filter points from those series.
func (m *Measurement) idsForExpr(n *influxql.BinaryExpr) (SeriesIDs, influxql.Expr, error) {
	// If this binary expression has another binary expression, then this
	// is some expression math and we should just pass it to the underlying query.
	if _, ok := n.LHS.(*influxql.BinaryExpr); ok {
		return m.seriesIDs, n, nil
	} else if _, ok := n.RHS.(*influxql.BinaryExpr); ok {
		return m.seriesIDs, n, nil
	}

	// Retrieve the variable reference from the correct side of the expression.
	name, ok := n.LHS.(*influxql.VarRef)
	value := n.RHS
	if !ok {
		name, ok = n.RHS.(*influxql.VarRef)
		if !ok {
			return nil, nil, fmt.Errorf("invalid expression: %s", n.String())
		}
		value = n.LHS
	}

	// For time literals, return all series IDs and "true" as the filter.
	if _, ok := value.(*influxql.TimeLiteral); ok || name.Val == "time" {
		return m.seriesIDs, &influxql.BooleanLiteral{Val: true}, nil
	}

	// For fields, return all series IDs from this measurement and return
	// the expression passed in, as the filter.
	if name.Val != "_name" && ((name.Type == influxql.Unknown && m.hasField(name.Val)) || name.Type == influxql.AnyField || (name.Type != influxql.Tag && name.Type != influxql.Unknown)) {
		return m.seriesIDs, n, nil
	} else if value, ok := value.(*influxql.VarRef); ok {
		// Check if the RHS is a variable and if it is a field.
		if value.Val != "_name" && ((value.Type == influxql.Unknown && m.hasField(value.Val)) || name.Type == influxql.AnyField || (value.Type != influxql.Tag && value.Type != influxql.Unknown)) {
			return m.seriesIDs, n, nil
		}
	}

	// Retrieve list of series with this tag key.
	tagVals := m.seriesByTagKeyValue[name.Val]

	// if we're looking for series with a specific tag value
	if str, ok := value.(*influxql.StringLiteral); ok {
		var ids SeriesIDs

		// Special handling for "_name" to match measurement name.
		if name.Val == "_name" {
			if (n.Op == influxql.EQ && str.Val == m.Name) || (n.Op == influxql.NEQ && str.Val != m.Name) {
				return m.seriesIDs, nil, nil
			}
			return nil, nil, nil
		}

		if n.Op == influxql.EQ {
			if str.Val != "" {
				// return series that have a tag of specific value.
				ids = tagVals[str.Val]
			} else {
				// Make a copy of all series ids and mark the ones we need to evict.
				seriesIDs := newEvictSeriesIDs(m.seriesIDs)

				// Go through each slice and mark the values we find as zero so
				// they can be removed later.
				for _, a := range tagVals {
					seriesIDs.mark(a)
				}

				// Make a new slice with only the remaining ids.
				ids = seriesIDs.evict()
			}
		} else if n.Op == influxql.NEQ {
			if str.Val != "" {
				ids = m.seriesIDs.Reject(tagVals[str.Val])
			} else {
				for k := range tagVals {
					ids = append(ids, tagVals[k]...)
				}
				sort.Sort(ids)
			}
		}
		return ids, nil, nil
	}

	// if we're looking for series with a tag value that matches a regex
	if re, ok := value.(*influxql.RegexLiteral); ok {
		var ids SeriesIDs

		// Special handling for "_name" to match measurement name.
		if name.Val == "_name" {
			match := re.Val.MatchString(m.Name)
			if (n.Op == influxql.EQREGEX && match) || (n.Op == influxql.NEQREGEX && !match) {
				return m.seriesIDs, &influxql.BooleanLiteral{Val: true}, nil
			}
			return nil, nil, nil
		}

		// Check if we match the empty string to see if we should include series
		// that are missing the tag.
		empty := re.Val.MatchString("")

		// Gather the series that match the regex. If we should include the empty string,
		// start with the list of all series and reject series that don't match our condition.
		// If we should not include the empty string, include series that match our condition.
		if empty && n.Op == influxql.EQREGEX {
			// See comments above for EQ with a StringLiteral.
			seriesIDs := newEvictSeriesIDs(m.seriesIDs)
			for k := range tagVals {
				if !re.Val.MatchString(k) {
					seriesIDs.mark(tagVals[k])
				}
			}
			ids = seriesIDs.evict()
		} else if empty && n.Op == influxql.NEQREGEX {
			ids = make(SeriesIDs, 0, len(m.seriesIDs))
			for k := range tagVals {
				if !re.Val.MatchString(k) {
					ids = append(ids, tagVals[k]...)
				}
			}
			sort.Sort(ids)
		} else if !empty && n.Op == influxql.EQREGEX {
			ids = make(SeriesIDs, 0, len(m.seriesIDs))
			for k := range tagVals {
				if re.Val.MatchString(k) {
					ids = append(ids, tagVals[k]...)
				}
			}
			sort.Sort(ids)
		} else if !empty && n.Op == influxql.NEQREGEX {
			// See comments above for EQ with a StringLiteral.
			seriesIDs := newEvictSeriesIDs(m.seriesIDs)
			for k := range tagVals {
				if re.Val.MatchString(k) {
					seriesIDs.mark(tagVals[k])
				}
			}
			ids = seriesIDs.evict()
		}
		return ids, nil, nil
	}

	// compare tag values
	if ref, ok := value.(*influxql.VarRef); ok {
		var ids SeriesIDs

		if n.Op == influxql.NEQ {
			ids = m.seriesIDs
		}

		rhsTagVals := m.seriesByTagKeyValue[ref.Val]
		for k := range tagVals {
			tags := tagVals[k].Intersect(rhsTagVals[k])
			if n.Op == influxql.EQ {
				ids = ids.Union(tags)
			} else if n.Op == influxql.NEQ {
				ids = ids.Reject(tags)
			}
		}
		return ids, nil, nil
	}

	if n.Op == influxql.NEQ || n.Op == influxql.NEQREGEX {
		return m.seriesIDs, nil, nil
	}
	return nil, nil, nil
}
*/
