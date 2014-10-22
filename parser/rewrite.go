package parser

import "regexp"

type RegexMatcher func(r *regexp.Regexp) []string

// Given a function that returns the series names that match the given
// regex, this function will rewrite the query such that the matching
// serires names are included in the query. E.g. if we have series
// names foobar, foobaz and barbaz and a query
//     select * from merge(/foo.*/)
// the query will be rewritten to
//     select * from merge(foobar, foobaz)
func RewriteMergeQuery(query *SelectQuery, rm RegexMatcher) {
	if query.FromClause.Type != FromClauseMergeRegex {
		return
	}

	series := rm(query.FromClause.Regex)
	f := query.FromClause
	f.Type = FromClauseMerge
	f.Regex = nil
	for _, s := range series {
		f.Names = append(f.Names, &TableName{
			Name: &Value{Name: s, Type: ValueSimpleName},
		})
	}
}
