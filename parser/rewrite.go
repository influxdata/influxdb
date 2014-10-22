package parser

import "regexp"

type RegexMatcher func(r *regexp.Regexp) []string

func RewriteMergeQuery(query *SelectQuery, rm RegexMatcher) {
	if query.FromClause.Type != FromClauseMergeFun {
		return
	}

	series := rm(query.FromClause.Regex)
	f := query.FromClause
	f.Type = FromClauseMerge
	f.Regex = nil
	for _, s := range series {
		f.Names = append(f.Names, &TableName{
			Name: &Value{Name: s, Type: ValueTableName},
		})
	}
}
