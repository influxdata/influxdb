package plan

type RewriteRule interface {
	Root() ProcedureKind
	Rewrite(*Procedure, PlanRewriter) error
}

var rewriteRules []RewriteRule

func RegisterRewriteRule(r RewriteRule) {
	rewriteRules = append(rewriteRules, r)
}
