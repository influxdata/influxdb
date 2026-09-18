use std::collections::HashSet;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{BinaryExpr, LogicalPlan, Operator};
use datafusion::optimizer::utils::NamePreserver;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule, optimizer::ApplyOrder};
use datafusion::prelude::{Expr, binary_expr};
use datafusion::scalar::ScalarValue;
use regex_syntax::hir::{self, Class, Hir, HirKind, Look};

use crate::config::IoxConfigExt;

/// Rewrites anchored, finite-language regex predicates into equality / `IN`
/// list predicates.
///
/// InfluxQL tag predicates such as `tag =~ /^val(1|2|3)$/` are planned as
/// `tag ~ '^val(1|2|3)$'` (see [`InfluxRegexToDataFusionRegex`]). DataFusion's
/// own regex simplifier only handles `^lit$` and `^(lit|lit)$`, so anything
/// more structured stays a regex. When the regex describes a small, finite set
/// of strings, `tag IN ('val1', 'val2', 'val3')` (or a single `=` for one
/// string) tests the same thing more cheaply.
///
/// Patterns are accepted when they are fully anchored (`^...$`, or an
/// alternation whose every branch is fully anchored) and their body consists
/// only of literals, character classes, captures (capturing or `(?:...)`),
/// concatenations, alternations, bounded repetitions and the empty pattern.
/// Unbounded repetitions (`*`, `+`, `{n,}`) and other look-around assertions
/// leave the expression untouched, as does outgrowing any of
/// [`IoxConfigExt::regex_to_in_list_max_entries`],
/// [`IoxConfigExt::regex_to_in_list_max_bytes`] or
/// [`IoxConfigExt::regex_to_in_list_max_repetition`]. The whole rewrite is
/// turned off by [`IoxConfigExt::use_regex_to_in_list`].
///
/// Case-insensitive operators (`~*` / `!~*`) are never rewritten. An inline
/// `(?i)` flag is, however, since the regex parser resolves it into the
/// character classes it case-folds to, which enumerate exactly.
///
/// [`InfluxRegexToDataFusionRegex`]: super::influx_regex_to_datafusion_regex::InfluxRegexToDataFusionRegex
#[derive(Debug, Clone)]
pub struct RegexToInList {}

impl RegexToInList {
    /// Create new optimizer rule.
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for RegexToInList {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizerRule for RegexToInList {
    fn name(&self) -> &str {
        "regex_to_in_list"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        let Some(limits) = Limits::from_config(config) else {
            return Ok(Transformed::no(plan));
        };

        // Inputs have already been rewritten (due to bottom-up traversal handled by
        // Optimizer). Just need to rewrite our own expressions
        let mut expr_rewriter = RegexRewriter { limits };

        let name_preserver = NamePreserver::new(&plan);
        plan.map_expressions(|expr| {
            let saved_name = name_preserver.save(&expr);
            let transformed = expr
                .rewrite(&mut expr_rewriter)?
                .update_data(|expr| saved_name.restore(expr));
            Ok(transformed)
        })
    }
}

/// How large a language may grow, and how much work it may take, before
/// enumerating it is abandoned.
#[derive(Debug, Clone, Copy)]
struct Limits {
    /// The greatest number of strings a language may contain.
    max_entries: usize,
    /// The greatest total size of those strings.
    max_bytes: usize,
    /// The greatest number of times a sub-expression may be repeated.
    max_repetition: usize,
}

impl Limits {
    /// The limits to enumerate within, or `None` if the rewrite is turned off.
    fn from_config(config: &dyn OptimizerConfig) -> Option<Self> {
        let options = config.options();
        let default = IoxConfigExt::default();
        let iox_config = options.extensions.get::<IoxConfigExt>().unwrap_or(&default);
        iox_config.use_regex_to_in_list.then_some(Self {
            max_entries: iox_config.regex_to_in_list_max_entries,
            max_bytes: iox_config.regex_to_in_list_max_bytes,
            max_repetition: iox_config.regex_to_in_list_max_repetition,
        })
    }
}

struct RegexRewriter {
    limits: Limits,
}

impl TreeNodeRewriter for RegexRewriter {
    type Node = Expr;

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>, DataFusionError> {
        let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr else {
            return Ok(Transformed::no(expr));
        };

        let negated = match op {
            Operator::RegexMatch => false,
            Operator::RegexNotMatch => true,
            _ => {
                return Ok(Transformed::no(Expr::BinaryExpr(BinaryExpr {
                    left,
                    op,
                    right,
                })));
            }
        };

        let rewritten = pattern_and_ctor(&right)
            .and_then(|(pattern, ctor)| Some((enumerate_language(pattern, self.limits)?, ctor)));

        let Some((strings, ctor)) = rewritten else {
            return Ok(Transformed::no(Expr::BinaryExpr(BinaryExpr {
                left,
                op,
                right,
            })));
        };

        let mut lits = strings
            .into_iter()
            .map(|s| Expr::Literal(ctor(s), None))
            .collect::<Vec<_>>();

        let expr = if lits.len() == 1 {
            let op = if negated {
                Operator::NotEq
            } else {
                Operator::Eq
            };
            binary_expr(*left, op, lits.remove(0))
        } else {
            left.in_list(lits, negated)
        };

        Ok(Transformed::yes(expr))
    }
}

/// Builds a string literal of a particular string type.
type LiteralCtor = fn(String) -> ScalarValue;

/// Extract the pattern string from a string literal together with a
/// constructor producing literals of the same string type, so that the
/// rewrite does not change the (already coerced) operand types.
fn pattern_and_ctor(expr: &Expr) -> Option<(&str, LiteralCtor)> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _) => Some((s, |s| ScalarValue::Utf8(Some(s)))),
        Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => {
            Some((s, |s| ScalarValue::LargeUtf8(Some(s))))
        }
        Expr::Literal(ScalarValue::Utf8View(Some(s)), _) => {
            Some((s, |s| ScalarValue::Utf8View(Some(s))))
        }
        _ => None,
    }
}

/// Enumerate the finite language of a fully anchored regex pattern.
///
/// Returns `None` if the pattern does not parse, is not fully anchored, uses
/// unsupported constructs, or its language exceeds `limits`. The returned
/// strings are deduplicated and in first-seen order (as laid out by the regex
/// HIR, which e.g. sorts character classes and may factor common prefixes out
/// of alternations).
fn enumerate_language(pattern: &str, limits: Limits) -> Option<Vec<String>> {
    let hir = regex_syntax::parse(pattern).ok()?;
    if !may_be_anchored(&hir) {
        return None;
    }

    // The parser may factor anchors into or out of alternations (e.g. `^a$|^b$`
    // becomes `^(?:a$|b$)`), so anchors are enumerated as positional tokens and
    // every resulting word is validated to be `^ text $`.
    let language = enumerate_hir(&hir, limits)?;
    if language.words.is_empty() {
        return None;
    }

    let mut seen = HashSet::with_capacity(language.words.len());
    let mut deduped = Vec::with_capacity(language.words.len());
    for word in &language.words {
        let text = anchored_text(word)?;
        if seen.insert(text.clone()) {
            deduped.push(text);
        }
    }
    Some(deduped)
}

/// Whether `hir` could describe a fully anchored language: it asserts both `^`
/// and `$` somewhere, and uses no other kind of assertion.
///
/// Enumerating is the expensive part, so this rules out the patterns that
/// cannot possibly qualify before any of it is done. It cannot be the whole
/// test, as whether an anchor falls at the end of every string it matches is
/// only known once the strings are laid out: the parser hoists and distributes
/// anchors, so `^a$|^b$` keeps its `^` at the top but a `$` inside each branch.
fn may_be_anchored(hir: &Hir) -> bool {
    let mut start = false;
    let mut end = false;
    let mut stack = vec![hir];
    while let Some(hir) = stack.pop() {
        match hir.kind() {
            HirKind::Look(Look::Start) => start = true,
            HirKind::Look(Look::End) => end = true,
            // Any other assertion constrains the surrounding text rather than
            // contributing to it, so no string can be read off it.
            HirKind::Look(_) => return false,
            kind => stack.extend(kind.subs()),
        }
    }
    start && end
}

/// One element of an enumerated word: a text fragment or an anchor.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Piece {
    Start,
    End,
    Text(String),
}

/// A single string of the language, as a sequence of pieces.
type Word = Vec<Piece>;

/// Validate that `word` is `^+ text $+` and return `text`.
///
/// Anything else (anchors in the middle, a missing anchor) either matches
/// nothing or is not fully anchored; in both cases the rewrite is abandoned.
fn anchored_text(word: &[Piece]) -> Option<String> {
    let start_len = word.iter().take_while(|p| **p == Piece::Start).count();
    let end_len = word.iter().rev().take_while(|p| **p == Piece::End).count();
    if start_len == 0 || end_len == 0 || start_len + end_len > word.len() {
        return None;
    }

    let mut text = String::new();
    for piece in &word[start_len..word.len() - end_len] {
        match piece {
            Piece::Text(s) => text.push_str(s),
            Piece::Start | Piece::End => return None,
        }
    }
    Some(text)
}

/// The words of a regular expression's language enumerated so far, bounded in
/// both number and total size.
#[derive(Debug, Clone)]
struct Language {
    words: Vec<Word>,
    /// The total size of the text in `words`, tracked so that a language can be
    /// abandoned as soon as it grows too large rather than after building it.
    bytes: usize,
    limits: Limits,
}

impl Language {
    /// The empty language, matching nothing.
    fn none(limits: Limits) -> Self {
        Self {
            words: Vec::new(),
            bytes: 0,
            limits,
        }
    }

    /// The language of just the empty string.
    fn empty_string(limits: Limits) -> Self {
        Self {
            words: vec![Word::new()],
            bytes: 0,
            limits,
        }
    }

    /// Add `word`, or return `None` if the language has outgrown its limits.
    fn push(&mut self, word: Word) -> Option<()> {
        if self.words.len() == self.limits.max_entries {
            return None;
        }
        let bytes = self.bytes.saturating_add(word_size(&word));
        if bytes > self.limits.max_bytes {
            return None;
        }
        self.bytes = bytes;
        self.words.push(word);
        Some(())
    }

    /// Add every word of `other`, or return `None` if the language has outgrown
    /// its limits.
    fn append(&mut self, other: Self) -> Option<()> {
        for word in other.words {
            self.push(word)?;
        }
        Some(())
    }

    /// Follow every word with every word of `suffixes`, or return `None` if the
    /// language has outgrown its limits.
    fn concat(&mut self, suffixes: &Self) -> Option<()> {
        let mut product = Self::none(self.limits);
        for prefix in &self.words {
            for suffix in &suffixes.words {
                let mut word = prefix.clone();
                extend_word(&mut word, suffix);
                product.push(word)?;
            }
        }
        *self = product;
        Some(())
    }
}

/// Append `suffix` to `word`, joining the text either side of the seam.
///
/// Without this a word holds one piece per sub-expression it was built from,
/// so `a{50}` is fifty pieces of one byte rather than one piece of fifty. The
/// pieces are what a word actually costs to hold, but [`word_size`] can only
/// charge for the text in them, so leaving them unjoined lets a pattern take
/// orders of magnitude more memory than [`Limits::max_bytes`] allows for.
fn extend_word(word: &mut Word, suffix: &[Piece]) {
    let joined = match (word.last_mut(), suffix.first()) {
        (Some(Piece::Text(text)), Some(Piece::Text(next))) => {
            text.push_str(next);
            1
        }
        _ => 0,
    };
    word.extend_from_slice(&suffix[joined..]);
}

/// The total size of the text in `word`, ignoring the anchors, which contribute
/// nothing to the string that is matched.
fn word_size(word: &[Piece]) -> usize {
    word.iter()
        .map(|piece| match piece {
            Piece::Text(text) => text.len(),
            Piece::Start | Piece::End => 0,
        })
        .sum()
}

/// Enumerate the language of `hir`, or return `None` if it is infinite, uses a
/// construct that cannot be enumerated, or outgrows `limits`.
fn enumerate_hir(hir: &Hir, limits: Limits) -> Option<Language> {
    match hir.kind() {
        HirKind::Empty => Some(Language::empty_string(limits)),
        HirKind::Literal(literal) => {
            // A literal holds bytes rather than a string, but always whole
            // characters: `regex_syntax::parse` rejects outright any pattern
            // that could match invalid UTF-8, and a common prefix is factored
            // out of an alternation at sub-expression boundaries rather than
            // part way through one. Checked rather than asserted so that a
            // parser configured otherwise would cost a rewrite, not panic.
            let text = std::str::from_utf8(&literal.0).ok()?.to_owned();
            let mut language = Language::none(limits);
            language.push(vec![Piece::Text(text)])?;
            Some(language)
        }
        HirKind::Class(class) => class_language(class, limits),
        HirKind::Look(look @ (Look::Start | Look::End)) => {
            let piece = if *look == Look::Start {
                Piece::Start
            } else {
                Piece::End
            };
            let mut language = Language::none(limits);
            language.push(vec![piece])?;
            Some(language)
        }
        // Any other assertion, such as `\b` or the multi-line `^`, constrains
        // the surrounding text rather than contributing to it.
        HirKind::Look(_) => None,
        HirKind::Capture(capture) => enumerate_hir(&capture.sub, limits),
        HirKind::Concat(parts) => {
            let mut language = Language::empty_string(limits);
            for part in parts {
                language.concat(&enumerate_hir(part, limits)?)?;
            }
            Some(language)
        }
        HirKind::Alternation(alternatives) => {
            let mut language = Language::none(limits);
            for alternative in alternatives {
                language.append(enumerate_hir(alternative, limits)?)?;
            }
            Some(language)
        }
        // A repetition with an upper bound is an alternation over the number of
        // times the sub-expression repeats.
        HirKind::Repetition(hir::Repetition {
            min,
            max: Some(max),
            sub,
            ..
        }) => {
            // Enumerating a repetition costs at least as much as the strings it
            // produces, which grows with the bound even while each string stays
            // short enough to pass the other limits.
            if *max as usize > limits.max_repetition {
                return None;
            }

            let sub = enumerate_hir(sub, limits)?;

            // Each repeat count extends the one before it, rather than being
            // built from nothing: rebuilding would make the work cubic in the
            // bound, which `^a{0,999}$` is large enough to be felt.
            let mut repeated = Language::empty_string(limits);
            for _ in 0..*min {
                repeated.concat(&sub)?;
            }
            let mut language = Language::none(limits);
            for count in *min..=*max {
                if count > *min {
                    repeated.concat(&sub)?;
                }
                language.append(repeated.clone())?;
            }
            Some(language)
        }
        // An unbounded repetition matches an unbounded number of strings.
        HirKind::Repetition(_) => None,
    }
}

/// Each code point of a character class as its own word.
fn class_language(class: &Class, limits: Limits) -> Option<Language> {
    let mut language = Language::none(limits);
    let mut push = |ch: char| language.push(vec![Piece::Text(ch.to_string())]);
    match class {
        Class::Unicode(class) => {
            for range in class.ranges() {
                for ch in range.start()..=range.end() {
                    push(ch)?;
                }
            }
        }
        Class::Bytes(class) => {
            for range in class.ranges() {
                for byte in range.start()..=range.end() {
                    // A non-ASCII byte is not a string on its own.
                    if !byte.is_ascii() {
                        return None;
                    }
                    push(char::from(byte))?;
                }
            }
        }
    }
    Some(language)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::config::ConfigOptions;
    use datafusion::error::Result;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, logical_plan};
    use datafusion::optimizer::{Optimizer, OptimizerContext};
    use datafusion::prelude::{col, lit};

    use super::*;
    use crate::logical_optimizer::register_iox_logical_optimizers;

    /// The number of values the rule rewrites into by default.
    fn default_max_entries() -> usize {
        IoxConfigExt::default().regex_to_in_list_max_entries
    }

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("tag", DataType::Utf8, true),
            Field::new("other", DataType::Utf8, true),
        ])
    }

    fn table_scan() -> Result<LogicalPlan> {
        logical_plan::table_scan(Some("t"), &schema(), None)?.build()
    }

    fn filter_plan(predicate: Expr) -> Result<LogicalPlan> {
        LogicalPlanBuilder::from(table_scan()?)
            .filter(predicate)?
            .build()
    }

    fn optimize_with(plan: LogicalPlan, iox_config: IoxConfigExt) -> Result<LogicalPlan> {
        let mut options = ConfigOptions::default();
        options.extensions.insert(iox_config);
        let optimizer = Optimizer::with_rules(vec![Arc::new(RegexToInList::new())]);
        optimizer.optimize(
            plan,
            &OptimizerContext::new_with_config_options(Arc::new(options)),
            |_, _| {},
        )
    }

    fn optimize(plan: LogicalPlan) -> Result<LogicalPlan> {
        optimize_with(plan, IoxConfigExt::default())
    }

    fn regex(pattern: &str) -> Expr {
        binary_expr(col("tag"), Operator::RegexMatch, lit(pattern))
    }

    fn not_regex(pattern: &str) -> Expr {
        binary_expr(col("tag"), Operator::RegexNotMatch, lit(pattern))
    }

    /// Optimize a filter with the given predicate and return the `Filter:`
    /// line of the optimized plan.
    fn optimized_filter_with(predicate: Expr, iox_config: IoxConfigExt) -> Result<String> {
        let plan = optimize_with(filter_plan(predicate)?, iox_config)?;
        let display = plan.display_indent().to_string();
        Ok(display
            .lines()
            .next()
            .expect("plan has a filter line")
            .to_string())
    }

    fn optimized_filter(predicate: Expr) -> Result<String> {
        optimized_filter_with(predicate, IoxConfigExt::default())
    }

    fn assert_rewritten(predicate: Expr, expected: &str) -> Result<()> {
        assert_eq!(optimized_filter(predicate)?, format!("Filter: {expected}"));
        Ok(())
    }

    fn assert_unchanged_with(predicate: Expr, iox_config: IoxConfigExt) -> Result<()> {
        let plan = filter_plan(predicate)?;
        let before = plan.display_indent().to_string();
        let after = optimize_with(plan, iox_config)?
            .display_indent()
            .to_string();
        assert_eq!(before, after);
        Ok(())
    }

    fn assert_unchanged(predicate: Expr) -> Result<()> {
        assert_unchanged_with(predicate, IoxConfigExt::default())
    }

    fn in_list(items: &[&str]) -> String {
        let items = items
            .iter()
            .map(|s| format!("Utf8(\"{s}\")"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("t.tag IN ([{items}])")
    }

    #[test]
    fn group_alternation_suffix() -> Result<()> {
        assert_rewritten(regex("^val(1|2|3)$"), &in_list(&["val1", "val2", "val3"]))
    }

    #[test]
    fn group_alternation_full() -> Result<()> {
        assert_rewritten(regex("^(val1|val2)$"), &in_list(&["val1", "val2"]))
    }

    #[test]
    fn single_literal_becomes_eq() -> Result<()> {
        assert_rewritten(regex("^val1$"), "t.tag = Utf8(\"val1\")")
    }

    #[test]
    fn single_literal_negated_becomes_not_eq() -> Result<()> {
        assert_rewritten(not_regex("^val1$"), "t.tag != Utf8(\"val1\")")
    }

    #[test]
    fn empty_anchored_pattern() -> Result<()> {
        assert_rewritten(regex("^$"), "t.tag = Utf8(\"\")")
    }

    #[test]
    fn redundant_anchors_rewritten() -> Result<()> {
        assert_rewritten(regex("^^a$$"), "t.tag = Utf8(\"a\")")
    }

    #[test]
    fn two_groups_cartesian_product() -> Result<()> {
        assert_rewritten(
            regex("^val(1|2)(a|b)$"),
            &in_list(&["val1a", "val1b", "val2a", "val2b"]),
        )
    }

    #[test]
    fn class_enumerated() -> Result<()> {
        assert_rewritten(regex("^val[123]$"), &in_list(&["val1", "val2", "val3"]))
    }

    #[test]
    fn class_range_enumerated() -> Result<()> {
        assert_rewritten(
            regex("^val[0-9]$"),
            &in_list(&[
                "val0", "val1", "val2", "val3", "val4", "val5", "val6", "val7", "val8", "val9",
            ]),
        )
    }

    #[test]
    fn top_level_anchored_alternation() -> Result<()> {
        assert_rewritten(regex("^a$|^b$"), &in_list(&["a", "b"]))
    }

    #[test]
    fn factored_alternation_with_shared_suffix() -> Result<()> {
        assert_rewritten(regex("^(a|b)c$|^d$"), &in_list(&["ac", "bc", "d"]))
    }

    #[test]
    fn non_capturing_group() -> Result<()> {
        assert_rewritten(regex("^(?:a|b)$"), &in_list(&["a", "b"]))
    }

    #[test]
    fn nested_groups() -> Result<()> {
        assert_rewritten(regex("^((a|b)c|d)$"), &in_list(&["ac", "bc", "d"]))
    }

    #[test]
    fn empty_alternative_yields_empty_string() -> Result<()> {
        assert_rewritten(regex("^(a|)$"), &in_list(&["a", ""]))
    }

    #[test]
    fn duplicates_removed() -> Result<()> {
        assert_rewritten(regex("^(a|b|a)$"), &in_list(&["a", "b"]))
    }

    #[test]
    fn negated_becomes_not_in() -> Result<()> {
        assert_rewritten(
            not_regex("^val(1|2|3)$"),
            "t.tag NOT IN ([Utf8(\"val1\"), Utf8(\"val2\"), Utf8(\"val3\")])",
        )
    }

    #[test]
    fn multi_byte_characters_rewritten() -> Result<()> {
        // A literal always holds whole characters, so the bytes of one are
        // never split across the strings enumerated from it.
        assert_rewritten(
            regex("^(\u{e9}a|\u{e8}b)$"),
            &in_list(&["\u{e9}a", "\u{e8}b"]),
        )?;
        assert_rewritten(
            regex("^(\u{e9}ab|\u{e9}ac)$"),
            &in_list(&["\u{e9}ab", "\u{e9}ac"]),
        )?;
        // Single characters become a class, which is ordered by code point.
        assert_rewritten(regex("^(\u{e9}|\u{e8})$"), &in_list(&["\u{e8}", "\u{e9}"]))
    }

    #[test]
    fn escaped_metacharacters() -> Result<()> {
        assert_rewritten(
            regex("^a\\.b(\\*|\\$)$"),
            // single-character alternatives become a (sorted) class
            &in_list(&["a.b$", "a.b*"]),
        )
    }

    #[test]
    fn rewrites_inside_larger_expression() -> Result<()> {
        assert_rewritten(
            regex("^val(1|2)$").and(col("other").eq(lit("x"))),
            "t.tag IN ([Utf8(\"val1\"), Utf8(\"val2\")]) AND t.other = Utf8(\"x\")",
        )
    }

    #[test]
    fn preserves_projection_names() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .project(vec![regex("^val(1|2)$")])?
            .build()?;
        let before = plan.schema().field(0).name().to_string();
        let after = optimize(plan)?;
        assert_eq!(after.schema().field(0).name(), &before);
        assert_eq!(
            after.display_indent().to_string().lines().next().unwrap(),
            format!("Projection: t.tag IN ([Utf8(\"val1\"), Utf8(\"val2\")]) AS {before}")
        );
        Ok(())
    }

    #[test]
    fn large_utf8_pattern_keeps_type() -> Result<()> {
        let predicate = binary_expr(
            col("tag"),
            Operator::RegexMatch,
            Expr::Literal(ScalarValue::LargeUtf8(Some("^(a|b)$".into())), None),
        );
        assert_rewritten(predicate, "t.tag IN ([LargeUtf8(\"a\"), LargeUtf8(\"b\")])")
    }

    #[test]
    fn utf8view_pattern_keeps_type() -> Result<()> {
        let predicate = binary_expr(
            col("tag"),
            Operator::RegexMatch,
            Expr::Literal(ScalarValue::Utf8View(Some("^a$".into())), None),
        );
        assert_rewritten(predicate, "t.tag = Utf8View(\"a\")")
    }

    #[test]
    fn bounded_repetition_expanded() -> Result<()> {
        assert_rewritten(regex("^a{1,3}$"), &in_list(&["a", "aa", "aaa"]))?;
        assert_rewritten(regex("^ab?$"), &in_list(&["a", "ab"]))?;
        assert_rewritten(regex("^(ab){2}$"), "t.tag = Utf8(\"abab\")")?;
        assert_rewritten(regex("^a[bc]{2}$"), &in_list(&["abb", "abc", "acb", "acc"]))
    }

    #[test]
    fn inline_case_insensitive_flag_expanded() -> Result<()> {
        // The parser resolves `(?i)` into the classes it case-folds to, which
        // enumerate exactly.
        assert_rewritten(regex("^(?i)ab$"), &in_list(&["AB", "Ab", "aB", "ab"]))?;
        assert_rewritten(regex("^(?i:a)b$"), &in_list(&["Ab", "ab"]))
    }

    #[test]
    fn at_entry_limit_is_rewritten() -> Result<()> {
        // 10 * 10 * 10 = 1000 strings, exactly at the default limit
        let line = optimized_filter(regex("^[0-9][0-9][0-9]$"))?;
        assert!(line.contains(" IN ("), "{line}");
        assert_eq!(line.matches("Utf8(").count(), default_max_entries());
        Ok(())
    }

    #[test]
    fn entry_limit_comes_from_config() -> Result<()> {
        let config = |max_entries| IoxConfigExt {
            regex_to_in_list_max_entries: max_entries,
            ..Default::default()
        };
        assert_eq!(
            optimized_filter_with(regex("^(a|b|c)$"), config(3))?,
            format!("Filter: {}", in_list(&["a", "b", "c"]))
        );
        assert_unchanged_with(regex("^(a|b|c)$"), config(2))
    }

    #[test]
    fn byte_limit_comes_from_config() -> Result<()> {
        let config = |max_bytes| IoxConfigExt {
            regex_to_in_list_max_bytes: max_bytes,
            ..Default::default()
        };
        // Two strings of five bytes each.
        assert_eq!(
            optimized_filter_with(regex("^(alpha|bravo)$"), config(10))?,
            format!("Filter: {}", in_list(&["alpha", "bravo"]))
        );
        assert_unchanged_with(regex("^(alpha|bravo)$"), config(9))
    }

    #[test]
    fn disabled_by_config() -> Result<()> {
        assert_unchanged_with(
            regex("^a$"),
            IoxConfigExt {
                use_regex_to_in_list: false,
                ..Default::default()
            },
        )
    }

    #[test]
    fn repetition_limit_comes_from_config() -> Result<()> {
        let default_max = IoxConfigExt::default().regex_to_in_list_max_repetition;
        // At the default limit the repetition is expanded, one beyond it the
        // pattern is left alone, however few values it would reach.
        let at_limit = format!("^a{{0,{default_max}}}$");
        assert!(optimized_filter(regex(&at_limit))?.contains(" IN ("));
        assert_unchanged(regex(&format!("^a{{0,{}}}$", default_max + 1)))?;

        // A lower limit declines what the default accepts.
        assert_unchanged_with(
            regex(&at_limit),
            IoxConfigExt {
                regex_to_in_list_max_repetition: default_max - 1,
                ..Default::default()
            },
        )
    }

    #[test]
    fn unanchored_not_rewritten() -> Result<()> {
        assert_unchanged(regex("val1"))
    }

    #[test]
    fn start_only_not_rewritten() -> Result<()> {
        assert_unchanged(regex("^val1"))
    }

    #[test]
    fn end_only_not_rewritten() -> Result<()> {
        assert_unchanged(regex("val1$"))
    }

    #[test]
    fn group_without_end_anchor_not_rewritten() -> Result<()> {
        assert_unchanged(regex("^val(1|2)"))
    }

    #[test]
    fn partially_anchored_alternation_not_rewritten() -> Result<()> {
        assert_unchanged(regex("^a$|b"))
    }

    #[test]
    fn anchor_inside_body_not_rewritten() -> Result<()> {
        // matches nothing; leave it to the regex engine rather than guess
        assert_unchanged(regex("^a$b$"))?;
        assert_unchanged(regex("^a^b$"))
    }

    #[test]
    fn wildcard_not_rewritten() -> Result<()> {
        assert_unchanged(regex("^val.*$"))
    }

    #[test]
    fn unbounded_repetition_not_rewritten() -> Result<()> {
        assert_unchanged(regex("^a*$"))?;
        assert_unchanged(regex("^(a|b)+$"))?;
        assert_unchanged(regex("^a{2,}$"))
    }

    #[test]
    fn unicode_shorthand_class_expands_beyond_its_ascii_subset() -> Result<()> {
        // `\d` is Unicode-aware and matches every `Nd` code point, so it
        // expands to hundreds of values rather than the ten ASCII digits.
        let line = optimized_filter(regex("^val\\d$"))?;
        assert!(line.matches("Utf8(").count() > 10, "{line}");
        assert_eq!(
            optimized_filter(regex("^val[0-9]$"))?
                .matches("Utf8(")
                .count(),
            10
        );
        Ok(())
    }

    #[test]
    fn class_over_limit_not_rewritten() -> Result<()> {
        // 2048 code points
        assert_unchanged(regex("^[\\x{0}-\\x{7ff}]$"))
    }

    #[test]
    fn product_over_limit_not_rewritten() -> Result<()> {
        // 8 * 8 * 8 * 2 = 1024 strings
        assert_unchanged(regex("^[a-h][a-h][a-h](x|y)$"))
    }

    #[test]
    fn repetition_over_limit_not_rewritten() -> Result<()> {
        // 36 * 36 = 1296 strings
        assert_unchanged(regex("^val[0-9a-z]{2}$"))
    }

    #[test]
    fn alternation_over_limit_not_rewritten() -> Result<()> {
        // 1000 + 2 = 1002 strings
        assert_unchanged(regex("^[0-9][0-9][0-9]$|^(aa|bb)$"))
    }

    #[test]
    fn case_insensitive_operator_not_rewritten() -> Result<()> {
        assert_unchanged(binary_expr(col("tag"), Operator::RegexIMatch, lit("^a$")))
    }

    #[test]
    fn word_boundary_not_rewritten() -> Result<()> {
        assert_unchanged(regex("^\\ba$"))
    }

    #[test]
    fn multi_line_anchors_not_rewritten() -> Result<()> {
        // `(?m)` makes `^` and `$` match at a line break rather than only at
        // the ends of the value, so the language is not the anchored one.
        assert_unchanged(regex("(?m)^a$"))
    }

    #[test]
    fn non_literal_pattern_not_rewritten() -> Result<()> {
        assert_unchanged(binary_expr(col("tag"), Operator::RegexMatch, col("other")))
    }

    #[test]
    fn invalid_regex_not_rewritten() -> Result<()> {
        assert_unchanged(regex("^a($"))
    }

    #[test]
    fn wide_repetition_bound_expanded_incrementally() {
        // Each repeat count extends the one before it, so the work stays
        // proportional to the strings produced. Rebuilding each count from
        // nothing made this cubic in the bound, and took seconds.
        let limits = Limits {
            max_repetition: 999,
            ..Limits::from_config(&OptimizerContext::new()).expect("enabled")
        };
        let language = enumerate_language("^a{0,999}$", limits).expect("expanded");
        assert_eq!(language.len(), 1000);
        assert_eq!(language[0], "");
        assert_eq!(language[999], "a".repeat(999));
    }

    #[test]
    fn extend_word_joins_adjacent_text() {
        let text = |s: &str| Piece::Text(s.to_owned());

        // Text either side of the seam becomes one piece, so a word costs what
        // `word_size` charges for it.
        let mut word = vec![Piece::Start, text("a")];
        extend_word(&mut word, &[text("b"), Piece::End]);
        assert_eq!(word, vec![Piece::Start, text("ab"), Piece::End]);

        // An anchor keeps text apart, as it must.
        let mut word = vec![text("a"), Piece::End];
        extend_word(&mut word, &[text("b")]);
        assert_eq!(word, vec![text("a"), Piece::End, text("b")]);
    }

    #[test]
    fn nested_repetition_declined_without_building_it() -> Result<()> {
        // Nested repetitions multiply out to far more text than the byte limit
        // allows. Each is within `regex_to_in_list_max_repetition`, so they are
        // only stopped by the text they produce being counted honestly.
        assert_unchanged(regex("^(?:(?:(?:(?:(?:a{10}){50}){50}){50}){50})$"))?;
        assert_unchanged(regex("^(?:(?:(?:(?:a{50}){50}){50}){50}){50}$"))
    }

    /// Every string of the enumerated language, and no other, must be one the
    /// regex engine matches. This holds only because the enumeration and the
    /// engine share a parser; it would catch them drifting apart.
    #[test]
    fn enumeration_agrees_with_the_regex_engine() {
        let limits = Limits::from_config(&OptimizerContext::new()).unwrap();

        // Nothing enumerated may fail to match.
        for pattern in [
            "^val(1|2|3)$",
            "^val[0-9]$",
            "^a{1,3}$",
            "^ab?$",
            "^(ab){2}$",
            "^a[bc]{2}$",
            "^(?i)ab$",
            "^(a|)$",
            "^$",
            "^(\u{e9}|\u{e8})$",
            r"^a\.b$",
            "^a$|^b$",
            "^((a|b)c|d)$",
            "^\u{e9}ab$",
        ] {
            let strings = enumerate_language(pattern, limits).expect(pattern);
            let regex = regex::Regex::new(pattern).expect(pattern);
            for string in &strings {
                assert!(regex.is_match(string), "{pattern} should match {string:?}");
            }
        }

        // And nothing may be missed: exhaustively, over a small alphabet.
        let mut candidates = vec![String::new()];
        let mut longest = vec![String::new()];
        for _ in 0..4 {
            longest = longest
                .iter()
                .flat_map(|s| ["a", "b", "c", "A", "B"].map(|c| format!("{s}{c}")))
                .collect();
            candidates.extend(longest.iter().cloned());
        }
        for pattern in [
            "^a{1,3}$",
            "^ab?$",
            "^(ab){2}$",
            "^a[bc]{2}$",
            "^(a|)$",
            "^$",
            "^(a|b|a)$",
            "^(?i)ab$",
            "^a$|^b$",
            "^(?:a|b)c$",
        ] {
            let strings = enumerate_language(pattern, limits).expect(pattern);
            let regex = regex::Regex::new(pattern).expect(pattern);
            for candidate in &candidates {
                assert_eq!(
                    regex.is_match(candidate),
                    strings.contains(candidate),
                    "{pattern} disagrees on {candidate:?}"
                );
            }
        }
    }

    #[test]
    fn may_be_anchored_declines_before_enumerating() {
        let anchored = |pattern: &str| may_be_anchored(&regex_syntax::parse(pattern).unwrap());

        assert!(anchored("^a$"));
        // The anchors need not sit together: the parser keeps this one's `^` at
        // the top and puts a `$` inside each branch.
        assert!(anchored("^a$|^b$"));

        assert!(!anchored("a"));
        assert!(!anchored("^a"));
        assert!(!anchored("a$"));
        // Another kind of assertion cannot be enumerated at all.
        assert!(!anchored("^\\ba$"));
        // A pattern whose language would be large is declined without building
        // any of it, which is the point of checking this first.
        assert!(!anchored("^[0-9][0-9][0-9]"));

        // Only a necessary condition, though: an anchor in the middle is left
        // for `anchored_text` to reject once the strings are laid out.
        assert!(anchored("^a$b$"));
        assert_eq!(
            enumerate_language(
                "^a$b$",
                Limits::from_config(&OptimizerContext::new()).unwrap()
            ),
            None
        );
    }

    /// Run the full IOx logical optimizer chain (DataFusion defaults plus the
    /// IOx rules) over a dictionary-encoded tag column, as produced by real
    /// InfluxQL / SQL planning, and check that the regex becomes a
    /// dictionary-typed equality disjunction pushed into the scan, with no
    /// lingering `CAST`.
    #[test]
    fn full_optimizer_chain_dictionary_column() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new(
                "tag",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new("f", DataType::Float64, true),
        ]);
        let plan = logical_plan::table_scan(Some("t"), &schema, None)?
            .filter(query_functions::regex_match_expr(
                col("tag"),
                "^val(1|2|3)$".to_owned(),
            ))?
            .build()?;

        let state =
            register_iox_logical_optimizers(SessionStateBuilder::new().with_default_features())
                .build();
        let optimized = state.optimize(&plan)?;
        let display = optimized.display_indent().to_string();

        assert_eq!(
            display,
            "TableScan: t projection=[tag, f], full_filters=[\
             t.tag = Dictionary(Int32, Utf8(\"val1\")) OR \
             t.tag = Dictionary(Int32, Utf8(\"val2\")) OR \
             t.tag = Dictionary(Int32, Utf8(\"val3\"))]",
        );
        Ok(())
    }
}
