// Package checktest holds the helpers shared by tests that compare two
// renderings of a /health or /ready document.
//
// It is a package rather than a helper per test package because the callers --
// kit/check, http, and cmd/influxd/launcher -- each need the same thing and
// cannot share a _test.go file across package boundaries. The thing they need
// is not incidental: two renderings of the same frozen report are equal only
// once the values that move on their own have been masked, and a caller that
// gets that set wrong writes an equality assertion that is flaky rather than
// one that fails.
package checktest

import (
	"encoding/json"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The fields of a rendered check document whose values move on their own, and
// the sentinels Normalize puts in their place.
const (
	// FieldStarted and FieldUp belong to /ready alone (the http package's
	// readyBody). started is fixed at handler construction; up is the elapsed
	// time since, recomputed per request, so it advances even across a frozen
	// check set. /health carries neither.
	FieldStarted = "started"
	FieldUp      = "up"

	// FieldMessage is common to both, and moves only while it carries a
	// staleness message; see StaleMessagePattern.
	FieldMessage = "message"

	SentinelStarted = "<started>"
	SentinelUp      = "<up>"
	SentinelStale   = "<stale>"
)

// StaleMessagePattern matches the message a check.FreshnessResponse renders
// once its snapshot has aged out. The age in it advances between two renders of
// the same response, so a test comparing two renders must mask it. Tracks
// check.staleMessage; a change to that format belongs here too.
var StaleMessagePattern = regexp.MustCompile(`^stale: last probe .* ago \(threshold .*\)$`)

// Normalize replaces every value in a decoded check document that moves on its
// own with a fixed sentinel, and returns doc, so two captures taken a moment
// apart compare equal on everything they actually pin.
//
// Each field is checked for shape before it is masked, so a normalized document
// still fails a comparison if one of them changes type -- masking must not
// become a way to stop noticing. Masking is conditional on presence, because
// /health carries neither started nor up, and a message is masked only while it
// matches StaleMessagePattern: every other message is content a caller is
// probably asserting on, and blanking it would quietly gut the assertion.
func Normalize(t testing.TB, doc map[string]any) map[string]any {
	t.Helper()

	if v, ok := doc[FieldStarted]; ok {
		s, isString := v.(string)
		require.Truef(t, isString, "%s must be a string: %v", FieldStarted, v)
		_, err := time.Parse(time.RFC3339Nano, s)
		require.NoErrorf(t, err, "%s must be an RFC3339 timestamp: %q", FieldStarted, s)
		doc[FieldStarted] = SentinelStarted
	}
	if v, ok := doc[FieldUp]; ok {
		_, isString := v.(string)
		require.Truef(t, isString, "%s must be a string: %v", FieldUp, v)
		doc[FieldUp] = SentinelUp
	}
	if msg, ok := doc[FieldMessage].(string); ok && StaleMessagePattern.MatchString(msg) {
		doc[FieldMessage] = SentinelStale
	}
	return doc
}

// NormalizeJSON decodes a rendered check document and normalizes it.
func NormalizeJSON(t testing.TB, b []byte) map[string]any {
	t.Helper()
	var doc map[string]any
	require.NoErrorf(t, json.Unmarshal(b, &doc), "body: %s", b)
	return Normalize(t, doc)
}
