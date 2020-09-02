package gota

import (
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func diffFloats(exp, act []float64, delta float64) string {
	return cmp.Diff(exp, act, cmpopts.EquateApprox(0, delta))
}
