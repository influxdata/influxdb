package models

import (
	"sort"
	"testing"
	"testing/quick"
)

func TestFlatTagsInsertionSort(t *testing.T) {
	f := func(fts flatTags) bool {
		// set up the expected key data:
		gold := []string{}
		for i := 0; i < fts.Len(); i++ {
			k, _ := fts.Get(i)
			gold = append(gold, string(k))
		}
		sort.Strings(gold)

		// perform the work:
		fts.InsertionSort()

		// set up the got key data:
		got := []string{}
		for i := 0; i < fts.Len(); i++ {
			k, _ := fts.Get(i)
			got = append(got, string(k))
		}

		// check that what we want matches what we got:
		if len(gold) != len(got) {
			return false
		}
		for i := range gold {
			if gold[i] != got[i] {
				return false
			}
		}
		return true
	}

	cfg := quick.Config{
		MaxCount: 1000,
	}
	if err := quick.Check(f, &cfg); err != nil {
		t.Fatal(err)
	}
}
