package point

import (
	"fmt"
	"strings"
)

func primeFactorization(n int) (factors map[int]int) {
	factors = make(map[int]int, 0)
	if n == 1 {
		factors[n] = 1
		return
	}

	i := 2
	for n != 1 {

		if n%i == 0 {
			n = n / i
			factors[i] += 1
		} else {
			i++
		}

	}

	return
}

func tagCardinalityPartition(numTags int, factors map[int]int) []int {
	buckets := make([]int, numTags)

	for i := range buckets {
		buckets[i] = 1
	}

	i := 0
	for factor := range factors {
		for {
			i++
			count := factors[factor]
			if count == 0 {
				i--
				break
			} else {
				buckets[i%numTags] *= factor
				factors[factor] = count - 1
			}
		}
	}

	return buckets
}

func generateSeriesKeys(tmplt string, card int) [][]byte {
	fmtTmplt, numTags := formatTemplate(tmplt)
	tagCardinalities := tagCardinalityPartition(numTags, primeFactorization(card))

	series := [][]byte{}

	for i := 0; i < card; i++ {
		mods := sliceMod(i, tagCardinalities)
		series = append(series, []byte(fmt.Sprintf(fmtTmplt, mods...)))
	}

	return series
}

func formatTemplate(s string) (string, int) {
	parts := strings.Split(s, ",")
	numTags := len(parts) - 1

	for i, part := range parts {
		if i == 0 {
			continue
		}
		parts[i] = part + "-%v"
	}

	return strings.Join(parts, ","), numTags
}

func sliceMod(m int, mods []int) []interface{} {
	ms := []interface{}{}
	for _, mod := range mods {
		ms = append(ms, m%mod)
	}

	return ms
}
