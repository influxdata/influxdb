package point

import (
	"math"
	"testing"
)

func product(m map[int]int) int {
	p := 1

	for k, v := range m {
		p *= int(math.Pow(float64(k), float64(v)))
	}

	return p
}

func TestPrimeFactorization(t *testing.T) {
	ns := []int{1232, 14, 135432, 4312, 5321, 3, 53, 15}

	for _, n := range ns {
		if got, exp := product(primeFactorization(n)), n; exp != got {
			t.Errorf("Factorization was off. Got %v Expected %v\n", got, exp)
		}
	}
}

func mult(xs []int) int {
	p := 1
	for _, x := range xs {
		p *= x
	}

	return p
}

func gcd(x, y int) int {
	if x < y {
		return gcd(y, x)
	}

	if y == 0 {
		return x
	}

	if y == 1 {
		return 1
	}

	return gcd(y, x%y)

}

func coprime(xs []int) bool {
	for i, x := range xs {
		for j, y := range xs {
			if j <= i {
				continue
			}
			if gcd(x, y) != 1 {
				return false
			}
		}
	}

	return true
}

func TestTagCardinalityPartition(t *testing.T) {
	ns := []int{1232, 14, 135432, 4312, 5321, 3, 53, 15}
	tagNs := []int{1, 2, 3, 4, 5}

	for _, n := range ns {
		for _, tagN := range tagNs {
			pf := primeFactorization(n)
			tcp := tagCardinalityPartition(tagN, pf)
			if got, exp := mult(tcp), n; exp != got {
				t.Errorf("Partition was off with %v tags. Got %v Expected %v\n", tagN, got, exp)
			}

			if !coprime(tcp) {
				t.Errorf("Elements of Partition were not coprime with %v tags. %v\n", tagN, tcp)
			}
		}
	}

}
