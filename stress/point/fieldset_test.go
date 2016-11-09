package point

import (
	"reflect"
	"testing"
)

func TestGenerateFieldSet_onlyInts(t *testing.T) {
	ints, floats := generateFieldSet("a=0i,fields=9i")

	if got, exp := len(floats), 0; exp != got {
		t.Errorf("Expected no floats. Got %v, Expected: %v\n", got, exp)
	}

	if got, exp := ints, []string{"a", "fields"}; !reflect.DeepEqual(got, exp) {
		t.Errorf("Wrong integer fields pulled. Got %v, Expected: %v\n", got, exp)
	}
}

func TestGenerateFieldSet_onlyFloats(t *testing.T) {
	ints, floats := generateFieldSet("b=0,things=9")

	if got, exp := len(ints), 0; exp != got {
		t.Errorf("Expected no ints. Got %v, Expected: %v\n", got, exp)
	}

	if got, exp := floats, []string{"b", "things"}; !reflect.DeepEqual(got, exp) {
		t.Errorf("Wrong float fields pulled. Got %v, Expected: %v\n", got, exp)
	}
}

func TestGenerateFieldSet_mixed(t *testing.T) {
	ints, floats := generateFieldSet("a=1i,b=0,fields=92i,things=9")

	if got, exp := ints, []string{"a", "fields"}; !reflect.DeepEqual(got, exp) {
		t.Errorf("Wrong integer fields pulled. Got %v, Expected: %v\n", got, exp)
	}

	if got, exp := floats, []string{"b", "things"}; !reflect.DeepEqual(got, exp) {
		t.Errorf("Wrong float fields pulled. Got %v, Expected: %v\n", got, exp)
	}
}
