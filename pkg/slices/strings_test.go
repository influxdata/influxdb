package slices

import "testing"

func TestExists(t *testing.T) {
	tests := []struct {
		set    []string
		find   string
		output bool
	}{
		{
			set:    []string{},
			find:   "foo",
			output: false,
		},
		{
			set:    []string{"foo"},
			find:   "foo",
			output: true,
		},
		{
			set:    []string{"bar", "foo"},
			find:   "foo",
			output: true,
		},
		{
			set:    []string{"bar", "foo"},
			find:   "stuff",
			output: false,
		},
		{
			set:    []string{"bar", "Foo"},
			find:   "foo",
			output: false,
		},
	}
	for i, tt := range tests {
		actual := Exists(tt.set, tt.find)
		if actual != tt.output {
			t.Errorf("[%d] set: %v , find: %s , expected: %t , actual: %t", i, tt.set, tt.find, tt.output, actual)
		}
	}
}

func TestExistsIgnoreCase(t *testing.T) {
	tests := []struct {
		set    []string
		find   string
		output bool
	}{
		{
			set:    []string{},
			find:   "foo",
			output: false,
		},
		{
			set:    []string{"foo"},
			find:   "foo",
			output: true,
		},
		{
			set:    []string{"bar", "foo"},
			find:   "foo",
			output: true,
		},
		{
			set:    []string{"bar", "foo"},
			find:   "stuff",
			output: false,
		},
		{
			set:    []string{"bar", "Foo"},
			find:   "foo",
			output: true,
		},
	}
	for i, tt := range tests {
		actual := ExistsIgnoreCase(tt.set, tt.find)
		if actual != tt.output {
			t.Errorf("[%d] set: %v , find: %s , expected: %t , actual: %t", i, tt.set, tt.find, tt.output, actual)
		}
	}
}
