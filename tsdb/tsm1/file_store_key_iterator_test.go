package tsm1

import (
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestNewMergeKeyIterator(t *testing.T) {
	cases := []struct {
		name  string
		seek  string
		files []TSMFile

		exp []string
	}{
		{
			name: "mixed",
			files: newTSMFiles(
				[]string{"aaaa", "bbbb", "cccc", "dddd"},
				[]string{"aaaa", "cccc", "dddd"},
				[]string{"eeee", "ffff", "gggg"},
				[]string{"aaaa"},
				[]string{"dddd"},
			),
			exp: []string{"aaaa", "bbbb", "cccc", "dddd", "eeee", "ffff", "gggg"},
		},

		{
			name: "similar keys",
			files: newTSMFiles(
				[]string{"a", "aaa"},
				[]string{"aa", "aaaa"},
			),
			exp: []string{"a", "aa", "aaa", "aaaa"},
		},

		{
			name: "seek skips some files",
			seek: "eeee",
			files: newTSMFiles(
				[]string{"aaaa", "bbbb", "cccc", "dddd"},
				[]string{"aaaa", "cccc", "dddd"},
				[]string{"eeee", "ffff", "gggg"},
				[]string{"aaaa"},
				[]string{"dddd"},
			),
			exp: []string{"eeee", "ffff", "gggg"},
		},

		{
			name: "keys same across all files",
			files: newTSMFiles(
				[]string{"aaaa", "bbbb", "cccc", "dddd"},
				[]string{"aaaa", "bbbb", "cccc", "dddd"},
				[]string{"aaaa", "bbbb", "cccc", "dddd"},
			),
			exp: []string{"aaaa", "bbbb", "cccc", "dddd"},
		},

		{
			name: "keys same across all files with extra",
			files: newTSMFiles(
				[]string{"aaaa", "bbbb", "cccc", "dddd"},
				[]string{"aaaa", "bbbb", "cccc", "dddd"},
				[]string{"aaaa", "bbbb", "cccc", "dddd", "eeee"},
			),
			exp: []string{"aaaa", "bbbb", "cccc", "dddd", "eeee"},
		},

		{
			name: "seek skips all files",
			seek: "eeee",
			files: newTSMFiles(
				[]string{"aaaa", "bbbb", "cccc", "dddd"},
				[]string{"aaaa", "bbbb", "cccc", "dddd"},
				[]string{"aaaa", "bbbb", "cccc", "dddd"},
			),
			exp: nil,
		},

		{
			name: "keys sequential across all files",
			files: newTSMFiles(
				[]string{"a", "b", "c", "d"},
				[]string{"e", "f", "g", "h"},
				[]string{"i", "j", "k", "l"},
			),
			exp: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"},
		},

		{
			name: "seek past one file",
			seek: "e",
			files: newTSMFiles(
				[]string{"a", "b", "c", "d"},
				[]string{"e", "f", "g", "h"},
				[]string{"i", "j", "k", "l"},
			),
			exp: []string{"e", "f", "g", "h", "i", "j", "k", "l"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ki := newMergeKeyIterator(tc.files, []byte(tc.seek))
			var act []string
			for ki.Next() {
				key, _ := ki.Read()
				act = append(act, string(key))
			}
			if !cmp.Equal(tc.exp, act) {
				t.Error(cmp.Diff(tc.exp, act))
			}
		})
	}

}

func newTSMFiles(keys ...[]string) []TSMFile {
	var files []TSMFile
	for _, k := range keys {
		files = append(files, newMockTSMFile(k...))
	}
	return files
}

type mockTSMFile struct {
	TSMFile
	keys []string
}

func newMockTSMFile(keys ...string) *mockTSMFile {
	sort.Strings(keys)
	return &mockTSMFile{keys: keys}
}

func (m *mockTSMFile) Iterator(seek []byte) TSMIterator {
	skey := string(seek)
	n := sort.Search(len(m.keys), func(i int) bool { return m.keys[i] >= skey })
	return &mockTSMIterator{
		n:    n - 1,
		keys: m.keys,
	}
}

type mockTSMIterator struct {
	TSMIndexIterator
	n    int
	keys []string
}

func (m *mockTSMIterator) Next() bool {
	m.n++
	return m.n < len(m.keys)
}

func (m *mockTSMIterator) Key() []byte { return []byte(m.keys[m.n]) }
func (m *mockTSMIterator) Type() byte  { return 0 }
