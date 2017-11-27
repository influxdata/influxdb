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
	keys []string
}

func newMockTSMFile(keys ...string) *mockTSMFile {
	sort.Strings(keys)
	return &mockTSMFile{keys: keys}
}

func (t *mockTSMFile) KeyCount() int { return len(t.keys) }

func (t *mockTSMFile) Seek(key []byte) int {
	k := string(key)
	return sort.Search(len(t.keys), func(i int) bool {
		return t.keys[i] >= k
	})
}

func (t *mockTSMFile) KeyAt(idx int) ([]byte, byte) {
	return []byte(t.keys[idx]), BlockFloat64
}

func (*mockTSMFile) Path() string                                               { panic("implement me") }
func (*mockTSMFile) Read(key []byte, t int64) ([]Value, error)                  { panic("implement me") }
func (*mockTSMFile) ReadAt(entry *IndexEntry, values []Value) ([]Value, error)  { panic("implement me") }
func (*mockTSMFile) Entries(key []byte) []IndexEntry                            { panic("implement me") }
func (*mockTSMFile) ReadEntries(key []byte, entries *[]IndexEntry) []IndexEntry { panic("implement me") }
func (*mockTSMFile) ContainsValue(key []byte, t int64) bool                     { panic("implement me") }
func (*mockTSMFile) Contains(key []byte) bool                                   { panic("implement me") }
func (*mockTSMFile) OverlapsTimeRange(min, max int64) bool                      { panic("implement me") }
func (*mockTSMFile) OverlapsKeyRange(min, max []byte) bool                      { panic("implement me") }
func (*mockTSMFile) TimeRange() (int64, int64)                                  { panic("implement me") }
func (*mockTSMFile) TombstoneRange(key []byte) []TimeRange                      { panic("implement me") }
func (*mockTSMFile) KeyRange() ([]byte, []byte)                                 { panic("implement me") }
func (*mockTSMFile) Type(key []byte) (byte, error)                              { panic("implement me") }
func (*mockTSMFile) BatchDelete() BatchDeleter                                  { panic("implement me") }
func (*mockTSMFile) Delete(keys [][]byte) error                                 { panic("implement me") }
func (*mockTSMFile) DeleteRange(keys [][]byte, min, max int64) error            { panic("implement me") }
func (*mockTSMFile) HasTombstones() bool                                        { panic("implement me") }
func (*mockTSMFile) TombstoneFiles() []FileStat                                 { panic("implement me") }
func (*mockTSMFile) Close() error                                               { panic("implement me") }
func (*mockTSMFile) Size() uint32                                               { panic("implement me") }
func (*mockTSMFile) Rename(path string) error                                   { panic("implement me") }
func (*mockTSMFile) Remove() error                                              { panic("implement me") }
func (*mockTSMFile) InUse() bool                                                { panic("implement me") }
func (*mockTSMFile) Ref()                                                       { panic("implement me") }
func (*mockTSMFile) Unref()                                                     { panic("implement me") }
func (*mockTSMFile) Stats() FileStat                                            { panic("implement me") }
func (*mockTSMFile) BlockIterator() *BlockIterator                              { panic("implement me") }
func (*mockTSMFile) Free() error                                                { panic("implement me") }

func (*mockTSMFile) ReadFloatBlockAt(*IndexEntry, *[]FloatValue) ([]FloatValue, error) {
	panic("implement me")
}

func (*mockTSMFile) ReadIntegerBlockAt(*IndexEntry, *[]IntegerValue) ([]IntegerValue, error) {
	panic("implement me")
}

func (*mockTSMFile) ReadUnsignedBlockAt(*IndexEntry, *[]UnsignedValue) ([]UnsignedValue, error) {
	panic("implement me")
}

func (*mockTSMFile) ReadStringBlockAt(*IndexEntry, *[]StringValue) ([]StringValue, error) {
	panic("implement me")
}

func (*mockTSMFile) ReadBooleanBlockAt(*IndexEntry, *[]BooleanValue) ([]BooleanValue, error) {
	panic("implement me")
}
