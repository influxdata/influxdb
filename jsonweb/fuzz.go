//go:build gofuzz

package jsonweb

// FuzzJsonWeb is the entry point for fuzzing when built with go-fuzz-build.
func FuzzJsonWeb(data []byte) int {
	var keyStore = KeyStoreFunc(func(kid string) ([]byte, error) {
		if kid != "some-key" {
			return nil, ErrKeyNotFound
		}

		return []byte("correct-key"), nil
	})

	parser := NewTokenParser(keyStore)
	if _, err := parser.Parse(string(data)); err != nil {
		// An error here means this input is not interesting
		// to the fuzzer.
		return 0
	}
	// The input valid, and the fuzzer should increase priority
	// along these lines.
	return 1
}
