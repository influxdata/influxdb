package platform

import "encoding/hex"

// Owner represents a resource owner
type Owner struct {
	ID ID
}

// Decode parses b as a hex-encoded byte-slice-string.
func (o *Owner) Decode(b []byte) error {
	dst := make([]byte, hex.DecodedLen(len(b)))
	_, err := hex.Decode(dst, b)
	if err != nil {
		return err
	}
	o.ID = dst
	return nil
}
