package platform

// Owner represents a resource owner
type Owner struct {
	ID ID
}

// Decode parses b as a hex-encoded byte-slice-string.
func (o *Owner) Decode(b []byte) error {
	var id ID
	if err := id.Decode(b); err != nil {
		return err
	}
	o.ID = id
	return nil
}
