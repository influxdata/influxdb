package models

import "testing"

func TestMarshalPointNoFields(t *testing.T) {
	points, err := ParsePointsString("m,k=v f=0i")
	if err != nil {
		t.Fatal(err)
	}

	// It's unclear how this can ever happen, but we've observed points that were marshalled without any fields.
	points[0].(*point).fields = []byte{}

	if _, err := points[0].MarshalBinary(); err != ErrPointMustHaveAField {
		t.Fatalf("got error %v, exp %v", err, ErrPointMustHaveAField)
	}
}
