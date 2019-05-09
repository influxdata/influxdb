package reads_test

import (
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
)

type mockStringValuesStream struct {
	responsesSent []*datatypes.StringValuesResponse
}

func (s *mockStringValuesStream) Send(response *datatypes.StringValuesResponse) error {
	responseCopy := &datatypes.StringValuesResponse{
		Values: make([][]byte, len(response.Values)),
	}
	for i := range response.Values {
		responseCopy.Values[i] = response.Values[i]
	}
	s.responsesSent = append(s.responsesSent, responseCopy)
	return nil
}

func TestStringIteratorWriter(t *testing.T) {
	mockStream := &mockStringValuesStream{}
	w := reads.NewStringIteratorWriter(mockStream)
	si := newMockStringIterator(1, 2, "foo", "bar")
	err := w.WriteStringIterator(si)
	if err != nil {
		t.Fatal(err)
	}
	w.Flush()

	var got []string
	for _, response := range mockStream.responsesSent {
		for _, v := range response.Values {
			got = append(got, string(v))
		}
	}

	expect := []string{"foo", "bar"}
	if !reflect.DeepEqual(expect, got) {
		t.Errorf("expected %v got %v", expect, got)
	}
}

func TestStringIteratorWriter_Nil(t *testing.T) {
	w := reads.NewStringIteratorWriter(&mockStringValuesStream{})
	err := w.WriteStringIterator(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	w.Flush()
}
