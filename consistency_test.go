package influxdb

import (
	"fmt"
	"testing"
)

func TestConsistencyOne(t *testing.T) {
	ap := newConsistencyPolicyN(1)

	if got := ap.IsDone(0, nil); got != true {
		t.Errorf("ack one mismatch: got %v, exp %v", got, true)
	}
}

func TestConsistencyOneError(t *testing.T) {
	ap := newConsistencyPolicyN(1)

	if got := ap.IsDone(0, fmt.Errorf("foo")); got != false {
		t.Errorf("ack one error mismatch: got %v, exp %v", got, false)
	}
}

func TestConsistencyOneMultiple(t *testing.T) {
	ap := newConsistencyPolicyN(1)

	if got := ap.IsDone(0, nil); got != true {
		t.Errorf("ack one error mismatch: got %v, exp %v", got, true)
	}

	if got := ap.IsDone(1, nil); got != true {
		t.Errorf("ack one error mismatch: got %v, exp %v", got, true)
	}
}

func TestConsistencyAll(t *testing.T) {
	ap := newConsistencyPolicyN(3)

	if got := ap.IsDone(0, nil); got != false {
		t.Errorf("ack one error mismatch: got %v, exp %v", got, false)
	}

	if got := ap.IsDone(1, nil); got != false {
		t.Errorf("ack one error mismatch: got %v, exp %v", got, false)
	}

	if got := ap.IsDone(2, nil); got != true {
		t.Errorf("ack one error mismatch: got %v, exp %v", got, true)
	}
}

func TestConsistencyAllError(t *testing.T) {
	ap := newConsistencyPolicyN(3)

	if got := ap.IsDone(0, nil); got != false {
		t.Errorf("ack one error mismatch: got %v, exp %v", got, false)
	}

	if got := ap.IsDone(1, fmt.Errorf("foo")); got != false {
		t.Errorf("ack one error mismatch: got %v, exp %v", got, false)
	}

	if got := ap.IsDone(2, nil); got != false {
		t.Errorf("ack one error mismatch: got %v, exp %v", got, true)
	}
}

func TestConsistencyQuorumError(t *testing.T) {
	ap := newConsistencyPolicyN(2)

	if got := ap.IsDone(0, nil); got != false {
		t.Errorf("ack one error mismatch: got %v, exp %v", got, false)
	}

	if got := ap.IsDone(1, fmt.Errorf("foo")); got != false {
		t.Errorf("ack one error mismatch: got %v, exp %v", got, false)
	}

	if got := ap.IsDone(2, nil); got != true {
		t.Errorf("ack one error mismatch: got %v, exp %v", got, true)
	}
}

func TestConsistencyOwner(t *testing.T) {
	ap := newConsistencyOwnerPolicy(2)

	// non-owner, not done
	if got := ap.IsDone(0, nil); got != false {
		t.Errorf("ack one error mismatch: got %v, exp %v", got, false)
	}

	// non-owner, not done
	if got := ap.IsDone(1, nil); got != false {
		t.Errorf("ack one error mismatch: got %v, exp %v", got, false)
	}

	// owner, should be done
	if got := ap.IsDone(2, nil); got != true {
		t.Errorf("ack one error mismatch: got %v, exp %v", got, true)
	}
}

func TestConsistencyOwnerError(t *testing.T) {
	ap := newConsistencyOwnerPolicy(2)

	// non-owner succeeds, should not be done
	if got := ap.IsDone(0, nil); got != false {
		t.Errorf("ack one error mismatch: got %v, exp %v", got, false)
	}

	// non-owner failed, should not be done
	if got := ap.IsDone(1, fmt.Errorf("foo")); got != false {
		t.Errorf("ack one error mismatch: got %v, exp %v", got, false)
	}

	// owner failed
	if got := ap.IsDone(2, fmt.Errorf("foo")); got != true {
		t.Errorf("ack one error mismatch: got %v, exp %v", got, true)
	}
}
