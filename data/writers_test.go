package data

import (
	"fmt"
	"testing"
)

func TestAckOne(t *testing.T) {
	ap := newAckPolicyN(1)

	if got := ap.IsDone(0, nil); got != true {
		t.Errorf("ack one mismatch: got %v, exp %v", got, true)
	}
}

func TestAckOneError(t *testing.T) {
	ap := newAckPolicyN(1)

	if got := ap.IsDone(0, fmt.Errorf("foo")); got != false {
		t.Errorf("ack one error mismatch: got %v, exp %v", got, false)
	}
}

func TestAckOneMultiple(t *testing.T) {
	ap := newAckPolicyN(1)

	if got := ap.IsDone(0, nil); got != true {
		t.Errorf("ack one error mismatch: got %v, exp %v", got, true)
	}

	if got := ap.IsDone(1, nil); got != true {
		t.Errorf("ack one error mismatch: got %v, exp %v", got, true)
	}
}

func TestAckAll(t *testing.T) {
	ap := newAckPolicyN(3)

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

func TestAckAllError(t *testing.T) {
	ap := newAckPolicyN(3)

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

func TestAckQuorumError(t *testing.T) {
	ap := newAckPolicyN(2)

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

func TestAckOwner(t *testing.T) {
	ap := newAckOwnerPolicy(2)

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

func TestAckOwnerError(t *testing.T) {
	ap := newAckOwnerPolicy(2)

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
