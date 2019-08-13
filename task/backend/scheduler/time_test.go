package scheduler

import (
	"testing"
	"time"
)

func TestStdTime_Now(t *testing.T) {
	t1 := stdTime{}.Now()
	time.Sleep(time.Nanosecond)
	t2 := stdTime{}.Now()
	if !t1.Before(t2) {
		t.Fatal()
	}
}

func TestStdTime_Unix(t *testing.T) {
	now := time.Now()
	t1 := stdTime{}.Unix(now.Unix(), int64(now.Nanosecond()))
	if !t1.Equal(now) {
		t.Fatal("expected the two times to be equivalent but they were not")
	}
}

func TestMockTimer(t *testing.T) {
	timeForComparison := time.Now() //time.Date(2016, 2, 3, 4, 5, 6, 7, time.UTC)
	mt := NewMockTime(timeForComparison)
	timer := mt.NewTimer(10 * time.Second)
	select {
	case <-timer.C():
		t.Fatalf("expected timer not to fire till time was up, but did")
	default:
	}
	go mt.Set(timeForComparison.Add(10 * time.Second))
	select {
	case <-timer.C():
	case <-time.After(3 * time.Second):
		t.Fatal("expected timer to fire when time was up, but it didn't, it fired after a 3 second timeout")
	}
	timer.Reset(33 * time.Second)
	go mt.Set(timeForComparison.Add(50 * time.Second))
	select {
	case <-timer.C():
	case <-time.After(4 * time.Second):
		t.Fatal("expected timer to fire when time was up, but it didn't, it fired after a 4 second timeout")
	}
	if !timer.Stop() {
		<-timer.C()
	}
	timer.Reset(10000 * time.Second)
	select {
	case ts := <-timer.C():
		t.Errorf("expected timer to NOT fire if time was not up, but it did at ts: %s", ts)
	default:
	}

	timer2 := mt.NewTimer(10000 * time.Second)
	select {
	case ts := <-timer2.C():
		t.Errorf("expected timer to NOT fire if time was not up, but it did at ts: %s", ts)
	case <-time.After(4 * time.Second):
	}

	if !timer2.Stop() {
		<-timer2.C()
	}
	timer2.Reset(0)
	select {
	case <-time.After(4 * time.Second):
		t.Error("expected timer to fire when it was reset to 0, but it didn't")
	case <-timer2.C():
	}

	if !timer2.Stop() {
		<-timer2.C()
	}
	timer2.Reset(-time.Second)
	select {
	case <-time.After(4 * time.Second):
		t.Error("expected timer to fire when it was reset to a negative duration, but it didn't")
	case <-timer2.C():
	}

	if !timer2.Stop() {
		<-timer2.C()
	}
	timer2.Reset(1 * time.Second)
	go func() {
		mt.Set(mt.T.Add(1 * time.Second))
	}()
	select {
	case <-time.After(4 * time.Second):
		t.Error("expected timer to fire after it was reset to a small duration, but it didn't")
	case <-timer2.C():
	}

	timer2.Reset(1 * time.Second)
	go func() {
		mt.Set(mt.T.Add(time.Second / 2))
	}()
	select {
	case <-time.After(time.Second):
	case <-timer2.C():
		t.Error("expected timer to not fire after it was reset to a too small duration, but it did")

	}

}

func TestMockTimer_Stop(t *testing.T) {
	timeForComparison := time.Date(2016, 2, 3, 4, 5, 6, 7, time.UTC)
	mt := NewMockTime(timeForComparison)
	timer := mt.NewTimer(10 * time.Second)
	if !timer.Stop() {
		t.Fatal("expected MockTimer.Stop() to be true  if it hadn't fired yet")
	}

	if !timer.Stop() {
		select {
		case <-timer.C():
		case <-time.After(2 * time.Second):
			t.Fatal("timer didn't fire to clear when it should have")
		}
	} else {
		t.Fatalf("Expected MockTimer.Stop() to be false when it was already stopped but it wasn't")
	}

	timer.Reset(time.Second)
	go mt.Set(timeForComparison.Add(20 * time.Second))
	select {
	case <-timer.C():
	case <-time.After(2 * time.Second):
		t.Fatal("timer didn't fire when it should have")
	}
	if !timer.Stop() {
		select {
		case <-timer.C():
		case <-time.After(2 * time.Second):
			t.Fatal("timer didn't fire to clear when it should have")
		}
	} else {
		t.Fatalf("Expected MockTimer.Stop() to be false when it was already fired but it wasn't")
	}
}

func TestMockTime_Until(t *testing.T) {
	tests := []struct {
		name     string
		mocktime time.Time
		ts       time.Time
		want     time.Duration
	}{{
		name:     "happy",
		mocktime: time.Date(2016, 2, 3, 4, 5, 6, 7, time.UTC),
		ts:       time.Date(2016, 2, 3, 4, 7, 56, 11, time.UTC),
		want:     2*time.Minute + 50*time.Second + 4*time.Nanosecond,
	}, {
		name:     "negative",
		mocktime: time.Date(2016, 2, 3, 4, 7, 56, 11, time.UTC),
		ts:       time.Date(2016, 2, 3, 4, 5, 6, 7, time.UTC),
		want:     -(2*time.Minute + 50*time.Second + 4*time.Nanosecond),
	}, {
		name:     "zero",
		mocktime: time.Date(2016, 2, 3, 4, 7, 56, 11, time.UTC),
		ts:       time.Date(2016, 2, 3, 4, 7, 56, 11, time.UTC),
		want:     0,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tm := NewMockTime(tt.mocktime)
			if got := tm.Until(tt.ts); got != tt.want {
				t.Errorf("MockTime.Until() = %v, want %v", got, tt.want)
			}
		})
	}
}
