package tsdb

import (
	"testing"
	"time"
)

func TestEpochTracker(t *testing.T) {
	t.Run("Delete waits", func(t *testing.T) {
		tr := newEpochTracker()

		// delete should proceed with no pending writes
		waiter := tr.WaitDelete(newGuard(0, 0, nil, nil))
		waiter.Wait()
		waiter.Done()

		for i := 0; i < 1000; i++ {
			// start up some writes
			_, w1 := tr.StartWrite()
			_, w2 := tr.StartWrite()
			_, w3 := tr.StartWrite()

			// wait for a delete. this time based stuff isn't sufficient
			// to check every problem, but it can catch some.
			waiter := tr.WaitDelete(nil)
			done := make(chan time.Time, 1)
			go func() { waiter.Wait(); done <- time.Now() }()

			// future writes should not block the waiter
			_, w4 := tr.StartWrite()

			// ending the writes allows the waiter to proceed
			tr.EndWrite(w1)
			tr.EndWrite(w2)
			now := time.Now()
			tr.EndWrite(w3)
			if (<-done).Before(now) {
				t.Fatal("Wait ended too soon")
			}
			tr.EndWrite(w4)
		}
	})

	t.Run("Guards tracked", func(t *testing.T) {
		checkGuards := func(got []*guard, exp ...*guard) {
			t.Helper()
			if len(exp) != len(got) {
				t.Fatalf("invalid: %p != %p", exp, got)
			}
		next:
			for _, g1 := range got {
				for _, g2 := range exp {
					if g1 == g2 {
						continue next
					}
				}
				t.Fatalf("invalid: %p != %p", exp, got)
			}
		}

		tr := newEpochTracker()
		g1, g2, g3 := newGuard(0, 0, nil, nil), newGuard(0, 0, nil, nil), newGuard(0, 0, nil, nil)

		guards, _ := tr.StartWrite()
		checkGuards(guards)

		d1 := tr.WaitDelete(g1)
		guards, _ = tr.StartWrite()
		checkGuards(guards, g1)

		d2 := tr.WaitDelete(g2)
		guards, _ = tr.StartWrite()
		checkGuards(guards, g1, g2)

		d3 := tr.WaitDelete(g3)
		guards, _ = tr.StartWrite()
		checkGuards(guards, g1, g2, g3)

		d2.Done()
		guards, _ = tr.StartWrite()
		checkGuards(guards, g1, g3)

		d1.Done()
		guards, _ = tr.StartWrite()
		checkGuards(guards, g3)

		d3.Done()
		guards, _ = tr.StartWrite()
		checkGuards(guards)
	})
}

func BenchmarkEpochTracker(b *testing.B) {
	b.Run("Writes with deletes", func(b *testing.B) {
		b.Run("Serial", func(b *testing.B) {
			run := func(b *testing.B, deletes int) {
				tr := newEpochTracker()
				tr.StartWrite()
				for i := 0; i < deletes; i++ {
					tr.WaitDelete(nil)
				}
				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					_, gen := tr.StartWrite()
					tr.EndWrite(gen)
				}
			}

			b.Run("0", func(b *testing.B) { run(b, 0) })
			b.Run("1", func(b *testing.B) { run(b, 1) })
			b.Run("10", func(b *testing.B) { run(b, 10) })
			b.Run("100", func(b *testing.B) { run(b, 100) })
		})

		b.Run("Parallel", func(b *testing.B) {
			run := func(b *testing.B, deletes int) {
				tr := newEpochTracker()
				tr.StartWrite()
				for i := 0; i < deletes; i++ {
					tr.WaitDelete(nil)
				}
				b.ReportAllocs()
				b.ResetTimer()

				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						_, gen := tr.StartWrite()
						tr.EndWrite(gen)
					}
				})
			}

			b.Run("0", func(b *testing.B) { run(b, 0) })
			b.Run("1", func(b *testing.B) { run(b, 1) })
			b.Run("10", func(b *testing.B) { run(b, 10) })
			b.Run("100", func(b *testing.B) { run(b, 100) })
		})
	})
}
