package scheduler

// NoopScheduler is a no-op scheduler. It is used when we don't want the
// standard scheduler to run (e.g. when "--no-tasks" flag is present).
type NoopScheduler struct{}

// Schedule is a mocked Scheduler.Schedule method.
func (n *NoopScheduler) Schedule(task Schedulable) error {
	return nil
}

// Release is a mocked Scheduler.Release method.
func (n *NoopScheduler) Release(taskID ID) error {
	return nil
}

// Stop is a mocked stop method.
func (n *NoopScheduler) Stop() {}
