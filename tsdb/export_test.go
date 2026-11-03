package tsdb

// LockChangesFile takes the lock SaveWriter holds while writing the changes
// file, so queued saves stay unwritten until unlock is called.
func (fs *MeasurementFieldSet) LockChangesFile() (unlock func()) {
	fs.changeMgr.mu.Lock()
	return fs.changeMgr.mu.Unlock
}

// SaveAsync queues changes without waiting for them to be written. The
// result of the write arrives on the returned channel.
func (fs *MeasurementFieldSet) SaveAsync(changes FieldChanges) (<-chan error, error) {
	done := make(chan error, 1)
	if _, err := fs.changeMgr.enqueue(done, func() (FieldChanges, error) { return changes, nil }); err != nil {
		return nil, err
	}
	return done, nil
}

// ClosedForSaves reports whether Close has stopped accepting saves.
func (fs *MeasurementFieldSet) ClosedForSaves() bool {
	fs.changeMgr.chMu.RLock()
	defer fs.changeMgr.chMu.RUnlock()
	return fs.changeMgr.writeRequests == nil
}
