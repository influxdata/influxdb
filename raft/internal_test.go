package raft

func (l *Log) WaitUncommitted(index uint64) error { return l.waitUncommitted(index) }
func (l *Log) WaitCommitted(index uint64) error   { return l.waitCommitted(index) }
func (l *Log) WaitApplied(index uint64) error     { return l.Wait(index) }
