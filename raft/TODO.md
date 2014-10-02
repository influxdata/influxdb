TODO
====

## Uncompleted

- [ ] Proxy Apply() to leader
- [ ] Callback
- [ ] Leave / RemovePeer
- [ ] Periodic flushing (maybe in applier?)

## Completed

- [x] Encoding
- [x] Streaming
- [x] Log initialization
- [x] Only store pending entries.
- [x] Consolidate segment into log.
- [x] Snapshot FSM
- [x] Initialize last log index from FSM.
- [x] Election
- [x] Candidate loop
- [x] Save current term to disk.
