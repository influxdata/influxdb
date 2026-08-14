package storageflux

func (t *table) IsDone() bool {
	return t.used.Load()
}
