package tenant

import "github.com/influxdata/influxdb"

type Service struct {
	store *Store
}

func NewService(st *Store) influxdb.TenantService {
	return &Service{
		store: st,
	}
}
