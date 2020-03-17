package tenant

type Service struct {
	store *Store
}

func NewService(st *Store) *Service {
	return &Service{
		store: st,
	}
}
