//go:generate env GO111MODULE=on go run github.com/kevinburke/go-bindata/go-bindata -prefix "migrations/" -o ./migrations_gen.go -ignore README|go|conf -pkg migrations .

package migrations

type All struct{}

func (s *All) ListNames() []string {
	return AssetNames()
}

func (s *All) MustAssetString(n string) string {
	return MustAssetString(n)
}
