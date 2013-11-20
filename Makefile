all:
	bash -c 'pushd src/parser; ./build_parser.sh; popd'
	go build src/server/server.go
