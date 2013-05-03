coverage:
	gocov test github.com/benbjohnson/go-raft | gocov-html > coverage.html
	open coverage.html
