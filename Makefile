
covertools:
	go get github.com/mattn/goveralls
	go get golang.org/x/tools/cmd/cover
	go get github.com/wadey/gocovmerge

deps: covertools
	go get golang.org/x/sys/unix
	go get golang.org/x/net/trace

test:
	go test ./... -v

install:
	go install

circle: deps
	go vet
	$(eval PKGS := $(shell go list ./...))
	$(eval PKGS_DELIM := $(shell echo $(PKGS) | sed -e 's/ /,/g'))
	go list -f '{{if or (len .TestGoFiles) (len .XTestGoFiles)}}go test -test.v -covermode=atomic -coverprofile={{.Name}}_{{len .Imports}}_{{len .Deps}}.coverprofile -coverpkg $(PKGS_DELIM) {{.ImportPath}}{{end}}' $(PKGS) | xargs -I {} bash -c {}
	gocovmerge `ls *.coverprofile` > coverage.out
