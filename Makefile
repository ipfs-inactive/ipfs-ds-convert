all: deps

gx:
	go get github.com/whyrusleeping/gx
	go get github.com/whyrusleeping/gx-go

covertools:
	go get github.com/mattn/goveralls
	go get golang.org/x/tools/cmd/cover
	go get github.com/wadey/gocovmerge

deps: gx covertools
	go get golang.org/x/sys/unix
	go get golang.org/x/net/trace
	gx --verbose install --global
	gx-go rewrite

publish:
	gx-go rewrite --undo

test: deps
	go test ./... -v

circle: deps
	go vet
	$(eval PKGS := $(shell go list ./...))
	$(eval PKGS_DELIM := $(shell echo $(PKGS) | sed -e 's/ /,/g'))
	go list -f '{{if or (len .TestGoFiles) (len .XTestGoFiles)}}go test -test.v -covermode=atomic -coverprofile=/home/ubuntu/{{.Name}}_{{len .Imports}}_{{len .Deps}}.coverprofile -coverpkg $(PKGS_DELIM) {{.ImportPath}}{{end}}' $(PKGS) | xargs -I {} bash -c {}
	gocovmerge `ls /home/ubuntu/*.coverprofile` > /home/ubuntu/coverage.out
	rm /home/ubuntu/*.coverprofile
	$(GOPATH)/bin/goveralls -coverprofile=/home/ubuntu/coverage.out -service=circle-ci -repotoken=$(COVERALLS_TOKEN)
