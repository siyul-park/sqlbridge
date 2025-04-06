-include .env

CURRENT_DIR = $(shell realpath .)

CGO_ENABLED ?= 1

.PHONY: init generate clean tidy update sync check test coverage benchmark lint fmt vet doc
all: lint test

init:
	@$(MAKE) install-tools
	@$(MAKE) install-modules

install-tools:
	@go install golang.org/x/tools/cmd/godoc@latest
	@go install golang.org/x/tools/cmd/goimports@latest

install-modules:
	@go install -v ./...

generate:
	@go generate ./...

clean:
	@go clean -cache
	@rm -rf dist

tidy:
	@go mod tidy

update:
	@go get -u all

clean-sum:
	@rm go.sum

clean-cache:
	@for dir in $(MODULE_DIRS); do \
		cd $$dir && go clean -modcache; \
	done

sync:
	@go work sync

check: lint test

test:
	@go test -race $(test-options) ./...

coverage:
	@go test -race --coverprofile=coverage.out --covermode=atomic $(test-options) ./...

benchmark:
	@go test -run="-" -bench=".*" -benchmem $(test-options) ./...

lint: fmt vet

fmt:
	@goimports -w .

vet:
	@go vet ./...

doc: init
	@godoc -http=:6060
