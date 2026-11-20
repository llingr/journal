.PHONY: all build test coverage lint

# bash for pipefail, so a test failure fails the run however its output is filtered
SHELL := /bin/bash

# Built with a Go at least as new as go.mod's go directive, which it refuses otherwise
GOLANGCI_LINT_VERSION := v2.15.0

COVERAGE_PROFILE := .coverage/coverage.out

# The library's packages, measured by every test that runs them: not the examples, nor the
# suites and the in-memory backend that exist to exercise the library
COVERED_PACKAGES = $(shell go list ./... | grep -v -e /docs/ -e /journaltest -e /backendtest \
	-e /internal/memory | paste -sd, -)

all: build test

build:
	go build ./...
	go vet ./...

# Every suite runs over the in-memory backend, so no database is needed. go test's own
# coverage figure on each line is dropped, as it is that package's share of the whole library;
# the report after the run gives each package's coverage by every test.
test:
	@mkdir -p $(dir $(COVERAGE_PROFILE))
	@echo "go test -race -count=1 -coverpkg=<the library's packages> ./..."
	@set -o pipefail; go test -race -count=1 -coverpkg=$(COVERED_PACKAGES) \
		-coverprofile=$(COVERAGE_PROFILE) ./... | \
		grep -vE 'no test files|^[[:space:]]+[^[:space:]]+[[:space:]]+coverage: ' | \
		sed -E 's/[[:space:]]+coverage: .*//'
	@$(MAKE) --no-print-directory coverage

# Each package's statement coverage from the last test run, then the total
coverage:
	@echo "coverage by package:"
	@awk -F'[: ]' 'NR > 1 { block = $$1 ":" $$2; statements[block] = $$3; \
		if ($$4 > 0) covered[block] = 1 } \
		END { for (block in statements) { split(block, parts, ":"); package = parts[1]; \
		sub("/[^/]*$$", "", package); total[package] += statements[block]; \
		if (block in covered) hit[package] += statements[block] } \
		for (package in total) printf "  %5.1f%%  %s\n", 100 * hit[package] / total[package], \
		package }' $(COVERAGE_PROFILE) | sort -k2
	@go tool cover -func=$(COVERAGE_PROFILE) | tail -1 | awk '{print "total:", $$NF}'

lint:
	docker run --rm -v "$(CURDIR)":/src -w /src \
		golangci/golangci-lint:$(GOLANGCI_LINT_VERSION) golangci-lint run ./...
