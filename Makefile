.PHONY: build test test-race test-e2e cover vet fmt-check check benchmark

build:
	go build ./...

test:
	go test ./...

test-race:
	go test -race ./...

test-e2e:
	python3 scripts/python_e2e.py

cover:
	go test -cover ./...

vet:
	go vet ./...

fmt-check:
	@test -z "$$(gofmt -l $$(find cmd internal -type f -name '*.go'))" || \
		(gofmt -l $$(find cmd internal -type f -name '*.go'); exit 1)

check: fmt-check test test-race cover vet build

benchmark:
	@command -v redis-benchmark >/dev/null || \
		(echo "redis-benchmark is required (Redis CLI tools)" >&2; exit 1)
	./scripts/benchmark.sh
