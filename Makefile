docker-compose ?= docker-compose

# The end-to-end suite lives in its own repository. Defined here so the
# Makefile is the single source of truth for both; CI calls `make e2e-deps`
# rather than duplicating the checkout.
E2E_TESTS_REPO ?= https://github.com/metrico/gigapipe-tests.git
E2E_TESTS_DIR ?= ./deps/gigapipe-tests

# Enable the recording-rules ruler for e2e: exported so both the gigapipe
# server (via compose interpolation) and the test runner see the same value.
export QRYN_RULER_ENABLED ?= true

# Query heads with a parser fuzz target.
heads := promql logql traceql

# HEAD/DURATION are set on the command line, e.g. `make fuzz HEAD=promql`.
HEAD ?=
DURATION ?= 60s

# Only issues introduced since this revision are reported. CI passes the pull
# request's base commit.
LINT_NEW_FROM_REV ?= origin/master

.PHONY: docker e2e-deps e2e-build e2e-test e2e-cleanup e2e-full e2e-ci \
	fmt-check build test-unit lint fuzz

docker:
	docker build -f scripts/deploy/docker/Dockerfile -t gigapipe .

# Fail if any Go file is not gofmt-formatted, listing the offending files.
fmt-check:
	@test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }

# Build the gigapipe binary the same way CI does.
build:
	go mod tidy
	CGO_ENABLED=0 go build -ldflags="-extldflags=-static" -o gigapipe cmd/gigapipe/main.go

# Run the full unit-test suite with the race detector, matching CI.
test-unit:
	go test -race ./...

# Run golangci-lint with the repo's committed config, gating only issues
# introduced since LINT_NEW_FROM_REV so this doesn't fail on pre-existing
# repo-wide debt. golangci-lint is deliberately unpinned: @latest keeps up with
# new Go releases without a version to bump by hand, and the linters it runs
# are listed explicitly in .golangci.yml.
lint:
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run --new-from-rev=$(LINT_NEW_FROM_REV)

# Run the native Go parser fuzz target for one query head:
# make fuzz HEAD=promql|logql|traceql [DURATION=60s]
fuzz:
	@case " $(heads) " in \
	  *" $(HEAD) "*) ;; \
	  *) echo "fuzz: unknown or missing HEAD '$(HEAD)' (expected one of: $(heads)); usage: make fuzz HEAD=promql [DURATION=60s]" >&2; exit 1 ;; \
	esac
	go test "./reader/$(HEAD)/$(HEAD)_parser" -run '^$$' -fuzz "FuzzParse" -fuzztime "$(DURATION)"

e2e-deps:
	if [ ! -d $(E2E_TESTS_DIR) ]; then git clone $(E2E_TESTS_REPO) $(E2E_TESTS_DIR); fi
	cd $(E2E_TESTS_DIR) && git pull && git checkout main && git pull;

e2e-build:
	docker build -f scripts/deploy/docker/Dockerfile -t gigapipe .

e2e-test:
	$(docker-compose) -f ./scripts/test/e2e/docker-compose.yml up -d && \
   	docker rm -f qryn-go-test && \
   	sleep 60 && \
   	docker run \
   	  -v `pwd`/$(E2E_TESTS_DIR):/deps/e2e \
   	  --network=e2e_common \
   	  --name=qryn-go-test \
   	  -e INTEGRATION_E2E=1\
   	  -e CLOKI_EXT_URL="e2e.aio:9080" \
   	  -e QRYN_LOGIN=a \
   	  -e QRYN_PASSWORD=b \
   	  -e QRYN_RULER_ENABLED \
   	  -e OTEL_COLL_URL="http://a:b@e2e.aio:9080" \
   	  node:18-alpine \
   	  sh -c 'cd /deps/e2e && npm install && npm test -- --forceExit'

e2e-cleanup:
	$(docker-compose) -f ./scripts/test/e2e/docker-compose.yml down
	docker rm -f qryn-go-test

e2e-full: e2e-deps e2e-build e2e-test e2e-cleanup

e2e-ci: e2e-build e2e-test e2e-cleanup
