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

# golangci-lint is built from source with the local Go toolchain, and pinned
# here only. The golang.org/x/tools it depends on must read the export data of
# the Go release in go.mod: go1.27.3 writes format V5, which needs x/tools
# >= v0.50.0, first used by v2.14.0. Re-check when go.mod moves.
GOLANGCI_LINT_VERSION ?= v2.14.0
# Only issues introduced since this revision are reported. CI passes the pull
# request's base commit.
LINT_NEW_FROM_REV ?= origin/master

.PHONY: docker e2e-deps e2e-build e2e-test e2e-cleanup e2e-full e2e-ci \
	fmt-check build test-unit integration-start test-integration lint arch-lint fuzz

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

# Start the binary built by `make build` in the background against the
# ClickHouse at CLICKHOUSE_SERVER:CLICKHOUSE_PORT and wait until it answers.
# Output goes to gigapipe.log. Used by the integration CI lane; locally,
# test/integration/run.sh starts ClickHouse and gigapipe in Docker instead.
integration-start:
	CLICKHOUSE_SERVER=$${CLICKHOUSE_SERVER:-localhost} CLICKHOUSE_PORT=$${CLICKHOUSE_PORT:-9000} \
		PORT=$${PORT:-3100} BULK_MAX_AGE_MS=$${BULK_MAX_AGE_MS:-100} \
		./gigapipe > gigapipe.log 2>&1 & echo $$! > gigapipe.pid
	@for i in $$(seq 1 60); do \
		if curl -sf http://localhost:$${PORT:-3100}/api/v1/status/buildinfo >/dev/null; then \
			echo "gigapipe ready"; exit 0; \
		fi; \
		sleep 1; \
	done; \
	echo "gigapipe never became ready; logs:"; cat gigapipe.log; exit 1

# Run the Go integration tests against the gigapipe at GIGAPIPE_URL
# (default http://localhost:3100).
test-integration:
	go test -tags integration -count=1 -v ./test/integration/...

# Run golangci-lint with the repo's committed config, gating only issues
# introduced since LINT_NEW_FROM_REV so this doesn't fail on pre-existing
# repo-wide debt.
lint:
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run --new-from-rev=$(LINT_NEW_FROM_REV)

# Run go-arch-lint in report-only mode against the baseline layering config.
# It is pinned in tools/archlint, which also raises golang.org/x/tools to a
# version that reads the export data of the Go release in go.mod.
arch-lint:
	go -C tools/archlint tool go-arch-lint check --project-path $(CURDIR)

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
