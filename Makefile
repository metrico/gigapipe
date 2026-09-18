docker-compose ?= docker-compose

# Enable the recording-rules ruler for e2e: exported so both the gigapipe
# server (via compose interpolation) and the test runner see the same value.
export QRYN_RULER_ENABLED ?= true

# Query heads with a parser fuzz target.
heads := promql logql traceql

# HEAD/DURATION are set on the command line, e.g. `make fuzz HEAD=promql`.
HEAD ?=
DURATION ?= 60s

.PHONY: docker e2e-deps e2e-build e2e-test e2e-cleanup e2e-full e2e-ci \
	build test-unit lint arch-lint fuzz

docker:
	docker build -f scripts/deploy/docker/Dockerfile -t gigapipe .

# Build the gigapipe binary the same way CI does.
build:
	go mod tidy
	CGO_ENABLED=0 go build -ldflags="-extldflags=-static" -o gigapipe cmd/gigapipe/main.go

# Run the full unit-test suite with the race detector, matching CI.
test-unit:
	go test -race ./...

# Run golangci-lint with the repo's committed config, gating only issues
# introduced since origin/master (mirrors CI's only-new-issues option) so
# this doesn't fail locally on pre-existing repo-wide debt.
lint:
	golangci-lint run --new-from-rev=origin/master

# Run go-arch-lint in report-only mode against the baseline layering config.
arch-lint:
	go-arch-lint check

# Run the native Go parser fuzz target for one query head:
# make fuzz HEAD=promql|logql|traceql [DURATION=60s]
fuzz:
	@case " $(heads) " in \
	  *" $(HEAD) "*) ;; \
	  *) echo "fuzz: unknown or missing HEAD '$(HEAD)' (expected one of: $(heads)); usage: make fuzz HEAD=promql [DURATION=60s]" >&2; exit 1 ;; \
	esac
	go test "./reader/$(HEAD)/$(HEAD)_parser" -run '^$$' -fuzz "FuzzParse" -fuzztime "$(DURATION)"

e2e-deps:
	if [ ! -d ./deps/qryn-test ]; then git clone https://github.com/metrico/qryn-test.git ./deps/qryn-test; fi
	cd deps/qryn-test && git pull && git checkout main && git pull;

e2e-build:
	docker build -f scripts/deploy/docker/Dockerfile -t gigapipe .

e2e-test:
	$(docker-compose) -f ./scripts/test/e2e/docker-compose.yml up -d && \
   	docker rm -f qryn-go-test && \
   	sleep 60 && \
   	docker run \
   	  -v `pwd`/deps/qryn-test:/deps/e2e \
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
