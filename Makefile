docker-compose ?= docker-compose

# The end-to-end suite lives in its own repository. Defined here so the
# Makefile is the single source of truth for both; CI calls `make e2e-deps`
# rather than duplicating the checkout.
E2E_TESTS_REPO ?= https://github.com/metrico/gigapipe-tests.git
E2E_TESTS_DIR ?= ./deps/gigapipe-tests

# Enable the recording-rules ruler for e2e: exported so both the gigapipe
# server (via compose interpolation) and the test runner see the same value.
export QRYN_RULER_ENABLED ?= true

docker:
	docker build -f scripts/deploy/docker/Dockerfile -t gigapipe .

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
