# Single entry point for build/test/lint lanes. Install: https://github.com/casey/just
#
# Run `just --list` to see all recipes.

set shell := ["bash", "-euo", "pipefail", "-c"]

# Query heads with a golden corpus / parser fuzz target.
heads := "promql logql traceql"

# Show available recipes.
default:
    just --list

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

# Regenerate golden fixtures for one query head: promql, logql, or traceql.
#
# Every head's golden-driving test function is named exactly TestGolden in
# its package (see reader/internal/goldentest's doc comment for the full
# convention). This only targets the package(s) that actually define
# TestGolden, found by grepping for the function rather than assuming a
# fixed subpackage path: `go test ./reader/<head>/... -update` would instead
# fail outright on any sibling package under that head that doesn't import
# goldentest (e.g. a plain parser-fuzz package), since -update must be a flag
# every targeted test binary recognizes.
update-golden head:
    #!/usr/bin/env bash
    set -euo pipefail
    case "{{head}}" in
        promql|logql|traceql) ;;
        *)
            echo "update-golden: unknown head '{{head}}' (expected one of: {{heads}})" >&2
            exit 1
            ;;
    esac
    pkgs=$(grep -rl '^func TestGolden(' --include='*_test.go' "reader/{{head}}" 2>/dev/null | xargs -r -n1 dirname | sort -u | sed 's#^#./#') || true
    if [ -z "$pkgs" ]; then
        echo "update-golden: no TestGolden found under reader/{{head}} (corpus not written yet?)" >&2
        exit 1
    fi
    go test $pkgs -run TestGolden -update -v

# Run the native Go parser fuzz target for one query head: promql, logql, or traceql.
fuzz head duration='60s':
    #!/usr/bin/env bash
    set -euo pipefail
    case "{{head}}" in
        promql)  pkg=./reader/promql/promql_parser ;;
        logql)   pkg=./reader/logql/logql_parser ;;
        traceql) pkg=./reader/traceql/traceql_parser ;;
        *)
            echo "fuzz: unknown head '{{head}}' (expected one of: {{heads}})" >&2
            exit 1
            ;;
    esac
    go test "$pkg" -run '^$' -fuzz "FuzzParse" -fuzztime "{{duration}}"
