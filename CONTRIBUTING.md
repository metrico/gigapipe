# Contributing to Gigapipe

Thank you for your interest in contributing to Gigapipe! This guide helps you understand how to contribute to the project, whether through code, documentation, or bug fixes.

## Before You Begin

Before contributing, you should be familiar with:

- **Go programming language** - Gigapipe is written in Go (version 1.26.3 or higher)
- **Observability concepts** - Understanding of logs, metrics, traces, and profiling
- **Development tools** - Go, Docker, and ClickHouse for local testing

## Getting Started

### Fork and Clone the Repository

1. Fork the [gigapipe repository](https://github.com/metrico/gigapipe) on GitHub
2. Clone your fork locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/gigapipe.git
   cd gigapipe
   ```
3. Add the upstream repository:
   ```bash
   git remote add upstream https://github.com/metrico/gigapipe.git
   ```

### Set Up Development Environment

1. **Install Go** - Download and install Go 1.26.3 or higher from [golang.org](https://golang.org/dl/)

2. **Install Docker** - Required for running ClickHouse and integration tests. Get it from [docker.com](https://www.docker.com/get-started)

3. **Install dependencies**:
   ```bash
   go mod download
   ```

4. **Set up ClickHouse** - For local testing, you can use Docker to run ClickHouse:
   ```bash
   docker run -d --name gigapipe-clickhouse \
     -p 9000:9000 -p 8123:8123 \
     clickhouse/clickhouse-server:latest
   ```

## Making Changes

### Code Style Guidelines

- Follow standard Go conventions and idioms
- Use `gofmt` to format your code
- Write clear, descriptive variable and function names
- Add comments for complex logic or non-obvious behavior

### Testing Requirements

**All changes must include appropriate tests.** This is critical for maintaining code quality and preventing regressions.

Run tests before submitting your PR:

```bash
go test ./...
```

For end-to-end tests:

```bash
make e2e-full
```

### Integration Compatibility Notes

When working on integration code, be aware of these critical compatibility requirements:

#### Grafana Tempo/TraceQL Integration

Duration values in the TraceQL search API **must be integers (int64), not floats**. Grafana's Tempo datasource expects `durationMs` as `uint32` and cannot unmarshal fractional values.

**Key points:**
- Use `intDiv()` for duration calculations, not floating-point division
- The `DurationMs` field in trace responses must be `int64`
- Fractional millisecond values cause JSON unmarshaling errors in Grafana

**Example:**
```go
// ✅ Correct - uses integer division
intDiv(max(traces.timestamp_ns + traces.duration_ns) - min(traces.timestamp_ns), 1000000)

// ❌ Wrong - produces float64
toFloat64(max(traces.timestamp_ns + traces.duration_ns) - min(traces.timestamp_ns)) / 1000000
```

**Reference:**
- Issue [#782](https://github.com/metrico/gigapipe/issues/782) - Original bug report
- PR [#784](https://github.com/metrico/gigapipe/pull/784) - Fix implementation
- Test: `reader/traceql/traceql_transpiler/clickhouse_transpiler/planner_test.go:TestTracesDataDurationMsSQL`

#### Prometheus remote_write Integration

The batch flush logic uses `flushLimit` (1000 samples) to handle large time series efficiently. Array sizes passed to `onEntries` **must match the flushed batch size**, not the total sample count.

**Key points:**
- When flushing batches, use `len(tsns)` for array sizes (accumulated samples)
- Do **not** use `len(ts.GetSamples())` (total samples in time series)
- Mismatched column lengths cause ClickHouse to reject blocks, resulting in **silent data loss**

**Example:**
```go
// ✅ Correct - array size matches flushed batch
err := l.onEntries(oLblsBuf, tsns, msg, value,
    fastFillArray[uint8](len(tsns), model.SAMPLE_TYPE_METRIC))

// ❌ Wrong - uses total sample count instead of batch size
err := l.onEntries(oLblsBuf, tsns, msg, value,
    fastFillArray[uint8](len(ts.GetSamples()), model.SAMPLE_TYPE_METRIC))
```

**Reference:**
- Issue [#783](https://github.com/metrico/gigapipe/issues/783) - Original bug report
- PR [#784](https://github.com/metrico/gigapipe/pull/784) - Fix implementation
- Test: `writer/utils/unmarshal/metrics_protobuf_test.go:TestPromMetricsFlushLimitTypesLength`

## Submitting a Pull Request

1. **Create a branch** for your changes:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** and commit them with clear, descriptive messages:
   ```bash
   git commit -m "Add support for X feature"
   ```

3. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```

4. **Open a Pull Request** on GitHub with:
   - Clear description of what the PR does
   - Reference to any related issues
   - Explanation of testing performed
   - Any breaking changes or migration notes

### Code Review Expectations

- Be responsive to feedback and questions
- Make requested changes promptly
- Keep discussions focused and professional
- PR reviews may take a few days - please be patient

## Get Help

If you need help or have questions:

- **Matrix room** - Join us at [#qryn:matrix.org](https://matrix.to/#/#qryn:matrix.org) for real-time discussions
- **GitHub Issues** - For bug reports and feature requests, open an issue
- **Gigapipe Deepwiki** - Ask questions at [deepwiki.com/metrico/gigapipe](https://deepwiki.com/metrico/gigapipe)
- **Code walkthrough** - Request a session with the team for guidance on complex changes

## License

By contributing to Gigapipe, you agree that your contributions will be licensed under the GNU Affero General Public License v3.0 (AGPL-3.0).
