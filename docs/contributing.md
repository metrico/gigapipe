# Contributing to Gigapipe

## Before You Begin

Before contributing, you should be familiar with:

- **Go programming language** - Gigapipe is written in Go
- **Observability concepts** - Understanding of logs, metrics, traces, and profiling
- **Development tools** - Go, Docker, and ClickHouse for local testing

## Code Originality

Gigapipe is a 100% clear-room, native implementation. **Do not submit code copied or derived from other projects** (Prometheus, Grafana, Loki, Tempo, or any other open source project). All contributions must be original work authored by you.

PRs containing copied or derived code will be rejected regardless of license compatibility.

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

1. **Install Go** - Download and install Go from [golang.org](https://golang.org/dl/)

2. **Install Docker** - Required for running ClickHouse and integration tests. Get it from [docker.com](https://www.docker.com/get-started)

3. **Install dependencies**:
   ```bash
   go mod download
   ```

4. **Set up ClickHouse** - For local testing, run ClickHouse with Docker:
   ```bash
   docker run -d --name gigapipe-clickhouse \
     -p 9000:9000 -p 8123:8123 \
     clickhouse/clickhouse-server:latest
   ```

## Making Changes

### Code Style

- Follow standard Go conventions and idioms
- Use `gofmt` to format your code
- Write clear, descriptive variable and function names

### Testing Requirements

All changes must include appropriate tests. Run tests before submitting a PR:

```bash
go test ./...
```

For end-to-end tests:

```bash
make e2e-full
```

## Submitting a Pull Request

1. Create a branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```
2. Commit with clear messages and push to your fork:
   ```bash
   git commit -m "Add support for X feature"
   git push origin feature/your-feature-name
   ```
3. Open a Pull Request with a description of what it does, references to related issues, and notes on testing performed.
4. **Sign the CLA** — a CLA signature is mandatory before any PR can be reviewed or merged. The CLA bot will prompt you on the PR.

## Get Help

- **Matrix room** - [#qryn:matrix.org](https://matrix.to/#/#qryn:matrix.org) for real-time discussions
- **GitHub Issues** - For bug reports and feature requests
- **Gigapipe Deepwiki** - [deepwiki.com/metrico/gigapipe](https://deepwiki.com/metrico/gigapipe)
