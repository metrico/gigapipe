# Gigapipe Grafana Dashboards

Pre-built Grafana dashboards for Gigapipe observability stacks.

---

## AI Agent Observability (`ai-agent-observability.json`)

Monitor any OTel-instrumented AI agent in a single dashboard — token usage, latency, cost estimates, and error rates across all your models and frameworks.

**Panels included:**
- Requests, error rate, total tokens, estimated hourly cost
- Token usage by model (input + output)
- Request latency — P50 / P95 / P99
- Request rate by model
- Error rate over time
- Input / output tokens by model (bar gauge)

**Works with:** Claude Code Agent SDK, Anthropic SDK, LangChain, LangGraph, OpenAI Agents SDK, GitHub Copilot SDK, and any framework that follows the [OpenTelemetry Gen AI semantic conventions](https://opentelemetry.io/docs/specs/semconv/gen-ai/).

**Requires:** A Prometheus datasource pointing at your Gigapipe endpoint.

### Import

1. In Grafana: **Dashboards → Import → Upload JSON file**
2. Select `ai-agent-observability.json`
3. Set the datasource to your Gigapipe Prometheus endpoint

### Setup guide

Full step-by-step setup — OTel Collector config, agent instrumentation for every major framework, and Grafana panel queries — in the blog post:

**[Your AI Agent Is Burning Money. You Just Can't See It Yet.](https://blog.gigapipe.com/your-ai-agent-is-burning-money-you-just-can-t-see-it-yet)**
