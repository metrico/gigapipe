<a href="https://qryn.dev" target="_blank">
<img src='https://user-images.githubusercontent.com/1423657/218816262-e0e8d7ad-44d0-4a7d-9497-0d383ed78b83.png' style="margin-left:-10px" width=350/>
</a>

[![Build Status](https://github.com/metrico/qryn/actions/workflows/bump_version.yml/badge.svg)](https://github.com/metrico/qryn/actions/workflows/bump_version.yml)
![CodeQL](https://github.com/lmangani/cLoki/workflows/CodeQL/badge.svg)
<a href="https://matrix.to/#/#qryn:matrix.org">
  <img src="https://img.shields.io/badge/Join%20Matrix-Chat-green" alt="Matrix">
</a>
<a href="https://qryn.cloud">
  <img src="https://img.shields.io/badge/Cloud-Service-purple" alt="cloud">
</a>


# [qryn](https://qryn.dev) :cloud: [qryn.cloud](https://qryn.cloud)
> ... it's pronounced /ˈkwɪr..ɪŋ/ or just _querying_

![image](https://user-images.githubusercontent.com/1423657/232089970-c4536f16-5967-4051-85a5-8ad94fcde67c.png)


:rocket: **qryn** is a _lighweight, multi-standard drop-in compatible_ **polyglot observability** framework for _**Logs, Metrics and Traces**_.<br/>

- **Polyglot**: Use **LogQL, PromQL**, and **TempoQL** languages to query, process and alert _any data_.
- **Voracious**: Ingest anything compatible with **Opentelemetry, Loki, Prometheus, Influx, Datadog, Elastic** _& more_.
- **Versatile**: Explore data with qryn's built-in **Explore UI** and **CLI** or _native_ **Grafana** compatibility.
- **Secure**: Keep control of your data, using **ClickHouse** or **InfluxDB IOx** on top of disposable **S3** storage.
- **Unmetered**: Unlimited FOSS deployments or **qryn.cloud** service with advanced features and pro performance.
- **Effective**: Do more with less, replace lock-in competitors for a fractions of the cost and complexity.
<br>


## 🚀 [Get Started](https://qryn.metrico.in/#/installation)

* Deploy qryn OSS using the [documentation](https://qryn.metrico.in/#/installation) or get help in our [Matrix room](https://matrix.to/#/#qryn:matrix.org) :octocat:
* Create a free account on [qryn.cloud](https://qryn.cloud) and get polyglot without moving a finger ☁️ 

<a href="https://qryn.cloud" target="_blank">
<img src="https://user-images.githubusercontent.com/1423657/218818279-3efff74f-0191-498a-bdc4-f2650c9d3b49.gif">
</a>

<br>

<br>

## Features

### 📚 OpenTelemetry
qryn is officially integrated with [opentelemetry](https://github.com/metrico/otel-collector) supports _any log, trace or metric format_

### 📚 LogQL

> Any Loki compatible client or application can be used with qryn out of the box

qryn implements a full [LogQL API](https://github.com/lmangani/qryn/wiki/LogQL-Supported-Queries) to provide transparent compatibility with Loki<br>
The Grafana Loki datasource can be used to natively browse and query _logs_ and display extracted _timeseries_<br>

<a href="https://qryn.metrico.in/#/logs/query" target="_blank">
<img src="https://user-images.githubusercontent.com/1423657/196654073-b84a218c-6a70-49bb-a477-e8be5714e0ba.gif" width=700 class=border />
</a>

:tada: _No plugins needed_ 


<br>

### 📈 Prometheus

> Any Prometheus compatible client or application can be used with qryn out of the box

qryn is the first platform to deliver full [Prometheus API](https://prometheus.io/docs/prometheus/latest/querying/api/) compatibility using WASM 🏆<br>
The Grafana Prometheus datasource can be used to natively to query _metrics_ and display _timeseries_<br>

<a href="https://qryn.metrico.in/#/metrics/query" target="_blank">
<img src="https://user-images.githubusercontent.com/1423657/196654084-1f1d8a62-3fd2-4420-a2fa-57ac2872938c.gif" width=700 class=border />
</a>

:tada: _No plugins needed_ 


<br>

### 🕛 Tempo

qryn implements the [Tempo API](https://github.com/lmangani/qryn/wiki/LogQL-Supported-Queries) to provide transparent compatibility with Tempo/OTLP clients.<br>

> Any Tempo/Opentelemetry compatible client or application can be used with qryn out of the box

The Tempo datasource can be used to natively query _traces_ including _**TraceQL**_ and supporting _service graphs_<br>

<a href="https://qryn.metrico.in/#/telemetry/query" target="_blank">
<img src="https://user-images.githubusercontent.com/1423657/196654097-8a235253-bf5d-4937-9e78-fddf12819d44.gif" width=700 class=border />
</a>

:tada: _No plugins needed_ 

<br>

### ↔️ Correlation
Data correlation is simple using qryn's drop-in compatibility with **Grafana** native datasources.

<a href="https://qryn.dev" target="_blank">
<img src="https://user-images.githubusercontent.com/1423657/184538094-13c11500-24ef-4468-9f33-dc9d564238e3.gif" width=700 class=border />
</a>

<br>

### :eye: View

No Grafana? No Problem. **qryn** ships with **view** - it's own lightweight data exploration tool

<a href="https://qryn.dev" target="_blank">
<img src="https://user-images.githubusercontent.com/1423657/200136242-f4133229-ee7c-45e0-8228-8734cf56140a.gif" width=700 class=border />
</a>

------------

📚 Follow our team _behind the scenes_ on the [qryn blog](https://blog.qryn.dev)

------------

#### Contributions

Whether it's code, documentation or grammar, we ❤️ all contributions. Not sure where to get started?

- Join our [Matrix Channel](https://matrix.to/#/#qryn:matrix.org), and ask us any questions.
- Have a PR or idea? Request a session / code walkthrough with our team for guidance.

<br>

#### Contributors

&nbsp;&nbsp;&nbsp;&nbsp;[![Contributors for @metrico/qryn](https://contributors-img.web.app/image?repo=lmangani/cloki)](https://github.com/metrico/qryn/graphs/contributors)

[![Stargazers repo roster for @metrico/qryn](https://reporoster.com/stars/metrico/qryn)](https://github.com/metrico/qryn/stargazers)

[![Forkers repo roster for @metrico/qryn](https://reporoster.com/forks/metrico/qryn)](https://github.com/metrico/qryn/network/members)


#### License

<img src="https://camo.githubusercontent.com/473b62766b498e4f2b008ada39f1d56fb3183649f24447866e25d958ac3fd79a/68747470733a2f2f7777772e676e752e6f72672f67726170686963732f6167706c76332d3135357835312e706e67">

©️ QXIP BV, released under the GNU Affero General Public License v3.0. See [LICENSE](LICENSE) for details.


[^1]: qryn is not affiliated or endorsed by Grafana Labs or ClickHouse Inc. All rights belong to their respective owners.

[^2]: qryn is a 100% clear-room api implementation and does not fork, use or derivate from Grafana Loki code or concepts.

[^3]: Grafana®, Loki™ and Tempo® are a Trademark of Raintank, Grafana Labs. ClickHouse® is a trademark of ClickHouse Inc. Prometheus is a trademark of The Linux Foundation.
