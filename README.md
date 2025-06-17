<a href="https://gigapipe.com" target="_blank">
  <img src='https://github.com/user-attachments/assets/fc8c7ca9-7a18-403d-b2a6-17899a534d33' style="margin-left:-10px;width:200px;" height=200 >
</a>

# <img src='https://avatars.githubusercontent.com/u/76224143?s=400&u=4e207cf756a7146392f9f04c6beb3940a417369d&v=4' style="margin-left:-10px" width=28 /> [Gigapipe: All-In-One Polyglot Observability](https://gigapipe.com)

> formerly known as _qryn_

<img src="https://user-images.githubusercontent.com/1423657/232089970-c4536f16-5967-4051-85a5-8ad94fcde67c.png" height=50>&nbsp; <img src="https://github.com/metrico/qryn/assets/1423657/546faddb-fbc6-4af5-9e32-4db6da10915d" height=49>

[![CI+CD](https://github.com/metrico/qryn/actions/workflows/build_release.yml/badge.svg)](https://github.com/metrico/qryn/actions/workflows/build_release.yml)
![CodeQL](https://github.com/lmangani/cLoki/workflows/CodeQL/badge.svg)
![GitHub Repo stars](https://img.shields.io/github/stars/metrico/qryn)


:rocket: Gigapipe is a _lighweight, multi-standard, polyglot **observability** stack for **Logs, Metrics**, **Traces** and **Profiling**_<br/>

* **Polyglot**: All-in-one, drop-in compatible with **Loki**, **Prometheus**, **Tempo**, **Pyroscope** 
* **Familiar**: Use stable & popular **LogQL**, **PromQL**, **TempoQL** languages to _query and visualize data_
* **Voracious**: Ingest using **OpenTelemetry, Loki, Prometheus, Tempo, Influx, Datadog, Elastic** _+ more_
* **Versatile**: Explore data with qryn's **built-in Explorer** and CLI or native **Grafana** datasource compatibility
* **Secure**: Retain total control of data, using **ClickHouse**, **DuckDB** or **GigAPI** with **S3** object storage
* **Independent**: Open source, community powered, anti lock-in alternative to vendor controlled stacks

<br>

# 🚀 Get Started

* Setup & Deploy **gigapipe** _opensource_ using the [documentation](https://gigapipe.com/docs/oss) and get help in our [Matrix room](https://matrix.to/#/#qryn:matrix.org) :octocat:
* Looking for a quick test before installing? Signup for a free trial at [gigapipe.com](https://gigapipe.com)

## Features
💡 _**gigapipe** independently implements popular observability standards, protocols and query languages_

## ➡️ Ingestion
### 📚 OpenTelemetry

⚡ **gigapipe** is officially integrated with [OpenTelemetry](https://github.com/metrico/otel-collector) supports _any log, trace or metric format_<br>
Ingested data can be queried using any of the available gigapipe APIs _(LogQL, PromQL, TraceQL)_

> 💡 _No modifications required to your OpenTelemetry instrumentation!_

### 📚 Native APIs
**gigapipe** supports [native ingestion](https://gigapipe.com/docs/api) for Loki, Prometheus, Tempo/Zipkin, Pyroscope and _[many other protocols](https://gigapipe.com/docs/api)_<br>
With gigapipe integrators can _push and read data using any desired combination of APIs and formats_

> 💡 _No opentelemetry or any other middlewayre/proxy required!_

<br>

## ⬅️ Query

### 📚 Loki + LogQL

> Any Loki compatible client or application can be used with gigapipe out of the box

⚡ **gigapipe** implements the *Loki API* for transparent compatibility with **[LogQL](https://grafana.com/docs/loki/latest/query/)** clients<br>

The Grafana Loki datasource can be used to natively browse and query _logs_ and display extracted _timeseries_<br>

<a href="https://gigapipe.com/docs" target="_blank">
<img src="https://user-images.githubusercontent.com/1423657/196654073-b84a218c-6a70-49bb-a477-e8be5714e0ba.gif" width=700 class=border />
</a>

> :tada: _No plugins needed_ <br>
> :eye: _No Grafana? No problem! Use View_


<br>

### 📈 Prometheus + PromQL

> Any Prometheus compatible client or application can be used with gigapipe out of the box

⚡ **gigapipe** implements the *Prometheus API* for transparent **[PromQL](https://prometheus.io/docs/prometheus/latest/querying/basics/)** compatibility using WASM 🏆<br>

The Grafana Prometheus datasource can be used to natively to query _metrics_ and display _timeseries_<br>

<a href="https://gigapipe.com/docs" target="_blank">
<img src="https://user-images.githubusercontent.com/1423657/196654084-1f1d8a62-3fd2-4420-a2fa-57ac2872938c.gif" width=700 class=border />
</a>

> :tada: _No plugins needed_ <br>
> :eye: _No Grafana? No problem! Use View_



<br>

### 🕛 Tempo + TraceQL

⚡ **gigapipe** implements the **Tempo API** for transparent compatibility with **[TraceQL](https://grafana.com/docs/tempo/latest/traceql/)** clients.<br>

> Any Tempo/OpenTelemetry compatible client or application can be used with gigapipe out of the box

The Tempo datasource can be used to natively query _traces_ including _**TraceQL**_ and supporting _service graphs_<br>

<a href="https://gigapipe.com/docs" target="_blank">
<img src="https://user-images.githubusercontent.com/1423657/196654097-8a235253-bf5d-4937-9e78-fddf12819d44.gif" width=700 class=border />
</a>

> :tada: _No plugins needed_ <br>
> :eye: _No Grafana? No problem! Use View_


<br>

### 🔥 Pyroscope + Phlare
⚡ **gigapipe** implements the **Pyroscope API** for transparent compatibility with **Pyroscope SDK** clients.<br>

> Any Pyroscope SDK client or Pyroscope compatible agent can be used with gigapipe out of the box for **continuous profiling**

<img src="https://github.com/metrico/qryn/assets/1423657/0bd11ca9-a2b4-41ee-9ea1-6f21fa4077b2" width=700>

<br>

<br>

### :eye: Built-In Explorer

**gigapipe** ships with **view** - our zero dependency, lightweight data explorer for **Logs, Metrics** and **Traces**

<a href="https://gigape.com/docs" target="_blank">
<img src="https://user-images.githubusercontent.com/1423657/200136242-f4133229-ee7c-45e0-8228-8734cf56140a.gif" width=700 class=border />
</a>

<br>

### 📚 Vendors Compatibility

**gigapipe** can ingest data using formats from [Grafana, InfluxDB, DataDog, Elastic](https://gigapipe.com/docs) and other vendors.


<br>


With **gigapipe** and **Grafana** everything _just works_ right out of the box: 

- Native datasource support without any plugin or extension
- Advanced Correlation between Logs, Metrics and Traces
- Service Graphs and Service Status Panels, and all the cool features

<br>

<a href="https://gigape.com" target="_blank">
<img src="https://user-images.githubusercontent.com/1423657/184538094-13c11500-24ef-4468-9f33-dc9d564238e3.gif" width=700 class=border />
</a>

<br>

<br>

------------

📚 Follow our team _behind the scenes_ on the [gigapipe blog](https://blog.gigapipe.com)

------------

### Got Questions?
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/metrico/gigapipe)

#### Contributions

Whether it's code, documentation or grammar, we ❤️ all contributions. Not sure where to get started?

- Join our [Matrix Channel](https://matrix.to/#/#qryn:matrix.org), and ask us any questions.
- Have a PR or idea? Request a session / code walkthrough with our team for guidance.

<br>

#### Contributors

&nbsp;&nbsp;&nbsp;&nbsp;[![Contributors for @metrico/gigapipe](https://contributors-img.web.app/image?repo=metrico/gigapipe)](https://github.com/metrico/gigapipe/graphs/contributors)

[![Stargazers repo roster for @metrico/gigapipe](https://bytecrank.com/nastyox/reporoster/php/stargazersSVG.php?user=metrico&repo=gigapipe)](https://github.com/metrico/gigapipe/stargazers)

[![Forkers repo roster for @metrico/gigapipe](https://bytecrank.com/nastyox/reporoster/php/forkersSVG.php?user=metrico&repo=gigapipe)](https://github.com/metrico/gigapipe/network/members)


#### License

<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/0/06/AGPLv3_Logo.svg/2560px-AGPLv3_Logo.svg.png" width=200>

©️ HEPVEST BV, All Rights Reserves. Released under the GNU Affero General Public License v3.0


[^1]: Gigapipe is a 100% clear-room api implementation and does not fork, use or derivate from Grafana code or design concepts.[^1]

[^2]: Gigapipe is not affiliated or endorsed by Grafana Labs or ClickHouse Inc. All rights belong to their respective owners.[^2]

[^3]: Grafana®, Loki™ and Tempo® are a Trademark of Raintank, Grafana Labs. ClickHouse® is a trademark of ClickHouse Inc.[^3]

[^4]: Prometheus is a trademark of The Linux Foundation.[^4]
