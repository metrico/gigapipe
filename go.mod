module github.com/metrico/qryn

go 1.24.0

toolchain go1.24.2

replace (
	cloud.google.com/go/compute v0.2.0 => cloud.google.com/go/compute v1.7.0
	github.com/docker/distribution v2.7.1+incompatible => github.com/docker/distribution v2.8.0+incompatible
	github.com/pascaldekloe/mqtt v1.0.0 => github.com/metrico/mqtt v1.0.1-0.20220314083119-cb53cdb0fcbe
	github.com/prometheus/common v0.63.0 => github.com/prometheus/common v0.61.0
	github.com/prometheus/prometheus v0.300.1 => github.com/prometheus/prometheus v1.8.2-0.20220714142409-b41e0750abf5
	//TODO: remove this
	go.opentelemetry.io/collector/pdata v1.12.0 => go.opentelemetry.io/collector/pdata v0.62.1
	go.opentelemetry.io/otel v1.19.0 => go.opentelemetry.io/otel v1.7.0
	go.opentelemetry.io/otel/internal/global v1.19.0 => go.opentelemetry.io/otel/internal/global v1.7.0
	go.opentelemetry.io/otel/metric v1.21.0 => go.opentelemetry.io/otel/metric v0.30.0
	google.golang.org/grpc v1.47.0 => google.golang.org/grpc v1.45.0
	gopkg.in/fatih/pool.v2 v2.0.0 => gopkg.in/fatih/pool.v3 v3.0.0
	k8s.io/api v0.32.3 => k8s.io/api v0.24.17
	k8s.io/apimachinery v0.32.3 => k8s.io/apimachinery v0.24.17
	k8s.io/client-go v12.0.0+incompatible => k8s.io/client-go v0.22.1
)

require (
	github.com/ClickHouse/ch-go v0.69.0
	github.com/ClickHouse/clickhouse-go/v2 v2.34.0
	github.com/Masterminds/sprig v2.22.0+incompatible
	github.com/VictoriaMetrics/fastcache v1.12.2
	github.com/alecthomas/participle/v2 v2.1.1
	github.com/avast/retry-go v3.0.0+incompatible
	github.com/bradleyjkemp/cupaloy v2.3.0+incompatible
	github.com/c2h5oh/datasize v0.0.0-20231215233829-aa82cc1e6500
	github.com/go-faster/city v1.0.1
	github.com/go-faster/jx v1.1.0
	github.com/go-kit/kit v0.13.0
	github.com/go-logfmt/logfmt v0.6.0
	github.com/gofiber/fiber/v2 v2.52.5
	github.com/gofiber/websocket/v2 v2.2.1
	github.com/golang/snappy v1.0.0
	github.com/google/pprof v0.0.0-20241029153458-d1b30febd7db
	github.com/gorilla/mux v1.8.1
	github.com/gorilla/schema v1.4.1
	github.com/gorilla/websocket v1.5.3
	github.com/grafana/pyroscope-go v1.2.0
	github.com/grafana/regexp v0.0.0-20240518133315-a468a5bfb3bc
	github.com/influxdata/telegraf v1.34.1
	github.com/jmoiron/sqlx v1.4.0
	github.com/json-iterator/go v1.1.12
	github.com/kr/logfmt v0.0.0-20210122060352-19f9bcb100e6
	github.com/labstack/gommon v0.4.2
	github.com/lestrrat-go/file-rotatelogs v2.4.0+incompatible
	github.com/m3db/prometheus_remote_client_golang v0.4.4
	github.com/metrico/cloki-config v0.0.89
	github.com/mochi-co/mqtt v1.3.2
	github.com/openzipkin/zipkin-go v0.4.3
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.20.5
	github.com/prometheus/common v0.63.0
	github.com/prometheus/prometheus v1.8.2-0.20220714142409-b41e0750abf5
	github.com/sirupsen/logrus v1.9.3
	github.com/stretchr/testify v1.11.1
	github.com/valyala/bytebufferpool v1.0.0
	github.com/valyala/fasthttp v1.66.0
	github.com/valyala/fastjson v1.6.4
	go.opentelemetry.io/collector/pdata v1.41.0
	go.opentelemetry.io/proto/otlp v1.8.0
	golang.org/x/exp v0.0.0-20250106191152-7588d65b2ba8
	golang.org/x/sync v0.17.0
	google.golang.org/grpc v1.75.0
	google.golang.org/protobuf v1.36.8
	gopkg.in/go-playground/validator.v9 v9.31.0
	gopkg.in/yaml.v2 v2.4.0
)

require (
	cel.dev/expr v0.24.0 // indirect
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/Masterminds/goutils v1.1.1 // indirect
	github.com/Masterminds/semver v1.5.0 // indirect
	github.com/alecthomas/units v0.0.0-20240626203959-61d1e3462e30 // indirect
	github.com/andybalholm/brotli v1.2.0 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/awnumar/memcall v0.3.0 // indirect
	github.com/awnumar/memguard v0.22.5 // indirect
	github.com/aws/aws-sdk-go v1.55.5 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/compose-spec/compose-go v1.20.2 // indirect
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dennwc/varint v1.0.0 // indirect
	github.com/dmarkham/enumer v1.6.1 // indirect
	github.com/edsrzf/mmap-go v1.1.0 // indirect
	github.com/fasthttp/websocket v1.5.3 // indirect
	github.com/fatih/color v1.18.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.7.0 // indirect
	github.com/go-faster/errors v0.7.1 // indirect
	github.com/go-kit/log v0.2.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-playground/locales v0.14.0 // indirect
	github.com/go-playground/universal-translator v0.18.0 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt/v5 v5.2.2 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/cel-go v0.23.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grafana/pyroscope-go/godeltaprof v0.1.8 // indirect
	github.com/hashicorp/go-version v1.7.0 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/huandu/xstrings v1.5.0 // indirect
	github.com/imdario/mergo v0.3.16 // indirect
	github.com/influxdata/toml v0.0.0-20190415235208-270119a8ce65 // indirect
	github.com/jedib0t/go-pretty/v6 v6.6.5 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/jonboulle/clockwork v0.4.0 // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/julienschmidt/httprouter v1.3.0 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/klauspost/pgzip v1.2.6 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/leodido/go-urn v1.2.1 // indirect
	github.com/lestrrat-go/strftime v1.1.0 // indirect
	github.com/magiconair/properties v1.8.9 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/mcuadros/go-defaults v1.2.0 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/mapstructure v1.5.1-0.20220423185008-bf980b35cac4 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/mwitkow/go-conntrack v0.0.0-20190716064945-2f068394615f // indirect
	github.com/naoina/go-stringutil v0.1.0 // indirect
	github.com/oklog/ulid v1.3.1 // indirect
	github.com/pascaldekloe/name v1.0.1 // indirect
	github.com/paulmach/orb v0.11.1 // indirect
	github.com/pelletier/go-toml/v2 v2.0.8 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common/sigv4 v0.1.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/rs/xid v1.5.0 // indirect
	github.com/savsgio/gotils v0.0.0-20230208104028-c358bd845dee // indirect
	github.com/segmentio/asm v1.2.1 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/spf13/afero v1.11.0 // indirect
	github.com/spf13/cast v1.7.1 // indirect
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/spf13/viper v1.16.0 // indirect
	github.com/stoewer/go-strcase v1.3.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/subosito/gotenv v1.4.2 // indirect
	github.com/tidwall/gjson v1.18.0 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/tidwall/tinylru v1.2.1 // indirect
	github.com/tidwall/wal v1.1.8 // indirect
	github.com/valyala/fasttemplate v1.2.2 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.41.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.59.0 // indirect
	go.opentelemetry.io/otel v1.38.0 // indirect
	go.opentelemetry.io/otel/metric v1.38.0 // indirect
	go.opentelemetry.io/otel/trace v1.38.0 // indirect
	go.step.sm/crypto v0.59.1 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/goleak v1.3.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	golang.org/x/crypto v0.42.0 // indirect
	golang.org/x/mod v0.27.0 // indirect
	golang.org/x/net v0.44.0 // indirect
	golang.org/x/oauth2 v0.30.0 // indirect
	golang.org/x/sys v0.36.0 // indirect
	golang.org/x/text v0.29.0 // indirect
	golang.org/x/time v0.10.0 // indirect
	golang.org/x/tools v0.36.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250825161204-c5933d9347a5 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250825161204-c5933d9347a5 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
