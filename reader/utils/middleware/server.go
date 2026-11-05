package middleware

import (
	"net/http"

	clconfig "github.com/metrico/cloki-config"
)

// WrapServer wraps h in the middleware chain every gigapipe HTTP server runs,
// outermost first: Logging, Cors, AcceptEncoding, BasicAuth. The gzip writer
// depends on the logging writer's Flush/Unwrap sitting between it and the
// transport, so the order matters.
func WrapServer(cfg *clconfig.ClokiConfig, h http.Handler) http.Handler {
	if cfg.Setting.AUTH_SETTINGS.BASIC.Username != "" &&
		cfg.Setting.AUTH_SETTINGS.BASIC.Password != "" {
		h = BasicAuthMiddleware(cfg.Setting.AUTH_SETTINGS.BASIC.Username,
			cfg.Setting.AUTH_SETTINGS.BASIC.Password)(h)
	}
	h = AcceptEncodingMiddleware(h)
	if cfg.Setting.HTTP_SETTINGS.Cors.Enable {
		h = CorsMiddleware(cfg.Setting.HTTP_SETTINGS.Cors.Origin)(h)
	}
	return LoggingMiddleware("[{{.status}}] {{.method}} {{.url}} - LAT:{{.latency}}")(h)
}
