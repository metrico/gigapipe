package middleware

import "net/http"

func CorsMiddleware(allowOrigin string) func(handler http.Handler) http.Handler {
	if allowOrigin == "" {
		allowOrigin = "*"
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			w.Header().Set("Access-Control-Allow-Headers",
				"Origin,Content-Type,Accept,Content-Length,Accept-Language,Accept-Encoding,Connection,Access-Control-Allow-Origin")
			w.Header().Set("Access-Control-Allow-Origin", allowOrigin)
			w.Header().Set("Access-Control-Allow-Methods", "GET,POST,HEAD,PUT,DELETE,PATCH,OPTIONS")
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			if request.Method == http.MethodOptions {
				w.WriteHeader(http.StatusOK)
				return
			}
			next.ServeHTTP(w, request)
		})
	}
}
