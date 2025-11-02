package apihttp

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/metrico/qryn/v4/logger"
	controllerv1 "github.com/metrico/qryn/v4/writer/controller"
)

type MiddlewareFunc func(w http.ResponseWriter, r *http.Request) error

type Router struct {
	routes         map[string]map[string]http.HandlerFunc // method -> path -> handler
	AuthMiddleware MiddlewareFunc
}

func NewRouter() *Router {
	return &Router{
		routes: make(map[string]map[string]http.HandlerFunc),
	}
}

// Define RouterHandleFunc to wrap router.HandleFunc with logging middleware
func (router *Router) RouterHandleFunc(method string, path string, controller controllerv1.Requester) {
	router.HandleFunc(method, path, LogStatusMiddleware(func(w http.ResponseWriter, r *http.Request) {
		err := controller(w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}))
}

func (router *Router) HandleFunc(method, path string, handlerFunc http.HandlerFunc) {
	if _, ok := router.routes[method]; !ok {
		router.routes[method] = make(map[string]http.HandlerFunc)
	}
	router.routes[method][path] = handlerFunc
}

func (router *Router) Handle(method, path string, handler http.HandlerFunc) {
	router.HandleFunc(method, path, handler)
}

// ServeHTTP handles incoming HTTP requests and routes them to the appropriate handler based on the registered routes.
func (router *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if handlers, ok := router.routes[r.Method]; ok {
		for routePath, handler := range handlers {
			// Check if the request path matches the registered route path
			params, match := matchPath(routePath, r.URL.Path)
			if match {
				// Set parameters in the request context
				r = r.WithContext(context.WithValue(r.Context(), "params", params))

				// Call the handler function
				if router.AuthMiddleware != nil {
					if err := router.AuthMiddleware(w, r); err != nil {
						logger.Error("Auth middleware failed: ", err)
						return
					}
				}
				handler(w, r)
				return
			}
		}
	}
	http.Error(w, "404 page not found", http.StatusNotFound)
}

// matchPath matches a request path to a registered route path with parameters.
// It returns a map of parameter names to values and a boolean indicating if the paths match.
func matchPath(routePath, requestPath string) (map[string]string, bool) {
	routeParts := strings.Split(routePath, "/")
	requestParts := strings.Split(requestPath, "/")

	if len(routeParts) != len(requestParts) {
		return nil, false
	}

	params := make(map[string]string)
	for i, part := range routeParts {
		if strings.HasPrefix(part, ":") {
			params[strings.TrimPrefix(part, ":")] = requestParts[i]
		} else if part != requestParts[i] {
			return nil, false
		}
	}

	return params, true
}

// LogStatusMiddleware is a middleware function to capture and log the status code
func LogStatusMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		// Create a custom ResponseWriter to capture the status code
		statusWriter := &statusResponseWriter{ResponseWriter: w}

		// Call the next handler with the custom ResponseWriter
		next(statusWriter, r)
		// Calculate the duration
		duration := time.Since(start)

		// Log information about the incoming request using logrus
		logInFO := logger.LogInfo{
			"[code]":   statusWriter.Status(),
			"method":   r.Method,
			"path":     r.URL.Path,
			"query":    r.URL.RawQuery,
			"duration": duration,
		}
		// Log response headers
		headers := make(map[string]string)
		for key, values := range w.Header() {
			headers[key] = strings.Join(values, ", ")
		}

		// Assign response headers to logInFO
		logInFO["response_headers"] = headers

		// Log the entire information
		logger.Info("HTTP request", logInFO)
	}
}

// statusResponseWriter is a custom ResponseWriter to capture the status code
type statusResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

// WriteHeader captures the status code
func (w *statusResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

// Status returns the captured status code
func (w *statusResponseWriter) Status() int {
	return w.statusCode
}
