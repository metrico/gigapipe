package controller

import "testing"

// TestTrimTagScope: Grafana asks for the values of "resource.service.name" and
// "span.http.status_code", the attribute tables store the names without the scope.
func TestTrimTagScope(t *testing.T) {
	cases := map[string]string{
		"resource.service.name": "service.name",
		"span.http.status_code": "http.status_code",
		"service.name":          "service.name",
		"name":                  "name",
		"status":                "status",
		"resourceful.tag":       "resourceful.tag",
	}
	for in, want := range cases {
		if got := trimTagScope(in); got != want {
			t.Errorf("trimTagScope(%q) = %q, want %q", in, got, want)
		}
	}
}
