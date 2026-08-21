package controller

import "testing"

func TestResolveServices_RequireRegistry(t *testing.T) {
	// With a nil Registry each resolver must return an error, not panic.
	old := Registry
	Registry = nil
	defer func() { Registry = old }()
	for _, fn := range []func(string) (InsertServices, error){
		ResolveTraceServices, ResolveLogServices, ResolveProfileServices,
	} {
		if _, err := fn("dsn"); err == nil {
			t.Fatal("expected error when Registry is nil")
		}
	}
}
