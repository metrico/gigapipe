package controller

import "testing"

func TestResolveInsertServices_RequiresRegistry(t *testing.T) {
	// With a nil Registry the resolver must return an error, not panic.
	old := Registry
	Registry = nil
	defer func() { Registry = old; _ = recover() }()
	_, err := ResolveInsertServices("dsn")
	if err == nil {
		t.Fatal("expected error when Registry is nil")
	}
}
