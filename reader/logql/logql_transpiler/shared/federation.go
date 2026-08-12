package shared

import (
	"context"
	"net/http"
	"regexp"

	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
	"github.com/metrico/qryn/v5/shared/federation"
)

// oidCtxKey is the context key under which the resolved tenant filter is stored
// by the read-path pre-request plugin and read back in PopulateTableNames.
type oidCtxKey struct{}

// OidFilter is the per-request tenant scoping decision for federated reads.
//
//   - Regex non-empty, Deny false: constrain to match(oid, '^(Regex)$').
//   - Deny true: the request had no valid tenant; return nothing (a 1=0
//     predicate on the query side, or an instant empty payload in controllers).
//   - Empty (zero value): federation is off; no filtering.
type OidFilter struct {
	Regex string
	Deny  bool
}

// safeOid rejects any value that could break out of the single-quoted regex
// literal we embed in SQL. Tenants are expected to be simple identifiers or
// alternations like "platform|data"; anything with quotes, backslashes or
// control characters is refused (the caller then denies the request).
var safeOid = regexp.MustCompile(`^[A-Za-z0-9_./|:*+?()\-\[\] ]+$`)

// SanitizeOid validates a raw X-Scope-OrgID header value. ok is false when the
// value is empty or contains disallowed characters, in which case the request
// must be denied rather than filtered.
func SanitizeOid(raw string) (clean string, ok bool) {
	if raw == "" || !safeOid.MatchString(raw) {
		return "", false
	}
	return raw, true
}

// ReadOidPreRequestPlugin reads X-Scope-OrgID and resolves it to an OidFilter on
// the context. It is a no-op when federation is disabled. An empty or invalid
// header yields a Deny filter (return nothing) rather than an error, so reads
// fail closed without leaking cross-tenant data.
func ReadOidPreRequestPlugin(ctx context.Context, req *http.Request) (context.Context, error) {
	if !federation.Enabled() {
		return ctx, nil
	}
	clean, ok := SanitizeOid(req.Header.Get("X-Scope-OrgID"))
	f := OidFilter{}
	if ok {
		f.Regex = clean
	} else {
		f.Deny = true
	}
	return context.WithValue(ctx, oidCtxKey{}, f), nil
}

// OidFilterFromContext returns the resolved tenant filter stored by the
// pre-request plugin. The zero value (no filtering) is returned when federation
// is off or the plugin did not run.
func OidFilterFromContext(ctx context.Context) OidFilter {
	if ctx == nil {
		return OidFilter{}
	}
	if f, ok := ctx.Value(oidCtxKey{}).(OidFilter); ok {
		return f
	}
	return OidFilter{}
}

// WithOidFilter stores an explicit tenant filter on the context. Used by the
// ruler to scope background rule evaluation, which has no HTTP request.
func WithOidFilter(ctx context.Context, f OidFilter) context.Context {
	return context.WithValue(ctx, oidCtxKey{}, f)
}

// OidConditionFromContext builds the tenant WHERE condition directly from a Go
// context, for inline (non-PlannerContext) query builders. Returns nil when
// federation is off. Mirrors clickhouse_planner.GetOidFilter semantics: deny or
// empty -> 1=0; otherwise match(<col>, '^(regex)$'). tableAlias may be "".
func OidConditionFromContext(ctx context.Context, tableAlias string) sql.SQLCondition {
	if !federation.Enabled() {
		return nil
	}
	f := OidFilterFromContext(ctx)
	if f.Deny || f.Regex == "" {
		return sql.Eq(sql.NewIntVal(1), sql.NewIntVal(0))
	}
	col := "oid"
	if tableAlias != "" {
		col = tableAlias + ".oid"
	}
	return sql.Eq(&oidMatch{col: col, pattern: "^(" + f.Regex + ")$"}, sql.NewIntVal(1))
}

// oidMatch renders match(col, 'pattern'); a local copy so the shared package
// need not import clickhouse_planner (which would be a cycle).
type oidMatch struct {
	col     string
	pattern string
}

func (m *oidMatch) String(ctx *sql.Ctx, opts ...int) (string, error) {
	val, err := sql.NewStringVal(m.pattern).String(ctx, opts...)
	if err != nil {
		return "", err
	}
	return "match(" + m.col + ", " + val + ")", nil
}
