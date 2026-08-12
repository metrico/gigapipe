// Package controller serves the ruler HTTP API: rule-group CRUD and the
// Prometheus-format read endpoint. It is recording-rule focused. Under
// federation (shared/federation) the tenant is taken from X-Scope-OrgID: writes
// without a tenant fail, reads without a tenant return empty.
package controller

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/metrico/qryn/v5/ruler"
	"github.com/metrico/qryn/v5/shared/federation"
	"gopkg.in/yaml.v3"
)

// Controller handles ruler HTTP requests for one rule type (loki or prom),
// backed by its Store and, for the Prometheus read endpoint, its Manager.
type Controller struct {
	Store   ruler.RuleStore
	Manager *ruler.RuleManager
}

// reqOid resolves the tenant for a request. When federation is off it returns
// "" and ok=true (single-tenant). When on, a missing/empty X-Scope-OrgID makes
// ok=false so the caller can fail writes / empty reads.
func reqOid(r *http.Request) (oid string, ok bool) {
	if !federation.Enabled() {
		return "", true
	}
	oid = strings.TrimSpace(r.Header.Get("X-Scope-OrgID"))
	return oid, oid != ""
}

func writeYAML(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/yaml")
	w.WriteHeader(status)
	w.Write(body)
}

func writeSuccessJSON(w http.ResponseWriter, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": nil, "errorType": "", "error": ""})
}

// SetRuleGroup handles POST /rules/{namespace}: it parses a YAML rule group and
// stores it.
func (c *Controller) SetRuleGroup(w http.ResponseWriter, r *http.Request) {
	oid, ok := reqOid(r)
	if !ok {
		writeYAML(w, http.StatusBadRequest, []byte("error: X-Scope-OrgID header is required when federation is enabled"))
		return
	}
	namespace := mux.Vars(r)["namespace"]
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeYAML(w, http.StatusBadRequest, []byte("error: failed to read request body"))
		return
	}
	var group ruler.RuleGroup
	if err := yaml.Unmarshal(body, &group); err != nil {
		writeYAML(w, http.StatusBadRequest, []byte("error: failed to parse rule group yaml"))
		return
	}
	if err := c.Store.SetRuleGroup(r.Context(), oid, namespace, group); err != nil {
		writeYAML(w, http.StatusInternalServerError, fmt.Appendf(nil, "error: %s", err.Error()))
		return
	}
	writeSuccessJSON(w, http.StatusAccepted)
}

// GetRuleGroup handles GET /rules/{namespace}/{group}: it returns one group as YAML.
func (c *Controller) GetRuleGroup(w http.ResponseWriter, r *http.Request) {
	namespace := mux.Vars(r)["namespace"]
	groupName := mux.Vars(r)["group"]
	oid, ok := reqOid(r)
	if !ok {
		// No tenant under federation: the group is not visible.
		writeYAML(w, http.StatusNotFound, fmt.Appendf(nil,
			`message: "group does not exist: namespace=%q, name=%q"`, namespace, groupName))
		return
	}
	group, err := c.Store.GetRuleGroup(r.Context(), oid, namespace, groupName)
	if err != nil {
		writeYAML(w, http.StatusNotFound, fmt.Appendf(nil,
			`message: "group does not exist: namespace=%q, name=%q"`, namespace, groupName))
		return
	}
	yamlData, err := yaml.Marshal(group)
	if err != nil {
		writeYAML(w, http.StatusInternalServerError, []byte("error: failed to marshal group to yaml"))
		return
	}
	writeYAML(w, http.StatusOK, yamlData)
}

// RulesByNamespace handles GET /rules/{namespace}: all groups in a namespace as YAML.
func (c *Controller) RulesByNamespace(w http.ResponseWriter, r *http.Request) {
	namespace := mux.Vars(r)["namespace"]
	oid, ok := reqOid(r)
	if !ok {
		writeYAML(w, http.StatusNotFound, []byte(`message: "no rule groups found"`))
		return
	}
	groups, err := c.Store.ListRuleGroups(r.Context(), oid, namespace)
	if err != nil {
		writeYAML(w, http.StatusInternalServerError, []byte(`message: "failed to fetch rules"`))
		return
	}
	if len(groups) == 0 {
		writeYAML(w, http.StatusNotFound, []byte(`message: "no rule groups found"`))
		return
	}
	yamlData, err := yaml.Marshal(map[string][]ruler.RuleGroup{namespace: groups})
	if err != nil {
		writeYAML(w, http.StatusInternalServerError, []byte("error: failed to marshal groups to yaml"))
		return
	}
	writeYAML(w, http.StatusOK, yamlData)
}

// AllRules handles GET /rules: all groups across namespaces as YAML. Under
// federation the cross-tenant store result is filtered to the requesting tenant.
func (c *Controller) AllRules(w http.ResponseWriter, r *http.Request) {
	oid, ok := reqOid(r)
	if !ok {
		writeYAML(w, http.StatusNotFound, []byte("no rule groups found"))
		return
	}
	groups, err := c.Store.GetAllRuleGroups(r.Context())
	if err != nil {
		writeYAML(w, http.StatusInternalServerError, []byte(`message: "failed to fetch rules"`))
		return
	}
	if federation.Enabled() {
		groups = filterByOid(groups, oid)
	}
	if len(groups) == 0 {
		writeYAML(w, http.StatusNotFound, []byte("no rule groups found"))
		return
	}
	yamlData, err := yaml.Marshal(map[string][]ruler.RuleGroup(groups))
	if err != nil {
		writeYAML(w, http.StatusInternalServerError, []byte("error: failed to marshal rule groups to yaml"))
		return
	}
	writeYAML(w, http.StatusOK, yamlData)
}

// filterByOid keeps only the groups owned by oid, dropping now-empty namespaces.
func filterByOid(groups ruler.NamespaceRuleGroups, oid string) ruler.NamespaceRuleGroups {
	out := make(ruler.NamespaceRuleGroups)
	for ns, gs := range groups {
		var kept []ruler.RuleGroup
		for _, g := range gs {
			if g.Oid == oid {
				kept = append(kept, g)
			}
		}
		if len(kept) > 0 {
			out[ns] = kept
		}
	}
	return out
}

// DeleteRuleGroup handles DELETE /rules/{namespace}/{group}.
func (c *Controller) DeleteRuleGroup(w http.ResponseWriter, r *http.Request) {
	namespace := mux.Vars(r)["namespace"]
	groupName := mux.Vars(r)["group"]
	oid, ok := reqOid(r)
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"status": "error", "message": "X-Scope-OrgID header is required when federation is enabled"})
		return
	}
	if err := c.Store.DeleteRuleGroup(r.Context(), oid, namespace, groupName); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]any{"status": "error", "message": err.Error()})
		return
	}
	writeSuccessJSON(w, http.StatusAccepted)
}

// DeleteNamespace handles DELETE /rules/{namespace}.
func (c *Controller) DeleteNamespace(w http.ResponseWriter, r *http.Request) {
	namespace := mux.Vars(r)["namespace"]
	oid, ok := reqOid(r)
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"status": "error", "message": "X-Scope-OrgID header is required when federation is enabled"})
		return
	}
	if err := c.Store.DeleteNamespace(r.Context(), oid, namespace); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]any{"status": "error", "message": err.Error()})
		return
	}
	writeSuccessJSON(w, http.StatusAccepted)
}

// PrometheusRules handles GET /api/v1/rules: recording rules in Prometheus JSON
// format, including evaluation health.
func (c *Controller) PrometheusRules(w http.ResponseWriter, r *http.Request) {
	var groups []ruler.PrometheusGroup
	if c.Manager != nil {
		groups = c.Manager.GetPrometheusRules()
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":    "success",
		"errorType": "",
		"error":     "",
		"data":      map[string]any{"groups": groups},
	})
}
