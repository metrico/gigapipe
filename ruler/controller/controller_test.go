package controller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/metrico/qryn/v4/ruler"
	"gopkg.in/yaml.v3"
)

// fakeStore implements ruler.RuleStore, recording mutations and serving canned reads.
type fakeStore struct {
	set        []ruler.RuleGroup
	setNs      []string
	deleted    [][2]string // {namespace, group}
	deletedNs  []string
	getGroup   ruler.RuleGroup
	getErr     error
	allGroups  ruler.NamespaceRuleGroups
	listGroups []ruler.RuleGroup
}

func (f *fakeStore) SetRuleGroup(ctx context.Context, namespace string, group ruler.RuleGroup) error {
	f.setNs = append(f.setNs, namespace)
	f.set = append(f.set, group)
	return nil
}
func (f *fakeStore) DeleteRuleGroup(ctx context.Context, namespace, groupName string) error {
	f.deleted = append(f.deleted, [2]string{namespace, groupName})
	return nil
}
func (f *fakeStore) DeleteNamespace(ctx context.Context, namespace string) error {
	f.deletedNs = append(f.deletedNs, namespace)
	return nil
}
func (f *fakeStore) GetRuleGroup(ctx context.Context, namespace, groupName string) (ruler.RuleGroup, error) {
	return f.getGroup, f.getErr
}
func (f *fakeStore) ListRuleGroups(ctx context.Context, namespace string) ([]ruler.RuleGroup, error) {
	return f.listGroups, nil
}
func (f *fakeStore) GetAllRuleGroups(ctx context.Context) (ruler.NamespaceRuleGroups, error) {
	return f.allGroups, nil
}

func newRouter(c *Controller) *mux.Router {
	r := mux.NewRouter()
	r.HandleFunc("/loki/api/v1/rules/{namespace}", c.SetRuleGroup).Methods("POST")
	r.HandleFunc("/loki/api/v1/rules/{namespace}/{group}", c.GetRuleGroup).Methods("GET")
	r.HandleFunc("/loki/api/v1/rules/{namespace}", c.DeleteNamespace).Methods("DELETE")
	r.HandleFunc("/loki/api/v1/rules/{namespace}/{group}", c.DeleteRuleGroup).Methods("DELETE")
	r.HandleFunc("/api/v1/rules", c.PrometheusRules).Methods("GET")
	return r
}

func TestSetRuleGroup_ParsesYAMLAndStores(t *testing.T) {
	store := &fakeStore{}
	c := &Controller{Store: store}
	srv := httptest.NewServer(newRouter(c))
	defer srv.Close()

	body := "name: g1\ninterval: 30s\nrules:\n  - record: job:rate\n    expr: rate(x[5m])\n"
	resp, err := http.Post(srv.URL+"/loki/api/v1/rules/ns1", "application/yaml", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	if len(store.set) != 1 || store.setNs[0] != "ns1" {
		t.Fatalf("store not called with ns1: %+v %v", store.set, store.setNs)
	}
	if store.set[0].Name != "g1" || store.set[0].Rules[0].Record != "job:rate" {
		t.Errorf("parsed group mismatch: %+v", store.set[0])
	}
}

func TestGetRuleGroup_ReturnsYAML(t *testing.T) {
	store := &fakeStore{getGroup: ruler.RuleGroup{Name: "g1", Interval: "30s", Rules: []ruler.Rule{{Record: "r", Expr: "up"}}}}
	c := &Controller{Store: store}
	srv := httptest.NewServer(newRouter(c))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/loki/api/v1/rules/ns1/g1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var got ruler.RuleGroup
	if err := yaml.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("response not yaml: %v", err)
	}
	if got.Name != "g1" {
		t.Errorf("group name = %q, want g1", got.Name)
	}
}

func TestDeleteRuleGroup_CallsStore(t *testing.T) {
	store := &fakeStore{}
	c := &Controller{Store: store}
	srv := httptest.NewServer(newRouter(c))
	defer srv.Close()

	req, _ := http.NewRequest("DELETE", srv.URL+"/loki/api/v1/rules/ns1/g1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	if len(store.deleted) != 1 || store.deleted[0] != [2]string{"ns1", "g1"} {
		t.Errorf("delete not recorded: %v", store.deleted)
	}
}

func TestPrometheusRules_RecordingOnlyJSON(t *testing.T) {
	store := &fakeStore{allGroups: ruler.NamespaceRuleGroups{
		"ns": {{Name: "g", Interval: "30s", Rules: []ruler.Rule{
			{Record: "rec", Expr: "up"},
			{Alert: "Down", Expr: "up==0"},
		}}},
	}}
	mgr := ruler.NewRuleManager(nil, store, nil, 0)
	c := &Controller{Store: store, Manager: mgr}
	srv := httptest.NewServer(newRouter(c))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/v1/rules")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body := make([]byte, 4096)
	n, _ := resp.Body.Read(body)
	s := string(body[:n])
	if !strings.Contains(s, "recording") || !strings.Contains(s, "rec") {
		t.Errorf("expected recording rule in response: %s", s)
	}
	if strings.Contains(s, "Down") {
		t.Errorf("alerting rule must not appear: %s", s)
	}
}
