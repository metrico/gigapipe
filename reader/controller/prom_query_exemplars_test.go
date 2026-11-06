package controller

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/service"
)

// unavailableDB fails every connection, the way a ClickHouse outage does.
type unavailableDB struct{ calls int }

func (u *unavailableDB) GetDB(context.Context) (*model.DataDatabasesMap, error) {
	u.calls++
	return nil, errors.New("connection refused")
}
func (u *unavailableDB) Run()        {}
func (u *unavailableDB) Stop()       {}
func (u *unavailableDB) Ping() error { return nil }

func queryExemplars(db model.IDBRegistry, query string) *httptest.ResponseRecorder {
	ctrl := &PromQueryLabelsController{
		ExemplarsService: service.NewExemplarsService(&model.ServiceData{Session: db}),
	}
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/query_exemplars?query="+url.QueryEscape(query), nil)
	w := httptest.NewRecorder()
	ctrl.QueryExemplars(w, req)
	return w
}

// A database failure is the server's fault. Answering 400 tells the client its
// query is wrong and not to retry, and hides an outage from anything alerting
// on 5xx.
func TestQueryExemplarsReportsADatabaseFailureAsServerError(t *testing.T) {
	if w := queryExemplars(&unavailableDB{}, "up"); w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500: %s", w.Code, w.Body)
	}
}

// A query that can never succeed is the caller's fault, and is rejected before
// the database is touched.
func TestQueryExemplarsReportsAnInvalidQueryAsClientError(t *testing.T) {
	for _, query := range []string{"{{", "1 + 1"} {
		db := &unavailableDB{}
		if w := queryExemplars(db, query); w.Code != http.StatusBadRequest {
			t.Errorf("%q: status = %d, want 400: %s", query, w.Code, w.Body)
		}
		if db.calls != 0 {
			t.Errorf("%q: reached the database %d times", query, db.calls)
		}
	}
}
