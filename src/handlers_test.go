package smda

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// sooo, we're actually not testing just the handlers, we're going through the router as well
func TestStatusHandling(t *testing.T) {
	db, err := NewDatabaseTemp()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(db.WorkingDirectory)
	srv := httptest.NewServer(db.server.Handler)
	defer srv.Close()

	url := fmt.Sprintf("%s/status", srv.URL)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("unexpected status: %v", resp.Status)
	}
	ct := resp.Header.Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("unexpected content type: %v", ct)
	}
	defer resp.Body.Close()

	var dec map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&dec); err != nil {
		t.Fatal(err)
	}
	if !(len(dec) == 1 && dec["status"] == "ok") {
		t.Fatalf("unexpected payload: %v", dec)
	}
}

func TestRootHandling(t *testing.T) {
	db, err := NewDatabaseTemp()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(db.WorkingDirectory)
	srv := httptest.NewServer(db.server.Handler)
	defer srv.Close()
	url := fmt.Sprintf("%s/", srv.URL)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("unexpected status: %v", resp.Status)
	}
	ct := resp.Header.Get("Content-Type")
	if ct != "text/html; charset=utf-8" {
		t.Errorf("unexpected content type: %v", ct)
	}
	defer resp.Body.Close()
	ret, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	// oh well, what else to test here?
	if !bytes.Contains(ret, []byte("<title>")) {
		t.Fatal("index does not contain a proper HTML site")
	}
}

func TestRootDoesNotHandle404(t *testing.T) {
	db, err := NewDatabaseTemp()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(db.WorkingDirectory)
	srv := httptest.NewServer(db.server.Handler)
	defer srv.Close()
	for _, path := range []string{"foo", "bar", "foo/bar"} {
		url := fmt.Sprintf("%s/%s", srv.URL, path)
		resp, err := http.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != 404 {
			t.Errorf("expected path %v to result in a 404, got %v", path, resp.Status)
		}
		if resp.Header.Get("Content-Type") != "application/json" {
			t.Errorf("expected a 404 to return a JSON, got: %v", resp.Header.Get("Content-Type"))
		}
		var errbody map[string]string
		if err := json.NewDecoder(resp.Body).Decode(&errbody); err != nil {
			t.Fatal(err)
		}
		if !(len(errbody) == 1 && errbody["error"] == "file not found") {
			t.Errorf("unexpected error message: %v", errbody)
		}
	}
}

func TestDatasetListing(t *testing.T) {
	db, err := NewDatabaseTemp()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(db.WorkingDirectory)
	dsets := []string{"foo,bar,baz\n1,2,3\n4,5,6", "foo,bar\ntrue,false\nfalse,true"}
	for _, dset := range dsets {
		_, err := db.loadDatasetFromReaderAuto(strings.NewReader(dset))
		if err != nil {
			t.Fatal(err)
		}
	}

	srv := httptest.NewServer(db.server.Handler)
	defer srv.Close()

	url := fmt.Sprintf("%s/api/datasets", srv.URL)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("unexpected status: %v", resp.Status)
	}
	ct := resp.Header.Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("unexpected content type: %v", ct)
	}
	defer resp.Body.Close()

	var dec []struct {
		ID     string
		Name   string
		Schema []struct {
			Name     string
			Dtype    string
			Nullable bool
		}
	}
	if err := json.NewDecoder(resp.Body).Decode(&dec); err != nil {
		t.Fatal(err)
	}
	for _, ds := range dec {
		if len(ds.ID) != 18 {
			t.Errorf("unexpected dataset ID: %v", ds.ID)
		}
		for _, col := range ds.Schema {
			if !(col.Dtype == "int" || col.Dtype == "float" || col.Dtype == "bool" || col.Dtype == "string") {
				t.Errorf("unexpected column type: %v", col.Dtype)
			}
		}
	}
}

func TestDatasetListingNoDatasets(t *testing.T) {
	db, err := NewDatabaseTemp()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(db.WorkingDirectory)

	srv := httptest.NewServer(db.server.Handler)
	defer srv.Close()

	url := fmt.Sprintf("%s/api/datasets", srv.URL)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("unexpected status: %v", resp.Status)
	}
	ct := resp.Header.Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("unexpected content type: %v", ct)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bytes.TrimSpace(body), []byte("[]")) {
		t.Errorf("expecting datasets listing to give us an empty array, got %v", string(body))
	}
}

func TestErrorsAreWrittenOut(t *testing.T) {
	tests := []struct {
		status int
		error  string
	}{
		{http.StatusInternalServerError, "failed to process"},
	}

	for _, test := range tests {
		rec := httptest.NewRecorder()
		responseError(rec, test.status, test.error)

		if rec.Result().StatusCode != test.status {
			t.Errorf("did not expect this status: %v", rec.Result().StatusCode)
		}

		if rec.Header().Get("Content-Type") != "application/json" {
			t.Errorf("unexpected content type: %v", rec.Header().Get("Content-Type"))
		}

		var resp map[string]string
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatal(err)
		}
		if !(len(resp) == 1 && resp["error"] == test.error) {
			t.Errorf("did not expect this response error: %v", resp)
		}
	}
}

// func (d *Database) handleQuery(w http.ResponseWriter, r *http.Request) {
// func (d *Database) handleUpload(w http.ResponseWriter, r *http.Request) {
// func (d *Database) handleAutoUpload(w http.ResponseWriter, r *http.Request) {
// func (d *Database) handleTypeInference(w http.ResponseWriter, r *http.Request) {
