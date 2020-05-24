package smda

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
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

func TestHandlingQueries(t *testing.T) {
	db, err := NewDatabaseTemp()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(db.WorkingDirectory)
	dsets := []string{"foo,bar\n1,3\n4,6", "foo,bar\n9,8\n1,2"}
	dss := make([]*Dataset, 0, len(dsets))
	for _, dset := range dsets {
		ds, err := db.loadDatasetFromReaderAuto(strings.NewReader(dset))
		if err != nil {
			t.Fatal(err)
		}
		dss = append(dss, ds)
	}

	srv := httptest.NewServer(db.server.Handler)
	defer srv.Close()

	for _, ds := range dss {
		url := fmt.Sprintf("%s/api/query", srv.URL)
		limit := 100
		query := Query{Dataset: ds.ID, Limit: &limit}
		body, err := json.Marshal(query)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.Post(url, "application/json", bytes.NewReader(body))
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

		var dec struct {
			Columns []string `json:"columns"`
			Data    [][]int  `json:"data"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&dec); err != nil {
			t.Fatal(err)
		}

		expCol := []string{"foo", "bar"}
		if !reflect.DeepEqual(expCol, dec.Columns) {
			t.Errorf("expected the columns to be %v, got %v", expCol, dec.Columns)
		}
		if !(len(dec.Data) == 2 && len(dec.Data[0]) == 2) {
			t.Errorf("unexpected payload: %v", dec.Data)
		}
	}
}

// At this point we only test that when passed an unexpected parameter, the query fails
func TestInvalidQueries(t *testing.T) {
	db, err := NewDatabaseTemp()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(db.WorkingDirectory)

	data := "foo\n1\n2\n3"
	ds, err := db.loadDatasetFromReaderAuto(strings.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(db.server.Handler)
	defer srv.Close()

	url := fmt.Sprintf("%s/api/query", srv.URL)
	body := fmt.Sprintf(`{"dataset": "%v", "foobar": 123}`, ds.ID)
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unexpected status: %v", resp.Status)
	}
	var rerr map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&rerr); err != nil {
		t.Fatal(err)
	}
	if rerr["error"] != `did not supply correct query parameters: json: unknown field "foobar"` {
		t.Fatalf("expected query to fail with an unexpected query parameter, but got: %v", rerr["error"])
	}
}

func TestBasicRawUpload(t *testing.T) {
	db, err := NewDatabaseTemp()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(db.WorkingDirectory)

	srv := httptest.NewServer(db.server.Handler)
	defer srv.Close()

	url := fmt.Sprintf("%s/upload/raw", srv.URL)
	body := strings.NewReader("foo,bar,baz\n1,2,3\n4,5,6")
	resp, err := http.Post(url, "text/csv", body)
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
	var dec Dataset

	if err := json.NewDecoder(resp.Body).Decode(&dec); err != nil {
		t.Fatal(err)
	}
	// TODO: test .Name when we start populating it
	if dec.ID.otype != otypeDataset {
		t.Errorf("expecting an ID for a dataset")
	}
	if dec.Schema != nil {
		t.Errorf("not expecting a schema to be present, got: %v", dec.Schema)
	}
}

func TestBasicAutoUpload(t *testing.T) {
	db, err := NewDatabaseTemp()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(db.WorkingDirectory)

	srv := httptest.NewServer(db.server.Handler)
	defer srv.Close()

	url := fmt.Sprintf("%s/upload/auto", srv.URL)
	body := strings.NewReader("foo,bar,baz\n1,2,true\n4,,false")
	resp, err := http.Post(url, "text/csv", body)
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
	var dec Dataset

	if err := json.NewDecoder(resp.Body).Decode(&dec); err != nil {
		t.Fatal(err)
	}
	// TODO: test .Name when we start populating it
	if dec.ID.otype != otypeDataset {
		t.Errorf("expecting an ID for a dataset")
	}
	if dec.Schema == nil {
		t.Error("expecting a schema to be present, got a nil")
	}
	es := []columnSchema{{"foo", dtypeInt, false}, {"bar", dtypeInt, true}, {"baz", dtypeBool, false}}
	if !reflect.DeepEqual(dec.Schema, es) {
		t.Errorf("expecting the schema to be inferred as %v, got %v", es, dec.Schema)
	}

}

func randomStringFuncer(n int) func() []byte {
	return func() []byte {
		ret := make([]byte, 0, n)
		for j := 0; j < n; j++ {
			char := byte('a') + byte(rand.Intn(26))
			ret = append(ret, char)
		}
		return ret
	}
}

func randomIntFuncer(n int) func() []byte {
	return func() []byte {
		rnd := rand.Intn(n)
		rnds := strconv.Itoa(rnd)
		return []byte(rnds)
	}
}

func BenchmarkAutoUpload(b *testing.B) {
	db, err := NewDatabaseTemp()
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(db.WorkingDirectory)

	srv := httptest.NewServer(db.server.Handler)
	defer srv.Close()
	url := fmt.Sprintf("%s/upload/auto", srv.URL)

	tests := []struct {
		name       string
		ncols      int
		nrows      int
		randomData func() []byte
	}{
		{"short strings", 10, 300_000, randomStringFuncer(3)},
		{"medium strings", 10, 300_000, randomStringFuncer(15)},
		{"small ints", 10, 300_000, randomIntFuncer(128)},
		{"medium ints", 10, 300_000, randomIntFuncer(100_000)},
		{"large ints", 10, 300_000, randomIntFuncer(10_000_000_000)},
	}

	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			bf := new(bytes.Buffer)

			for nr := 0; nr < test.nrows; nr++ {
				for nc := 0; nc < test.ncols; nc++ {
					if nr == 0 {
						if _, err := fmt.Fprintf(bf, "col%v", nc+1); err != nil {
							b.Fatal(err)
						}
						sep := byte(',')
						if nc == test.ncols-1 {
							sep = '\n'
						}
						if err := bf.WriteByte(sep); err != nil {
							b.Fatal(err)
						}
						continue
					}

					if _, err := bf.Write(test.randomData()); err != nil {
						b.Fatal(err)
					}
					sep := byte(',')
					if nc == test.ncols-1 {
						sep = '\n'
					}
					if err := bf.WriteByte(sep); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.ResetTimer()
			len := bf.Len()
			for j := 0; j < b.N; j++ {
				resp, err := http.Post(url, "text/csv", bytes.NewReader(bf.Bytes()))
				if err != nil {
					b.Fatal(err)
				}
				if resp.StatusCode != 200 {
					b.Fatalf("unexpected status: %v", resp.Status)
				}
			}
			b.SetBytes(int64(len))
		})
	}
}
