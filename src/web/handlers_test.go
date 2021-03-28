package web

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/kokes/smda/src/column"
	"github.com/kokes/smda/src/database"
	"github.com/kokes/smda/src/query"
	"github.com/kokes/smda/src/query/expr"
)

func newDatabaseWithRoutes() (*database.Database, error) {
	db, err := database.NewDatabase("", nil)
	if err != nil {
		return nil, err
	}
	db.ServerHTTP = &http.Server{
		Handler: setupRoutes(db, false, 1234),
	}
	return db, nil
}

// sooo, we're actually not testing just the handlers, we're going through the router as well
func TestStatusHandling(t *testing.T) {
	db, err := newDatabaseWithRoutes()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	srv := httptest.NewServer(db.ServerHTTP.Handler)
	defer srv.Close()

	url := fmt.Sprintf("%s/status", srv.URL)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("unexpected status: %+v", resp.Status)
	}
	ct := resp.Header.Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("unexpected content type: %+v", ct)
	}
	defer resp.Body.Close()

	var body map[string]string
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&body); err != nil {
		t.Fatal(err)
	}
	if dec.More() {
		t.Fatal("body cannot contain multiple JSON objects")
	}
	if !(len(body) == 1 && body["status"] == "ok") {
		t.Fatalf("unexpected payload: %+v", body)
	}
}

func TestRootHandling(t *testing.T) {
	db, err := newDatabaseWithRoutes()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	srv := httptest.NewServer(db.ServerHTTP.Handler)
	defer srv.Close()
	url := fmt.Sprintf("%s/", srv.URL)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("unexpected status: %+v", resp.Status)
	}
	ct := resp.Header.Get("Content-Type")
	if ct != "text/html; charset=utf-8" {
		t.Errorf("unexpected content type: %+v", ct)
	}
	defer resp.Body.Close()
	ret, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	// oh well, what else to test here?
	if !bytes.Contains(ret, []byte("<title>")) {
		t.Fatal("index does not contain a proper HTML site")
	}
}

func TestRootDoesNotHandle404(t *testing.T) {
	db, err := newDatabaseWithRoutes()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	srv := httptest.NewServer(db.ServerHTTP.Handler)
	defer srv.Close()
	for _, path := range []string{"foo", "bar", "foo/bar"} {
		url := fmt.Sprintf("%s/%s", srv.URL, path)
		resp, err := http.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != 404 {
			t.Errorf("expected path %+v to result in a 404, got %+v", path, resp.Status)
		}
		if resp.Header.Get("Content-Type") != "text/plain; charset=utf-8" {
			t.Errorf("expected a 404 to return a JSON, got: %+v", resp.Header.Get("Content-Type"))
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if strings.TrimSpace(string(body)) != "404 page not found" {
			t.Fatalf("unexpected body in a 404: %s", body)
		}
	}
}

func TestDatasetListing(t *testing.T) {
	db, err := newDatabaseWithRoutes()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	dsets := []string{"foo,bar,baz\n1,2,3\n4,5,6", "foo,bar\ntrue,false\nfalse,true"}
	for _, dset := range dsets {
		_, err := db.LoadDatasetFromReaderAuto(strings.NewReader(dset))
		if err != nil {
			t.Fatal(err)
		}
	}

	srv := httptest.NewServer(db.ServerHTTP.Handler)
	defer srv.Close()

	url := fmt.Sprintf("%s/api/datasets", srv.URL)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("unexpected status: %+v", resp.Status)
	}
	ct := resp.Header.Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("unexpected content type: %+v", ct)
	}
	defer resp.Body.Close()

	var body []struct {
		ID     string
		Name   string
		Schema []struct {
			Name     string
			Dtype    string
			Nullable bool
		}
	}
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&body); err != nil {
		t.Fatal(err)
	}
	if dec.More() {
		t.Fatal("body cannot contain multiple JSON objects")
	}
	for _, ds := range body {
		if len(ds.ID) != 18 {
			t.Errorf("unexpected dataset ID: %+v", ds.ID)
		}
		for _, col := range ds.Schema {
			if !(col.Dtype == "int" || col.Dtype == "float" || col.Dtype == "bool" || col.Dtype == "string") {
				t.Errorf("unexpected column type: %+v", col.Dtype)
			}
		}
	}
}

func TestDatasetListingNoDatasets(t *testing.T) {
	db, err := newDatabaseWithRoutes()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	srv := httptest.NewServer(db.ServerHTTP.Handler)
	defer srv.Close()

	url := fmt.Sprintf("%s/api/datasets", srv.URL)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("unexpected status: %+v", resp.Status)
	}
	ct := resp.Header.Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("unexpected content type: %+v", ct)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bytes.TrimSpace(body), []byte("[]")) {
		t.Errorf("expecting datasets listing to give us an empty array, got %+v", string(body))
	}
}

func TestQueryMethods(t *testing.T) {
	db, err := newDatabaseWithRoutes()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	srv := httptest.NewServer(db.ServerHTTP.Handler)
	defer srv.Close()

	tests := []struct {
		path   string
		method string
	}{
		{"api/query", http.MethodGet},
		{"upload/raw", http.MethodGet},
		{"upload/auto", http.MethodGet},
	}

	client := http.Client{}
	for _, test := range tests {
		url := fmt.Sprintf("%s/%s", srv.URL, test.path)
		req, err := http.NewRequest(test.method, url, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("expected a non-supported method to yield a 405, got %+v", resp.StatusCode)
		}
	}
}

func TestHandlingQueries(t *testing.T) {
	db, err := newDatabaseWithRoutes()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	dsets := []string{"foo,bar\n1,3\n4,6", "foo,bar\n9,8\n1,2"}
	dss := make([]*database.Dataset, 0, len(dsets))
	for _, dset := range dsets {
		ds, err := db.LoadDatasetFromReaderAuto(strings.NewReader(dset))
		if err != nil {
			t.Fatal(err)
		}
		if err := db.AddDataset(ds); err != nil {
			t.Fatal(err)
		}
		dss = append(dss, ds)
	}

	srv := httptest.NewServer(db.ServerHTTP.Handler)
	defer srv.Close()

	for _, ds := range dss {
		url := fmt.Sprintf("%s/api/query", srv.URL)
		limit := 100
		var cols []*expr.Expression
		for _, colSchema := range ds.Schema {
			colExpr, err := expr.ParseStringExpr(colSchema.Name)
			if err != nil {
				t.Fatal(err)
			}
			cols = append(cols, colExpr)
		}
		qr := query.Query{Select: cols, Dataset: ds.ID, Limit: &limit}
		body, err := json.Marshal(qr)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.Post(url, "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != 200 {
			t.Fatalf("unexpected status: %+v", resp.Status)
		}
		ct := resp.Header.Get("Content-Type")
		if ct != "application/json" {
			t.Errorf("unexpected content type: %+v", ct)
		}
		defer resp.Body.Close()

		var respBody struct {
			Schema database.TableSchema `json:"schema"`
			Data   [][]int              `json:"data"`
		}
		dec := json.NewDecoder(resp.Body)
		if err := dec.Decode(&respBody); err != nil {
			t.Fatal(err)
		}
		if dec.More() {
			t.Fatal("body cannot contain multiple JSON objects")
		}

		expSchema := database.TableSchema{
			column.Schema{Name: "foo", Dtype: column.DtypeInt, Nullable: false},
			column.Schema{Name: "bar", Dtype: column.DtypeInt, Nullable: false},
		}
		if !reflect.DeepEqual(expSchema, respBody.Schema) {
			t.Errorf("expected schema to be %+v, got %+v", expSchema, respBody.Schema)
		}
		if !(len(respBody.Data) == 2 && len(respBody.Data[0]) == 2) {
			t.Errorf("unexpected payload: %+v", respBody.Data)
		}
	}
}

// At this point we only test that when passed an unexpected parameter, the query fails
func TestInvalidQueries(t *testing.T) {
	db, err := newDatabaseWithRoutes()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	data := "foo\n1\n2\n3"
	ds, err := db.LoadDatasetFromReaderAuto(strings.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(db.ServerHTTP.Handler)
	defer srv.Close()

	url := fmt.Sprintf("%s/api/query", srv.URL)
	body := fmt.Sprintf(`{"dataset": "%v", "foobar": 123}`, ds.ID)
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unexpected status: %+v", resp.Status)
	}
	expErr := `did not supply correct query parameters: json: unknown field "foobar"`
	defer resp.Body.Close()
	ret, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(ret)) != expErr {
		t.Errorf("expected the query endpoint to result in %s, got %s instead", expErr, ret)
	}
}

func TestBasicRawUpload(t *testing.T) {
	db, err := newDatabaseWithRoutes()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	srv := httptest.NewServer(db.ServerHTTP.Handler)
	defer srv.Close()

	url := fmt.Sprintf("%s/upload/raw?name=test_file", srv.URL)
	body := strings.NewReader("foo,bar,baz\n1,2,3\n4,5,6")
	resp, err := http.Post(url, "text/csv", body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("unexpected status: %+v", resp.Status)
	}
	ct := resp.Header.Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("unexpected content type: %+v", ct)
	}
	defer resp.Body.Close()
	var dec database.Dataset

	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&dec); err != nil {
		t.Fatal(err)
	}
	if decoder.More() {
		t.Fatal("body cannot contain multiple JSON objects")
	}
	if dec.ID.Otype != database.OtypeDataset {
		t.Errorf("expecting an ID for a dataset")
	}
	if dec.Name != "test_file" {
		t.Errorf("expected the name to be %+v, got %+v", "test_file", dec.Name)
	}
	if dec.Schema != nil {
		t.Errorf("not expecting a schema to be present, got: %+v", dec.Schema)
	}
}

// TODO(next): test that file listing contains this dataset (we had a bug earlier that wouldn't write this metadata in)
func TestBasicAutoUpload(t *testing.T) {
	db, err := newDatabaseWithRoutes()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	srv := httptest.NewServer(db.ServerHTTP.Handler)
	defer srv.Close()

	url := fmt.Sprintf("%s/upload/auto?name=auto_file", srv.URL)
	body := strings.NewReader("foo,bar,baz\n1,2,true\n4,,false")
	resp, err := http.Post(url, "text/csv", body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("unexpected status: %+v", resp.Status)
	}
	ct := resp.Header.Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("unexpected content type: %+v", ct)
	}
	defer resp.Body.Close()
	var dec database.Dataset

	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&dec); err != nil {
		t.Fatal(err)
	}
	if decoder.More() {
		t.Fatal("body cannot contain multiple JSON objects")
	}
	if dec.Name != "auto_file" {
		t.Errorf("expected the name to be %+v, got %+v", "auto_file", dec.Name)
	}
	if dec.ID.Otype != database.OtypeDataset {
		t.Errorf("expecting an ID for a dataset")
	}
	if dec.Schema == nil {
		t.Error("expecting a schema to be present, got a nil")
	}
	es := database.TableSchema{{Name: "foo", Dtype: column.DtypeInt, Nullable: false}, {Name: "bar", Dtype: column.DtypeInt, Nullable: true}, {Name: "baz", Dtype: column.DtypeBool, Nullable: false}}
	if !reflect.DeepEqual(dec.Schema, es) {
		t.Errorf("expecting the schema to be inferred as %+v, got %+v", es, dec.Schema)
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
	db, err := newDatabaseWithRoutes()
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	srv := httptest.NewServer(db.ServerHTTP.Handler)
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
