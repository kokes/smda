package web

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/kokes/smda/src/database"
	"github.com/kokes/smda/src/query"
)

func handleRoot(db *database.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.Error(w, "file not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		// TODO: go generate or go-bindata (and there are plans to have this natively in Go)
		fn := "cmd/server/index.html"
		// this is an ugly hack for tests to find the asset (since their working directory is /src, not the root)
		// will get resolved once we have our assets in the binary
		wd, err := os.Getwd()
		if err != nil {
			// ARCH: we panic here in a few places (mostly when writing stuff to `w`) - is it too late to explicitly send a 500?
			panic(err)
		}
		if filepath.Base(wd) == "web" {
			fn = "../../cmd/server/index.html"
		}
		http.ServeFile(w, r, fn)
	}
}

func handleStatus(db *database.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status": "ok"}`)
	}
}

func handleDatasets(db *database.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// might be a bottleneck to indent it, but what the heck at this point
		// this is quite dangerous as there may be new fields that get automatically marshalled here
		if err := json.NewEncoder(w).Encode(db.Datasets); err != nil {
			panic(err)
		}
	}
}

func handleQuery(db *database.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, "only POST requests allowed for /api/query", http.StatusMethodNotAllowed)
			return
		}
		var qr query.Query
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&qr); err != nil {
			http.Error(w, fmt.Sprintf("did not supply correct query parameters: %v", err), http.StatusBadRequest)
			return
		}
		// NewDecoder(r).Decode() can lead to bugs: https://github.com/golang/go/issues/36225
		if dec.More() {
			http.Error(w, "body can only contain a single JSON object", http.StatusBadRequest)
			return
		}
		res, err := query.Run(db, qr)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed this query: %v", err), http.StatusInternalServerError)
			return
		}
		resp, err := json.Marshal(res)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to serialise query results: %v", err), http.StatusInternalServerError)
		}
		w.Write(resp)
	}
}

func handleUpload(db *database.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, "only POST requests allowed for /upload/raw", http.StatusMethodNotAllowed)
			return
		}
		// there are two reasons we don't operate on r.Body directly:
		// 1) we don't want to block the body read by our parser - we want to save the incoming
		// data as quickly as possible
		// 2) we want to have a local copy if we need to reprocess it
		ds := database.NewDataset()
		ds.Name = r.URL.Query().Get("name")

		if err := database.CacheIncomingFile(r.Body, db.DatasetPath(ds)); err != nil {
			http.Error(w, "could not upload file", http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()

		if err := json.NewEncoder(w).Encode(ds); err != nil {
			http.Error(w, fmt.Sprintf("failed to cache data: %v", err), http.StatusInternalServerError)
			return
		}
	}
}

// this will load the data, but also infer the schema and automatically load it with it
// the part with `loadDatasetFromLocalFileAuto` is potentially slow - do we want to make this asynchronous?
//   that is - we load the raw data and return a jobID - and let the requester ping the server backend for status
func handleAutoUpload(db *database.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "only POST requests allowed for /upload/auto", http.StatusMethodNotAllowed)
			return
		}

		ds, err := db.LoadDatasetFromReaderAuto(r.Body)
		defer r.Body.Close()
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to parse a given file: %v", err), http.StatusInternalServerError)
			return
		}
		ds.Name = r.URL.Query().Get("name")
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(ds); err != nil {
			panic(err)
		}
	}
}
