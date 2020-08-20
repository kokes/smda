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

func responseError(w http.ResponseWriter, status int, error string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	resp := make(map[string]string)
	resp["error"] = error
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		panic(err)
	}
}

func handleRoot(db *database.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			responseError(w, http.StatusNotFound, "file not found")
			return
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		// TODO: go generate or go-bin (and there are plans to have this natively in Go)
		fn := "cmd/index.html"
		// this is an ugly hack for tests to find the asset (since their working directory is /src, not the root)
		// will get resolved once we have our assets in the binary
		wd, err := os.Getwd()
		if err != nil {
			panic(err)
		}
		if filepath.Base(wd) == "web" {
			fn = "../../cmd/index.html"
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
			responseError(w, http.StatusMethodNotAllowed, "only POST requests allowed for /api/query")
			return
		}
		var qr query.Query
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&qr); err != nil {
			responseError(w, http.StatusBadRequest, fmt.Sprintf("did not supply correct query parameters: %v", err))
			return
		}
		// NewDecoder(r).Decode() can lead to bugs: https://github.com/golang/go/issues/36225
		if dec.More() {
			responseError(w, http.StatusBadRequest, "body can only contain a single JSON object")
			return
		}
		res, err := query.Run(db, qr)
		if err != nil {
			responseError(w, http.StatusInternalServerError, fmt.Sprintf("failed this query: %v", err))
			return
		}
		resp, err := json.Marshal(res)
		if err != nil {
			responseError(w, http.StatusInternalServerError, fmt.Sprintf("failed to serialise query results: %v", err))
		}
		w.Write(resp)
	}
}

func handleUpload(db *database.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			responseError(w, http.StatusMethodNotAllowed, "only POST requests allowed for /upload/raw")
			return
		}
		// there are two reasons we don't operate on r.Body directly:
		// 1) we don't want to block the body read by our parser - we want to save the incoming
		// data as quickly as possible
		// 2) we want to have a local copy if we need to reprocess it
		ds := database.NewDataset()
		ds.Name = r.URL.Query().Get("name")

		if err := database.CacheIncomingFile(r.Body, db.DatasetPath(ds)); err != nil {
			responseError(w, http.StatusInternalServerError, "could not upload file")
			return
		}
		defer r.Body.Close()

		if err := json.NewEncoder(w).Encode(ds); err != nil {
			responseError(w, http.StatusInternalServerError, fmt.Sprintf("failed to cache data: %v", err))
			return
		}
	}
}

// this will load the data, but also infer the schema and automatically load it with it
// the part with `loadDatasetFromLocalFileAuto` is potentially slow - do we want to make this asynchronous?
//   that is - we load the raw data and return a jobID - and let the requester ping the server backend for status
func handleAutoUpload(db *database.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			responseError(w, http.StatusMethodNotAllowed, "only POST requests allowed for /upload/auto")
			return
		}

		ds, err := db.LoadDatasetFromReaderAuto(r.Body)
		defer r.Body.Close()
		if err != nil {
			responseError(w, http.StatusInternalServerError, fmt.Sprintf("failed to parse a given file: %v", err))
			return
		}
		ds.Name = r.URL.Query().Get("name")
		if err := json.NewEncoder(w).Encode(ds); err != nil {
			panic(err)
		}
	}
}
