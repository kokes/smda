package smda

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
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

func (db *Database) handleRoot(w http.ResponseWriter, r *http.Request) {
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
	if strings.HasSuffix(wd, "/src") {
		fn = "../cmd/index.html"
	}
	http.ServeFile(w, r, fn)
}

func (db *Database) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, `{"status": "ok"}`)
}

func (db *Database) handleDatasets(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	// might be a bottleneck to indent it, but what the heck at this point
	// this is quite dangerous as there may be new fields that get automatically marshalled here
	if err := json.NewEncoder(w).Encode(db.Datasets); err != nil {
		panic(err)
	}
}

func (db *Database) handleQuery(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	urlQuery := r.URL.Query()
	q := Query{
		Dataset: urlQuery.Get("dataset"),
	}
	res, err := db.query(q)
	if err != nil {
		responseError(w, http.StatusInternalServerError, fmt.Sprintf("failed this query: %v", err))
		return
	}
	resp, err := json.Marshal(res)
	if err != nil {
		responseError(w, http.StatusInternalServerError, fmt.Sprintf("failed to serialise query results: %v", err))
	}
	w.Write(resp)
	return
}

func (db *Database) handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		responseError(w, http.StatusMethodNotAllowed, "only POST requests allowed for /upload/raw")
		return
	}
	// there are two reasons we don't operate on r.Body directly:
	// 1) we don't want to block the body read by our parser - we want to save the incoming
	// data as quickly as possible
	// 2) we want to have a local copy if we need to reprocess it
	ds, err := db.LoadRawDataset(r.Body)
	if err != nil {
		responseError(w, http.StatusInternalServerError, fmt.Sprintf("not able to accept this file: %v", err))
		return
	}

	if err = json.NewEncoder(w).Encode(ds); err != nil {
		responseError(w, http.StatusInternalServerError, fmt.Sprintf("failed to cache data: %v", err))
		return
	}
}

// this will load the data, but also infer the schema and automatically load it with it
// the part with `loadDatasetFromLocalFileAuto` is potentially slow - do we want to make this asynchronous?
//   that is - we load the raw data and return a jobID - and let the requester ping the server backend for status
func (db *Database) handleAutoUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		responseError(w, http.StatusMethodNotAllowed, "only POST requests allowed for /upload/auto")
		return
	}
	ds, err := db.LoadRawDataset(r.Body)
	if err != nil {
		responseError(w, http.StatusInternalServerError, fmt.Sprintf("not able to accept this file: %v", err))
		return
	}

	pds, err := db.loadDatasetFromLocalFileAuto(ds.LocalFilepath)
	if err != nil {
		responseError(w, http.StatusInternalServerError, fmt.Sprintf("failed to parse a given file: %v", err))
		return
	}
	if err := json.NewEncoder(w).Encode(pds); err != nil {
		panic(err)
	}
}

// TODO: this assumes data already loaded - incorrect - we need to process data
// as it arrives (raw) - we need to reuse loadSettings and other things
// func (db *Database) handleTypeInference(w http.ResponseWriter, r *http.Request) {
// 	w.Header().Set("Content-Type", "application/json")
// 	if r.Method != http.MethodGet {
// 		responseError(w, http.StatusMethodNotAllowed, "only GET requests allowed for /type-inference")
// 		return
// 	}
// 	urlQuery := r.URL.Query()
// 	datasetID := urlQuery.Get("dataset")
// 	if datasetID == "" {
// 		responseError(w, http.StatusBadRequest, "need to submit a dataset ID")
// 		return
// 	}
// 	limitRaw := urlQuery.Get("limit")
// 	var limit int64
// 	if limitRaw != "" {
// 		var err error
// 		limit, err = strconv.ParseInt(limitRaw, 10, 64)
// 		if err != nil {
// 			responseError(w, http.StatusBadRequest, fmt.Sprintf("limit must be a number, got: %v", limitRaw))
// 			return
// 		}
// 		if limit <= 0 {
// 			responseError(w, http.StatusBadRequest, fmt.Sprintf("limit must be a positive integer, got: %v", limitRaw))
// 			return
// 		}
// 	}

// 	dataset, err := db.getDataset(datasetID)
// 	if err != nil {
// 		responseError(w, http.StatusNotFound, fmt.Sprintf("did not find dataset %v", datasetID))
// 		return
// 	}

// 	res := dataset.inferTypes(limit)

// 	if err := json.NewEncoder(w).Encode(res); err != nil {
// 		responseError(w, http.StatusInternalServerError, fmt.Sprintf("could not infer types: %v", err))
// 		return
// 	}
// 	return
// }
