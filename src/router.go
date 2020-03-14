package smda

import (
	"fmt"
	"log"
	"net/http"
)

func (d *Database) setupRoutes() {
	mux := http.NewServeMux()
	// there is a great Mat Ryer talk about not building all the handle* funcs as taking
	// (w, r) as arguments, but rather returning handlefuncs themselves - this allows for
	// passing in arguments, setup before the closure and other nice things
	mux.HandleFunc("/", d.handleRoot)
	mux.HandleFunc("/status", d.handleStatus)
	mux.HandleFunc("/api/datasets", d.handleDatasets)
	mux.HandleFunc("/api/query", d.handleQuery)
	mux.HandleFunc("/upload/raw", d.handleUpload)
	mux.HandleFunc("/upload/auto", d.handleAutoUpload)
	// mux.HandleFunc("/upload/infer-schema", d.handleTypeInference)

	d.server = &http.Server{
		Handler: mux,
	}
}

func (d *Database) RunWebserver(port int) {
	d.setupRoutes()

	sport := fmt.Sprintf(":%v", port)
	d.server.Addr = sport
	log.Printf("listening on http://localhost%v", sport)
	// log.Fatal(http.ListenAndServe(sport, d.mux))
	log.Fatal(d.server.ListenAndServe())
}
