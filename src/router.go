package smda

import (
	"fmt"
	"log"
	"net/http"
)

func (db *Database) setupRoutes() {
	mux := http.NewServeMux()
	// there is a great Mat Ryer talk about not building all the handle* funcs as taking
	// (w, r) as arguments, but rather returning handlefuncs themselves - this allows for
	// passing in arguments, setup before the closure and other nice things
	mux.HandleFunc("/", db.handleRoot)
	mux.HandleFunc("/status", db.handleStatus)
	mux.HandleFunc("/api/datasets", db.handleDatasets)
	mux.HandleFunc("/api/query", db.handleQuery)
	mux.HandleFunc("/upload/raw", db.handleUpload)
	mux.HandleFunc("/upload/auto", db.handleAutoUpload)
	// mux.HandleFunc("/upload/infer-schema", d.handleTypeInference)

	db.server = &http.Server{
		Handler: mux,
	}
}

// RunWebserver sets up all the necessities for a server to run (namely routes) and launches one
func (db *Database) RunWebserver(port int) {
	db.setupRoutes()

	sport := fmt.Sprintf(":%v", port)
	db.server.Addr = sport
	log.Printf("listening on http://localhost%v", sport)
	// log.Fatal(http.ListenAndServe(sport, d.mux))
	log.Fatal(db.server.ListenAndServe())
}
