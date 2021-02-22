package web

import (
	"log"
	"net"
	"net/http"
	"strconv"

	"github.com/kokes/smda/src/database"
)

func setupRoutes(db *database.Database) {
	mux := http.NewServeMux()
	// there is a great Mat Ryer talk about not building all the handle* funcs as taking
	// (w, r) as arguments, but rather returning handlefuncs themselves - this allows for
	// passing in arguments, setup before the closure and other nice things
	mux.HandleFunc("/", handleRoot(db))
	mux.HandleFunc("/status", handleStatus(db))
	mux.HandleFunc("/api/datasets", handleDatasets(db))
	mux.HandleFunc("/api/query", handleQuery(db))
	mux.HandleFunc("/upload/raw", handleUpload(db))
	mux.HandleFunc("/upload/auto", handleAutoUpload(db))
	// mux.HandleFunc("/upload/infer-schema", handleTypeInference(db))

	db.Server = &http.Server{
		Handler: mux,
	}
}

// RunWebserver sets up all the necessities for a server to run (namely routes) and launches one
func RunWebserver(db *database.Database, port int, expose bool) error {
	setupRoutes(db)
	host := "localhost"
	if expose {
		host = ""
	}

	address := net.JoinHostPort(host, strconv.Itoa(port))
	db.Server.Addr = address
	log.Printf("listening on %v", address)
	// this won't immediately return, only when shut down
	return db.Server.ListenAndServe()
}
