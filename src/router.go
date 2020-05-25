package smda

import (
	"fmt"
	"log"
	"net"
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
func (db *Database) RunWebserver(port int, ensurePort, expose bool) {
	db.setupRoutes()

	// we're trying to find an available port, but this is for end users only - we may need to add
	// some guarantees in the future - bind to a port XYZ or die (TODO)
	for j := 0; j < 100; j++ {
		nport := port + j
		address := fmt.Sprintf("localhost:%v", nport)
		if expose {
			address = fmt.Sprintf(":%v", nport)
		}
		listener, err := net.Listen("tcp", address)
		if err != nil {
			if err.(*net.OpError).Err.Error() == "bind: address already in use" {
				if ensurePort {
					log.Fatalf("port %v busy", nport)
				}
				log.Printf("port %v busy, trying %v", nport, nport+1)
				continue
			}

			log.Fatal(err)
		}
		db.server.Addr = address
		log.Printf("listening on %v", address)
		log.Fatal(db.server.Serve(listener))
	}
	log.Fatal("could not find an available port, aborting")
}
