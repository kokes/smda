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
func (db *Database) RunWebserver(port int, ensurePort bool) {
	db.setupRoutes()

	// we're trying to find an available port, but this is for end users only - we may need to add
	// some guarantees in the future - bind to a port XYZ or die (TODO)
	for j := 0; j < 100; j++ {
		nport := port + j
		// TODO: we set the local address to be localhost:port, but that probably won't
		// work for hosted solutions - we may need a CLI flag for this
		localAddress := fmt.Sprintf("localhost:%v", nport)
		listener, err := net.Listen("tcp", localAddress)
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
		db.server.Addr = localAddress
		log.Printf("listening on http://%v", localAddress)
		log.Fatal(db.server.Serve(listener))
	}
	log.Fatal("could not find an available port, aborting")
}
