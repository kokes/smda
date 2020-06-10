package smda

import (
	"errors"
	"log"
	"net"
	"net/http"
	"strconv"
)

var errRequestedPortBusy = errors.New("requested port busy")
var errNoAvailablePorts = errors.New("no available ports to use")

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
func (db *Database) RunWebserver(port int, ensurePort, expose bool) error {
	db.setupRoutes()

	host := "localhost"
	if expose {
		host = ""
	}

	// we're trying to find an available port, but this is for end users only
	for j := 0; j < 100; j++ {
		nport := port + j
		address := net.JoinHostPort(host, strconv.Itoa(nport))
		listener, err := net.Listen("tcp", address)
		if err != nil {
			if err.(*net.OpError).Err.Error() == "bind: address already in use" {
				if ensurePort {
					return errRequestedPortBusy
				}
				log.Printf("port %v busy, trying %v", nport, nport+1)
				continue
			}

			return err
		}
		db.server.Addr = address
		log.Printf("listening on %v", address)
		// this won't immediately return, only when shut down
		return db.server.Serve(listener)
	}
	return errNoAvailablePorts
}
