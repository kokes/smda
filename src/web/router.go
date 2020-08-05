package web

import (
	"errors"
	"log"
	"net"
	"net/http"
	"strconv"

	"github.com/kokes/smda/src/database"
)

var errRequestedPortBusy = errors.New("requested port busy")
var errNoAvailablePorts = errors.New("no available ports to use")

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
func RunWebserver(db *database.Database, port int, ensurePort, expose bool) error {
	setupRoutes(db)
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
		db.Server.Addr = address
		log.Printf("listening on %v", address)
		// this won't immediately return, only when shut down
		return db.Server.Serve(listener)
	}
	return errNoAvailablePorts
}
