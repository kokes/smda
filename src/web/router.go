package web

import (
	"context"
	"log"
	"net"
	"net/http"
	"strconv"

	"github.com/kokes/smda/src/database"
)

func setupRoutes(db *database.Database, useTLS bool, portHTTPS int) http.Handler {
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

	if !useTLS {
		return mux
	}
	// if we have https enabled, we need to redirect all http traffic - we could have used HSTS or something,
	// but if https is there, let's use it unconditionally
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil {
			host, _, err := net.SplitHostPort(r.Host)
			if err != nil {
				http.Error(w, "failed to parse URL", http.StatusInternalServerError)
				return
			}
			newURL := r.URL
			newURL.Host = net.JoinHostPort(host, strconv.Itoa(portHTTPS))
			newURL.Scheme = "https"
			// ARCH: redirects are cached, do we want to set some expiration here? Or perhaps use
			// something other than a 301?
			http.Redirect(w, r, newURL.String(), http.StatusMovedPermanently)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

// RunWebserver sets up all the necessities for a server to run (namely routes) and launches one
func RunWebserver(ctx context.Context, db *database.Database, portHTTP, portHTTPS int, expose, useTLS bool, tlsCert, tlsKey string) error {
	mux := setupRoutes(db, useTLS, portHTTPS)
	host := "localhost"
	if expose {
		host = ""
	}

	errs := make(chan error)

	// http handling
	address := net.JoinHostPort(host, strconv.Itoa(portHTTP))

	db.Lock()
	db.ServerHTTP = &http.Server{
		Addr:    address,
		Handler: mux,
	}
	db.Unlock()
	log.Printf("listening on http://%v", address)
	go func() {
		errs <- db.ServerHTTP.ListenAndServe()
	}()

	if useTLS {
		address = net.JoinHostPort(host, strconv.Itoa(portHTTPS))
		log.Printf("listening on https://%v", address)
		db.Lock()
		db.ServerHTTPS = &http.Server{
			Addr:    address,
			Handler: mux,
		}
		db.Unlock()

		go func() {
			errs <- db.ServerHTTPS.ListenAndServeTLS(tlsCert, tlsKey)
		}()
	}
	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
		// ARCH(next): what errors should be returned in case of cancellation?
		return nil
	}
}
