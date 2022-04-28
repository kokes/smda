package web

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"

	"github.com/kokes/smda/src/database"
)

func SetupRoutes(db *database.Database) http.Handler {
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

	if !db.Config.UseTLS {
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
			newURL.Host = net.JoinHostPort(host, strconv.Itoa(db.Config.PortHTTPS))
			newURL.Scheme = "https"
			// redirects are cached, so we need to expire these in case we turn off TLS
			// this way if we try the HTTP endpoint a minute later (for some reason), the redirect
			// will be handled here, not by the browser
			w.Header().Set("Cache-Control", "max-age=60")
			// TODO: I think there's a bug here - the intention here is if we e.g. GET http://foo/
			// this will get redirected to GET https://foo/
			// BUT, this fails for POST http://foo/upload/auto, which gets redirected to GET for some reason
			// but maybe that's fine, maybe this is just for browsers...
			http.Redirect(w, r, newURL.String(), http.StatusMovedPermanently)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

// RunWebserver sets up all the necessities for a server to run (namely routes) and launches one
func RunWebserver(ctx context.Context, db *database.Database, expose bool, tlsCert, tlsKey string) error {
	mux := SetupRoutes(db)
	host := "localhost"
	if expose {
		host = ""
	}

	errs := make(chan error)

	// http handling
	address := net.JoinHostPort(host, strconv.Itoa(db.Config.PortHTTP))

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

	if db.Config.UseTLS {
		if tlsCert == "" || tlsKey == "" {
			return fmt.Errorf("if you enable TLS, you need to submit both a key and a cert")
		}

		address = net.JoinHostPort(host, strconv.Itoa(db.Config.PortHTTPS))
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
		var rval error
		if db.ServerHTTP != nil {
			log.Println("http webserver shutting down")
			if err := db.ServerHTTP.Shutdown(ctx); err != nil && err != context.Canceled {
				rval = err
			}
		}
		if db.ServerHTTPS != nil {
			log.Println("https webserver shutting down")
			if err := db.ServerHTTPS.Shutdown(ctx); err != nil && err != context.Canceled {
				rval = err
			}
		}
		return rval
	}
}
