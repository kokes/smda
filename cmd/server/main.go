package main

import (
	"context"
	"embed"
	"flag"
	"io/fs"
	"log"
	"os"
	"os/signal"

	"github.com/kokes/smda/src/database"
	"github.com/kokes/smda/src/web"
)

//go:embed samples/*.csv
var sampleDir embed.FS

func main() {
	expose := flag.Bool("expose", false, "expose the server on the network, do not run it just locally")
	portHTTP := flag.Int("port-http", 8822, "port to listen on for http traffic")
	portHTTPS := flag.Int("port-https", 8823, "port to listen on for https traffic")
	wdir := flag.String("wdir", "tmp", "working directory for the database")
	loadSamples := flag.Bool("samples", false, "load sample datasets")
	useTLS := flag.Bool("tls", false, "use TLS when hosting the server")
	tlsCert := flag.String("tls-cert", "", "TLS certificate to use")
	tlsKey := flag.String("tls-key", "", "TLS key to use")
	flag.Parse()

	log.Printf("starting up process %v", os.Getpid())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt)
		defer signal.Stop(signals)

		select {
		case s := <-signals:
			log.Printf("signal %v received, aborting", s)
			cancel()
		case <-ctx.Done():
		}
	}()

	if err := run(ctx, *wdir, *portHTTP, *portHTTPS, *expose, *loadSamples, *useTLS, *tlsCert, *tlsKey); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, wdir string, portHTTP, portHTTPS int, expose bool, loadSamples, useTLS bool, tlsCert, tlsKey string) error {
	d, err := database.NewDatabase(wdir, nil)
	if err != nil {
		return err
	}

	// for now, this is blocking, which means as soon as the site is ready, all the sample data are in there
	// it also means that if our sample data are large, the server takes that much longer to load
	// it's a tradeoff we need to keep in mind
	// once we implement automatic fetching of new datasets from the frontend, we should change this to be async
	if loadSamples {
		samplefs, err := fs.Sub(sampleDir, "samples")
		if err != nil {
			return err
		}
		if err := d.LoadSampleData(samplefs); err != nil {
			return err
		}
	}

	return web.RunWebserver(ctx, d, portHTTP, portHTTPS, expose, useTLS, tlsCert, tlsKey)
}
