package main

import (
	"embed"
	"flag"
	"io/fs"
	"log"

	"github.com/kokes/smda/src/database"
	"github.com/kokes/smda/src/web"
)

//go:embed samples/*.csv
var sampleDir embed.FS

func main() {
	expose := flag.Bool("expose", false, "expose the server on the network, do not run it just locally")
	port := flag.Int("port", 8822, "port to listen on")
	wdir := flag.String("wdir", "tmp", "working directory for the database")
	loadSamples := flag.Bool("samples", false, "load sample datasets")
	flag.Parse()

	if err := run(*wdir, *port, *expose, *loadSamples); err != nil {
		log.Fatal(err)
	}
}

func run(wdir string, port int, expose bool, loadSamples bool) error {
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

	if err := web.RunWebserver(d, port, expose); err != nil {
		return err
	}
	return nil
}
