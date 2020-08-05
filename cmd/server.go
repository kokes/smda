package main

import (
	"flag"
	"log"

	"github.com/kokes/smda/src/database"
	"github.com/kokes/smda/src/web"
)

func main() {
	expose := flag.Bool("expose", false, "expose the server on the network, do not run it just locally")
	port := flag.Int("port", 8822, "port to listen on")
	ensurePort := flag.Bool("ensure-port", false, "if the specified port is busy, do not attept any other ports")
	wdir := flag.String("wdir", "tmp", "working directory for the database")
	loadSamples := flag.Bool("samples", false, "load sample datasets")
	flag.Parse()

	d, err := database.NewDatabase(&database.Config{
		WorkingDirectory: *wdir,
	})
	if err != nil {
		log.Fatal(err)
	}

	// for now, this is blocking, which means as soon as the site is ready, all the sample data are in there
	// it also means that if our sample data are large, the server takes that much longer to load
	// it's a tradeoff we need to keep in mind
	// once we implement automatic fetching of new datasets from the frontend, we should change this to be async
	if *loadSamples {
		if err := d.LoadSampleData("samples"); err != nil {
			log.Fatal(err)
		}
	}

	if err := web.RunWebserver(d, *port, *ensurePort, *expose); err != nil {
		log.Fatal(err)
	}
}
