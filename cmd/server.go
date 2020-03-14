package main

import (
	"flag"
	"log"

	smda "github.com/kokes/smda/src"
)

func main() {
	port := flag.Int("port", 8080, "port to listen on")
	wdir := flag.String("wdir", "tmp", "working directory for the database")
	loadSamples := flag.Bool("samples", false, "load sample datasets")
	flag.Parse()

	d, err := smda.NewDatabase(*wdir)
	if err != nil {
		log.Fatal(err)
	}
	if *loadSamples {
		if err := d.LoadSampleData("samples"); err != nil {
			log.Fatal(err)
		}
	}

	d.RunWebserver(*port)
}
