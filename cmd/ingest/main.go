// TODO: nothing in the Makefile for this; no docs
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	port := flag.Int("port", 8822, "port where the smda server is running")
	flag.Parse()
	arg := flag.Arg(0)

	// TODO: support stdin
	if arg == "" {
		return errors.New("need to supply a file to ingest")
	}
	f, err := os.Open(arg)
	if err != nil {
		return err
	}
	defer f.Close()

	url := fmt.Sprintf("http://localhost:%d/upload/auto", *port)
	br := bufio.NewReader(f)

	resp, err := http.Post(url, "encoding/csv", br)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(os.Stdout, resp.Body)
	return nil
}
