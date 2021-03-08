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
	"path/filepath"
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

	// check if there's anything on standard in
	stat, err := os.Stdin.Stat()
	if err != nil {
		return err
	}
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		return publish(os.Stdin, "standard_input_data", *port)
	}

	// otherwise ingest a given file
	if arg == "" {
		return errors.New("need to supply a file to ingest")
	}
	f, err := os.Open(arg)
	if err != nil {
		return err
	}
	defer f.Close()

	return publish(f, filepath.Base(arg), *port)
}

func publish(r io.Reader, name string, port int) error {
	url := fmt.Sprintf("http://localhost:%d/upload/auto?name=%s", port, name)
	br := bufio.NewReader(r)

	resp, err := http.Post(url, "encoding/csv", br)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, err = io.Copy(os.Stdout, resp.Body)
	return err
}
