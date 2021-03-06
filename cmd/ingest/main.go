package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
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
	stat, err = os.Stat(arg)
	if err != nil {
		return err
	}
	if stat.IsDir() {
		files, err := os.ReadDir(arg)
		if err != nil {
			return err
		}

		for _, file := range files {
			path := filepath.Join(arg, file.Name())
			if err := publishFile(path, *port); err != nil {
				return err
			}
		}
		return nil
	}

	return publishFile(arg, *port)
}

func publishFile(path string, port int) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	return publish(f, filepath.Base(path), port)
}

func publish(r io.Reader, name string, port int) error {
	kv := url.Values{}
	kv.Set("name", name)
	turl := url.URL{
		Scheme:   "http",
		Host:     net.JoinHostPort("localhost", strconv.Itoa(port)),
		Path:     "/upload/auto",
		RawQuery: kv.Encode(),
	}
	br := bufio.NewReader(r)

	resp, err := http.Post(turl.String(), "encoding/csv", br)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, err = io.Copy(os.Stdout, resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status when submitting %v: %v", name, resp.Status)
	}
	return nil
}
