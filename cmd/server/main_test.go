package main

import (
	"net"
	"os"
	"path/filepath"
	"testing"
)

// ARCH: many of these tests duplicate what's in router_test.go - maybe move some of the
// router logic to `handlers` (setupRoutes) and the rest to this main.go?

// TODO(next): can we perhaps create certs for testing (on the fly), to test TLS, http->https redirects etc.

func TestRunningServer(t *testing.T) {
	dirname, err := os.MkdirTemp("", "running_server")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		if err := run(filepath.Join(dirname, "tmp"), 1234, 1235, false, false, false, "", ""); err != nil {
			panic(err)
		}
	}()
	// ARCH: no explicit server close, resources dangling...
	// TODO: resolve using context propagation, signal handling, cancellation throughout
}

func TestLoadingSamples(t *testing.T) {
	dirname, err := os.MkdirTemp("", "loading_samples")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		if err := run(filepath.Join(dirname, "tmp"), 1236, 1237, false, true, false, "", ""); err != nil {
			panic(err)
		}
	}()
	// ARCH: no explicit server close, resources dangling...
}

func TestBusyPort(t *testing.T) {
	listener, err := net.Listen("tcp", "localhost:1235")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	dirname, err := os.MkdirTemp("", "busy_port")
	if err != nil {
		t.Fatal(err)
	}

	if err := run(filepath.Join(dirname, "tmp"), 1235, 1236, false, false, false, "", ""); err == nil {
		t.Fatal("expecting launching with a port busy errs, it did not")
	}
}

// test exposure (it will trigger the macOS firewall)
