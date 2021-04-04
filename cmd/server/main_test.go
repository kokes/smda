package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"sync"
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
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := run(ctx, filepath.Join(dirname, "tmp"), 1234, 1235, false, false, false, "", ""); err != nil {
			panic(err)
		}
	}()

	cancel()
	wg.Wait()
}

func TestLoadingSamples(t *testing.T) {
	dirname, err := os.MkdirTemp("", "loading_samples")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := run(ctx, filepath.Join(dirname, "tmp"), 1236, 1237, false, true, false, "", ""); err != nil {
			panic(err)
		}
	}()

	cancel()
	wg.Wait()
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

	if err := run(context.Background(), filepath.Join(dirname, "tmp"), 1235, 1236, false, false, false, "", ""); err == nil {
		t.Fatal("expecting launching with a port busy errs, it did not")
	}
}

// test exposure (it will trigger the macOS firewall)
