package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

// ARCH: many of these tests duplicate what's in router_test.go - maybe move some of the
// router logic to `handlers` (setupRoutes) and the rest to this main.go?

func TestRunningServer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	port := 10000 + rand.Intn(1000)
	go func() {
		defer wg.Done()
		if err := run(ctx, filepath.Join(t.TempDir(), "tmp"), port, port+1, false, false, false, "", ""); err != nil {
			panic(err)
		}
	}()

	cancel()
	wg.Wait()
	// ARCH: all this waiting everywhere is not quite right
	time.Sleep(50 * time.Millisecond)
	listener, err := net.Listen("tcp", net.JoinHostPort("localhost", strconv.Itoa(port)))
	if err != nil {
		t.Fatalf("the port should be free, we should have shut down the server, got %v instead", err)
	}
	listener.Close()
}

func TestLoadingSamples(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := run(ctx, filepath.Join(t.TempDir(), "tmp"), 1236, 1237, false, true, false, "", ""); err != nil {
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

	if err := run(context.Background(), filepath.Join(t.TempDir(), "tmp"), 1235, 1236, false, false, false, "", ""); err == nil {
		t.Fatal("expecting launching with a port busy errs, it did not")
	}
}

func TestRunningHTTP(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	port := 10000 + rand.Intn(1000)
	go func() {
		defer wg.Done()
		if err := run(ctx, filepath.Join(t.TempDir(), "tmp"), port, port+1, false, false, false, "", ""); err != nil {
			panic(err)
		}
	}()

	time.Sleep(100 * time.Millisecond)
	for _, path := range []string{"/", "/status", "/datasets"} {
		turl := url.URL{
			Scheme: "http",
			Host:   net.JoinHostPort("localhost", strconv.Itoa(port)),
			Path:   path,
		}
		resp, err := http.Get(turl.String())
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%v: expected status OK, got %v", turl.String(), resp.StatusCode)
		}
	}

	cancel()
	wg.Wait()
}

// certs generated using mkcert
func TestRunningHTTPS(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	port := 10000 + rand.Intn(1000)
	portHttps := port + 1

	tlsKeyPath := "testdata/localhost-key.pem"
	tlsCertPath := "testdata/localhost.pem"

	go func() {
		defer wg.Done()
		if err := run(ctx, filepath.Join(t.TempDir(), "tmp"), port, portHttps, false, false, true, tlsCertPath, tlsKeyPath); err != nil {
			panic(err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	cert, err := os.ReadFile(tlsCertPath)
	if err != nil {
		t.Fatal(err)
	}
	certPool := x509.NewCertPool()
	if ok := certPool.AppendCertsFromPEM(cert); !ok {
		t.Fatalf("unable to add a certificate for making https requests")
	}

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs: certPool,
			},
		},
	}

	for _, path := range []string{"/", "/status", "/datasets"} {
		// perform requests to both http:// and https://, the former should redirect us automatically
		for _, scheme := range []string{"http", "https"} {
			host := net.JoinHostPort("localhost", strconv.Itoa(port))
			if scheme == "https" {
				host = net.JoinHostPort("localhost", strconv.Itoa(portHttps))
			}
			turl := url.URL{
				Scheme: scheme,
				Host:   host,
				Path:   path,
			}
			resp, err := client.Get(turl.String())
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("%v: expected status OK, got %v", turl.String(), resp.StatusCode)
			}
			if resp.Request.URL.Scheme != "https" {
				t.Fatalf("expecting automatic http -> https redirects, got this new URL instead: %s", resp.Request.URL.String())
			}
		}
	}

	cancel()
	wg.Wait()
}

// test exposure (it will trigger the macOS firewall)
