package web

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/kokes/smda/src/database"
)

func TestServerHappyPath(t *testing.T) {
	db, err := database.NewDatabase("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()
	port := 1000 + rand.Intn(1000)
	go func() {
		if err := RunWebserver(context.Background(), db, port, port+1, false, false, "", ""); err != http.ErrServerClosed {
			panic(fmt.Sprintf("unable to start a webserver: %v", err))
		}
	}()
	defer func() {
		if err := db.ServerHTTP.Close(); err != nil {
			panic(err)
		}
	}()

	// we need for the webserver to launch - we don't have a channel to notify us
	time.Sleep(100 * time.Millisecond)
	address := net.JoinHostPort("localhost", strconv.Itoa(port))
	if _, err := http.Get(fmt.Sprintf("http://%v/status", address)); err != nil {
		t.Fatal(err)
	}
}

func TestServerClosing(t *testing.T) {
	db, err := database.NewDatabase("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		port := 1000 + rand.Intn(1000)
		if err := RunWebserver(context.Background(), db, port, port+1, false, false, "", ""); err != http.ErrServerClosed {
			panic(fmt.Sprintf("expecting a server to be stopped with a ErrServerClosed, got %+v", err))
		}
	}()

	time.Sleep(50 * time.Millisecond)
	db.Lock()
	defer db.Unlock()
	if err := db.ServerHTTP.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBusyPort(t *testing.T) {
	db, err := database.NewDatabase("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	port := 1000 + rand.Intn(1000)
	address := net.JoinHostPort("localhost", strconv.Itoa(port))
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := listener.Close(); err != nil {
			panic(err)
		}
	}()
	if err := RunWebserver(context.Background(), db, port, port+1, false, false, "", ""); err == nil {
		t.Error("server started on a busy port with port ensuring should err, got nil")
	}
}

// func (db *Database) setupRoutes() {
