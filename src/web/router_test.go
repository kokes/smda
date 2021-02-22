package web

import (
	"fmt"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/kokes/smda/src/database"
)

func TestServerHappyPath(t *testing.T) {
	db, err := database.NewDatabase(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()
	port := 1234
	go func() {
		if err := RunWebserver(db, port, false); err != http.ErrServerClosed {
			panic("unable to start a webserver")
		}
	}()
	defer func() {
		if err := db.Server.Close(); err != nil {
			panic(err)
		}
	}()

	// we need for the webserver to launch - we don't have a channel to notify us
	time.Sleep(5 * time.Millisecond)
	address := net.JoinHostPort("localhost", strconv.Itoa(port))
	if _, err := http.Get(fmt.Sprintf("http://%v/status", address)); err != nil {
		t.Fatal(err)
	}
}

func TestServerClosing(t *testing.T) {
	db, err := database.NewDatabase(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	// TODO: this is a data race - but how can we force close a server, if its launch is blocking?
	// enable -race in our CI/Makefile once this is fixed
	go func() {
		time.Sleep(5 * time.Millisecond)
		if err := db.Server.Close(); err != nil {
			panic(err)
		}
	}()
	port := 1234
	if err := RunWebserver(db, port, false); err != http.ErrServerClosed {
		t.Fatalf("expecting a server to be stopped with a ErrServerClosed, got %+v", err)
	}
}

func TestBusyPort(t *testing.T) {
	db, err := database.NewDatabase(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	port := 1234
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
	if err := RunWebserver(db, port, false); err == nil {
		t.Error("server started on a busy port with port ensuring should err, got nil")
	}
}

// func (db *Database) setupRoutes() {
