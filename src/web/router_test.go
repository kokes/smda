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
	port := 10000 + rand.Intn(1000)
	db, err := database.NewDatabase("", &database.Config{
		PortHTTP: port,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()
	go func() {
		if err := RunWebserver(context.Background(), db, false, "", ""); err != http.ErrServerClosed {
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
	port := 10000 + rand.Intn(1000)
	db, err := database.NewDatabase("", &database.Config{
		PortHTTP: port,
	})
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
		if err := RunWebserver(context.Background(), db, false, "", ""); err != http.ErrServerClosed {
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
	port := 10000 + rand.Intn(1000)
	db, err := database.NewDatabase("", &database.Config{
		PortHTTP: port,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

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
	if err := RunWebserver(context.Background(), db, false, "", ""); err == nil {
		t.Error("server started on a busy port with port ensuring should err, got nil")
	}
}

// func (db *Database) setupRoutes() {
