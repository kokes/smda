package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	listener, err := net.Listen("tcp", "localhost:8824")
	if err != nil {
		return err
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		fmt.Println(conn)
		var length int32
		if err := binary.Read(conn, binary.BigEndian, &length); err != nil {
			return err
		}
		if length != 8 {
			return fmt.Errorf("unexpected length: %v", length) // TODO: this will be triggered if SSL is disabled
		}
		var payload int32
		// does NOT have to be protocol version - can be SSL request (80877103)
		// "To initiate an SSL-encrypted connection, the frontend initially sends an SSLRequest message
		// rather than a StartupMessage. The server then responds with a single byte containing S or N,
		// indicating that it is willing or unwilling to perform SSL, respectively."
		if err := binary.Read(conn, binary.BigEndian, &payload); err != nil {
			return err
		}
		// ssl request
		if payload == 80877103 {
			if _, err := conn.Write([]byte("N")); err != nil {
				return err
			}
		}
		// msglength
		if err := binary.Read(conn, binary.BigEndian, &payload); err != nil {
			return err
		}
		// read payload-4 bytes, expect StartupMessage now
		startup := make([]byte, payload-4)
		if _, err := io.ReadFull(conn, startup); err != nil {
			return err
		}
		// fmt.Printf("startup: %v\n", startup)
		sr := bufio.NewReader(bytes.NewReader(startup))
		var version int32
		if err := binary.Read(sr, binary.BigEndian, &version); err != nil {
			return err
		}
		if !(version>>16 == 3 && version&(1<<16-1) == 0) {
			return fmt.Errorf("version %v not supported", version)
		}
		// TODO: wrap into a key-value reader
		for {
			pb, err := sr.ReadByte()
			if err != nil {
				return err
			}
			if pb == 0 {
				break
			}
			sr.UnreadByte()
			// TODO: validate names? user/database/options/replication
			key, err := sr.ReadBytes(0) // TODO: strip the trailing zero
			if err != nil {
				return err
			}
			val, err := sr.ReadBytes(0)
			if err != nil {
				return err
			}
			fmt.Printf("key: %s; val: %s\n", key, val)
		}

		// if err := binary.Read(conn, binary.BigEndian, &payload); err != nil {
		// 	return err
		// }
		// fmt.Printf("got payload after kv: %v\n", payload)

		// TODO: auth ok too soon?
		// AuthenticationOk
		if _, err := conn.Write([]byte{'R'}); err != nil {
			return err
		}
		// size
		if err := binary.Write(conn, binary.BigEndian, int32(8)); err != nil {
			return err
		}
		// success
		if err := binary.Write(conn, binary.BigEndian, int32(0)); err != nil {
			return err
		}

		// ReadyForQuery
		if _, err := conn.Write([]byte{'Z'}); err != nil {
			return err
		}
		// size
		if err := binary.Write(conn, binary.BigEndian, int32(5)); err != nil {
			return err
		}
		// transaction status (idle)
		if _, err := conn.Write([]byte{'I'}); err != nil {
			return err
		}

	}
	return nil
}
