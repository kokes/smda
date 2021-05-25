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

const (
	authenticationOK byte = 'R'
	readyForQuery         = 'Z'
	transactionIdle       = 'I'
	queryIncoming         = 'Q'
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func readMessage(r io.Reader) ([]byte, error) {
	var length int32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, err
	}
	buf := make([]byte, length-4) // ARCH: can we do something like sizeof(int32)?
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func readMessageWithType(r io.Reader) (byte, []byte, error) {
	var (
		mtype   byte // ARCH: typed and with a stringer (TODO)
		length  int32
		payload []byte
	)
	if err := binary.Read(r, binary.BigEndian, &mtype); err != nil {
		return 0, nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return 0, nil, err
	}
	payload = make([]byte, length-4) // ARCH: sizeof
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return mtype, payload, nil
}

func readStartupMessage(r io.Reader) (map[string]string, error) {
	payload, err := readMessage(r)
	if err != nil {
		return nil, err
	}
	version := int32(binary.BigEndian.Uint32(payload[:4]))

	if !(version>>16 == 3 && version&(1<<16-1) == 0) {
		return nil, fmt.Errorf("version %v not supported", version)
	}
	log.Println("version is fine")

	params := make(map[string]string)
	sr := bufio.NewReader(bytes.NewReader(payload[4:]))
	for {
		pb, err := sr.ReadByte()
		if err != nil {
			return nil, err
		}
		if pb == 0 {
			break
		}
		sr.UnreadByte()
		// TODO: validate names? user/database/options/replication
		key, err := sr.ReadBytes(0)
		if err != nil {
			return nil, err
		}
		val, err := sr.ReadBytes(0)
		if err != nil {
			return nil, err
		}
		// stripping trailing null bytes
		params[string(key[:len(key)-1])] = string(val[:len(val)-1])
	}
	return params, nil
}

func beInt(val int32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(val))
	return buf
}

// ReadyForQuery - 'Z' + length (5) + 'I' (idle)
func sendReady(w io.Writer) error {
	rd := append(append([]byte{readyForQuery}, beInt(5)...), []byte{transactionIdle}...)
	_, err := w.Write(rd)
	return err
}

func run() error {
	listener, err := net.Listen("tcp", "localhost:8824") // TODO(PR): parametrise
	if err != nil {
		return err
	}
	for {
		conn, err := listener.Accept() // TODO(PR): goroutine; defer close?
		if err != nil {
			return err
		}
		log.Println("accepted a connection") // TODO(PR): remove all logs
		payload, err := readMessage(conn)
		if err != nil {
			return err
		}
		// does NOT have to be a StartupMessage - can be SSLRequest (80877103)
		// TODO: test cases: 1) ssl disabled, 2) ssl offered, 3) ssl required (fail)
		if bytes.Equal(payload[:4], []byte{0x04, 0xd2, 0x16, 0x2f}) {
			log.Println("rejecting SSL")
			if _, err := conn.Write([]byte("N")); err != nil {
				return err
			}
		}

		params, err := readStartupMessage(conn)
		if err != nil {
			return err
		}
		log.Printf("got params from client: %+v", params)

		// AuthenticationOK
		aok := append(append([]byte{authenticationOK}, beInt(8)...), beInt(0)...)
		if _, err := conn.Write(aok); err != nil {
			return err
		}

		if err := sendReady(conn); err != nil {
			return err
		}

		for {
			fmt.Println("reading a message")
			mtype, payload, err := readMessageWithType(conn)
			if err != nil {
				return err
			}
			// TODO: switch on mtype
			log.Printf("got %v with %s", mtype, payload)
			break
		}

		// we need to respond with RowDescription, DataRow; and CommandComplete/ErrorResponse
		// we don't quite know what RowDescription is, but we can wireshark it (try `select 1` against pg)

	}
}
