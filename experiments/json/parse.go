package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	input := strings.NewReader(`[{"foo": 12, "bar": [1,2], "nested": {"value": 12}}]`)

	dec := json.NewDecoder(input)
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		fmt.Printf("%T: %v\n", tok, tok)
	}
	return nil
}
