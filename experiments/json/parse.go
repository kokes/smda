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
	raw := `[{"foo": 12, "bar": [1,2], "nested": {"value": 12}}]`
	fmt.Printf("got input: %v\n", raw)
	input := strings.NewReader(raw)

	dec := json.NewDecoder(input)
	var next *json.RawMessage
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if _, ok := tok.(string); ok {
			fmt.Printf("\tgot a key - looking for a value! %v\n", tok)
			if err := dec.Decode(&next); err != nil {
				return err
			}
			fmt.Printf("\tgot next: %s\n", *next)
		}
		if err != nil {
			return err
		}
		fmt.Printf("%T: %v\n", tok, tok)
	}
	return nil
}
