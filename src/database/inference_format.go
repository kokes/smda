package database

import (
	"bufio"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"os"

	"github.com/golang/snappy"
)

type compression uint8

const (
	compressionNone compression = iota
	compressionGzip
	compressionBzip2
	compressionSnappy
)

// OPTIM: obvious reasons
func (c compression) String() string {
	return []string{"none", "gzip", "bzip2", "snappy"}[c]
}

type delimiter uint8

const (
	delimiterNone      delimiter = iota
	delimiterComma               = delimiter(',')
	delimiterSemicolon           = delimiter(';')
	delimiterTab                 = delimiter('\t')
	delimiterSpace               = delimiter(' ')
	delimiterPipe                = delimiter('|')
)

// OPTIM: obvious reasons
func (d delimiter) String() string {
	return map[delimiter]string{
		delimiterNone:      "none",
		delimiterComma:     "comma",
		delimiterSemicolon: "semicolon",
		delimiterTab:       "tab",
		delimiterSpace:     "space",
		delimiterPipe:      "pipe"}[d]
}

// https://en.wikipedia.org/wiki/List_of_file_signatures
func inferCompression(buffer []byte) compression {
	// 1) detect compression from contents, not filename
	signatures := map[compression][]byte{
		compressionGzip:  {0x1f, 0x8b},
		compressionBzip2: {0x42, 0x5A, 0x68},
		// TODO: support snappy? does it have a unified header (there are multiple formats etc.)
	}

	for ctype, signature := range signatures {
		if bytes.Equal(buffer[:len(signature)], signature) {
			return ctype
		}
	}

	return compressionNone
}

// the caller is responsible for closing this (but will they close the underlying file?
// or is that garbage collected somehow?)
func readCompressed(r io.Reader, ctype compression) (io.Reader, error) {
	switch ctype {
	case compressionNone:
		return r, nil
	case compressionGzip:
		return gzip.NewReader(r)
	case compressionBzip2:
		return bzip2.NewReader(r), nil
	case compressionSnappy:
		return snappy.NewReader(r), nil
	default:
		return nil, fmt.Errorf("cannot open a file compressed as %v", ctype)
	}
}

// this is now specifically for delimited files
// what we do is that we try to read two rows of data given various delimiters and if we succeed
// in getting the same number of entries per each row, this is our detected delimiter
// if we fail to find one this way, we try and detect it by looking up the most common character in the buffer
func inferDelimiter(buf []byte) delimiter {
	// TSVs are determined quite differently - there's no CSV specific parsing
	rows := bytes.SplitN(buf, []byte("\n"), 3)
	if len(rows) > 2 {
		tab := []byte("\t")
		r1c := len(bytes.Split(rows[0], tab))
		r2c := len(bytes.Split(rows[1], tab))
		if r1c > 1 && r1c == r2c {
			return delimiterTab
		}
	}
	for _, dlim := range []delimiter{delimiterComma, delimiterSemicolon, delimiterSpace, delimiterPipe} {
		br := bytes.NewReader(buf)
		cr := csv.NewReader(br)
		cr.Comma = rune(dlim)
		r1, err := cr.Read()
		// these err checks are quite lazy
		if err != nil {
			continue
		}
		r2, err := cr.Read()
		if err != nil {
			continue
		}
		if len(r1) > 1 && len(r1) == len(r2) {
			return dlim
		}
	}

	return delimiterNone
}

func inferCompressionAndDelimiter(path string) (compression, delimiter, error) {
	f, err := os.Open(path) // TODO(tiered)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	r := bufio.NewReader(f)

	header := make([]byte, 32)
	n, err := r.Read(header)
	if err != nil && err != io.EOF {
		return 0, 0, err
	}
	header = header[:n] // we'd otherwise have null-byte padding after whatever we loaded
	ctype := inferCompression(header)
	mr := io.MultiReader(bytes.NewReader(header), r)
	uf, err := readCompressed(mr, ctype)
	if err != nil {
		return 0, 0, err
	}
	br, err := skipBom(uf)
	if err != nil {
		return 0, 0, err
	}
	// now read some uncompressed data to determine a delimiter
	uheader := make([]byte, 64*1024)
	n, err = io.ReadFull(br, uheader)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return 0, 0, err
	}
	uheader = uheader[:n]

	dlim := inferDelimiter(uheader)

	return ctype, dlim, nil
}
