package database

import (
	"bufio"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"fmt"
	"io"
	"os"
)

type compression int

const (
	compressionNone compression = iota
	compressionGzip
	compressionBzip2
)

// OPTIM: obvious reasons
func (c compression) String() string {
	return []string{"none", "gzip", "bzip2"}[c]
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
func inferCompression(buffer []byte) (compression, error) {
	// 1) detect compression from contents, not filename
	signatures := map[compression][]byte{
		compressionGzip:  {0x1f, 0x8b},
		compressionBzip2: {0x42, 0x5A, 0x68},
	}

	for ctype, signature := range signatures {
		if bytes.Equal(buffer[:len(signature)], signature) {
			return ctype, nil
		}
	}

	return compressionNone, nil
}

// the caller is responsible for closing this (but will they close the underlying file?
// or is that garbage collected somehow?)
func wrapCompressed(r io.Reader, ctype compression) (io.Reader, error) {
	switch ctype {
	case compressionNone:
		return r, nil
	case compressionGzip:
		return gzip.NewReader(r)
	case compressionBzip2:
		return bzip2.NewReader(r), nil
	default:
		return nil, fmt.Errorf("cannot open a file compressed as %v", ctype)
	}
}

// this is now specifically for delimited files
func inferDelimiter(buf []byte) (delimiter, error) {
	var stats [256]uint32
	for _, char := range buf {
		stats[char]++
	}
	var mostCommon delimiter
	occurences := uint32(0)
	for _, dlim := range []delimiter{delimiterComma, delimiterSemicolon, delimiterTab, delimiterSpace, delimiterPipe} {
		if stats[dlim] > occurences {
			occurences = stats[dlim]
			mostCommon = dlim
		}
	}

	// could return delimiterNone! if it could not infer it
	return mostCommon, nil
}

func inferCompressionAndDelimiter(path string) (compression, delimiter, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	r := bufio.NewReader(f)

	header := make([]byte, 64)
	n, err := r.Read(header)
	if err != nil && err != io.EOF {
		return 0, 0, err
	}
	header = header[:n] // we'd otherwise have null-byte padding after whatever we loaded
	ctype, err := inferCompression(header)
	if err != nil {
		return 0, 0, err
	}
	mr := io.MultiReader(bytes.NewReader(header), r)
	uf, err := wrapCompressed(mr, ctype)

	// now read some uncompressed data to determine a delimiter
	uheader := make([]byte, 64*1024)
	n, err = uf.Read(uheader)
	if err != nil && err != io.EOF {
		return 0, 0, err
	}
	uheader = uheader[:n]

	dlim, err := inferDelimiter(uheader)
	if err != nil {
		return 0, 0, err
	}

	return ctype, dlim, nil
}
