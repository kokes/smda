package database

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/kokes/smda/src/column"
)

// ARCH: consider converting non-ascii to ascii?
func cleanupColumns(columns []string) []string {
	existing := make(map[string]bool)
	ret := make([]string, 0, len(columns))
	_ = existing
	for _, col := range columns {
		col = strings.TrimSpace(col)
		chars := []byte(col)
		for j, char := range chars {
			if char >= 'A' && char <= 'Z' {
				// we could lowercase it, but we'd rather de-camel case it (if needs be)
				// chars[j] = 'a' + char - 'A'
				continue
			}
			if !((char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || (char == '_')) {
				chars[j] = '_'
			}
		}

		// remove subsequent underscores
		length := len(chars)
		for j := length - 1; j > 0; j-- {
			if (chars[j] == chars[j-1]) && (chars[j] == '_') {
				copy(chars[j-1:], chars[j:])
				length--
			}
		}
		chars = chars[:length]

		// remove camel casing
		length = len(chars)
		for j := length - 1; j >= 0; j-- {
			if chars[j] >= 'A' && chars[j] <= 'Z' {
				chars[j] = 'a' + chars[j] - 'A' // lowercase it in any case
				// and if it's preceded by a lowercase letter...
				if j > 0 && chars[j-1] >= 'a' && chars[j-1] <= 'z' {
					chars = append(chars, '0') // add an empty space at the end
					copy(chars[j+1:], chars[j:])
					chars[j] = '_'
				}
			}
		}

		chars = bytes.Trim(chars, "_")
		col = string(chars)

		if _, ok := existing[col]; ok || col == "" {
			if col == "" {
				col = "column"
			}
			base, j := col, 1
			for {
				col = fmt.Sprintf("%v_%02d", base, j)
				if _, ok := existing[col]; !ok {
					break
				}
				j++
			}
		}

		existing[col] = true
		ret = append(ret, col)
	}

	return ret
}

// inferTypes loads a file from a path and tries to determine the schema of said file.
// This is only about the schema, not the file format (delimiter, BOM, compression, ...), all
// of that is within the loadSettings struct
func inferTypes(path string, settings *loadSettings) (column.TableSchema, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	rr, err := NewRowReader(f, settings)
	if err != nil {
		return nil, err
	}

	row, err := rr.ReadRow()
	if err != nil {
		// this may trigger an EOF, if the input file is empty - that's fine
		return nil, err
	}
	// we're reusing records, so we need to copy here
	hd := make([]string, len(row))
	copy(hd, row)
	if settings.cleanupColumns {
		hd = cleanupColumns(hd)
	}

	tgs := make([]*column.TypeGuesser, 0, len(hd))
	for range hd {
		tgs = append(tgs, column.NewTypeGuesser())
	}

	for {
		row, err := rr.ReadRow()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		for j, val := range row {
			tgs[j].AddValue(val)
		}
	}
	ret := make(column.TableSchema, len(tgs))
	for j, tg := range tgs {
		ret[j] = tg.InferredType()
		ret[j].Name = hd[j]
	}

	return ret, nil
}
