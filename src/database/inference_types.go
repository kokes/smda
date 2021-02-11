package database

import (
	"io"
	"os"

	"github.com/kokes/smda/src/column"
)

// inferTypes loads a file from a path and tries to determine the schema of said file.
// This is only about the schema, not the file format (delimiter, BOM, compression, ...), all
// of that is within the loadSettings struct
func inferTypes(path string, settings *loadSettings) (TableSchema, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	rl, err := newRawLoader(f, settings)
	if err != nil {
		return nil, err
	}

	row, err := rl.yieldRow()
	if err != nil {
		// this may trigger an EOF, if the input file is empty - that's fine
		return nil, err
	}
	hd := make([]string, len(row))
	copy(hd, row) // we're reusing records, so we need to copy here

	tgs := make([]*column.TypeGuesser, 0, len(hd))
	for range hd {
		tgs = append(tgs, column.NewTypeGuesser())
	}

	for {
		row, err := rl.yieldRow()
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
	ret := make(TableSchema, len(tgs))
	for j, tg := range tgs {
		ret[j] = tg.InferredType()
		ret[j].Name = hd[j]
	}

	return ret, nil
}
