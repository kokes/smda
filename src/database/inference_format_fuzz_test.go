// +build gofuzzbeta

package database

import (
	"testing"
)

func FuzzDelimiterInference(f *testing.F) {
	f.Add(123)
	f.Fuzz(func(t *testing.T, raw int) {
		delim := inferDelimiter([]byte("..."))
		if delim == delimiterNone {
			t.Skip() // TODO(PR): are we doing this right?
		}
		t.Skip()
	})
}

// func FuzzDelimiterInference(f *testing.F) {
// 	f.Add([]byte("foo,bar,baz\n1,2,3\n4,5,6\n"))
// 	f.Fuzz(func(t *testing.T, payload []byte) {
// 		delim := inferDelimiter(payload)
// 		if delim == delimiterNone {
// 			t.Skip() // TODO(PR): are we doing this right?
// 		}
// 		t.Skip()
// 	})
// }

func FuzzCompressionInference(f *testing.F) {
	f.Add([]byte("abc"))
	f.Fuzz(func(t *testing.T, payload []byte) {
		cmp := inferCompression(payload)
		if cmp == compressionNone {
			t.Skip() // TODO(PR): are we doing this right?
		}
	})
}
