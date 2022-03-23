package database

import (
	"strings"
	"testing"
)

func FuzzColumnCleanup(f *testing.F) {
	f.Add("hello world|1foo|b a r|BarBaz") // TODO: f.Add doesn't support []string
	f.Fuzz(func(t *testing.T, raw string) {
		payload := strings.Split(raw, "|")
		res := cleanupColumns(payload)
		if len(res) != len(payload) {
			t.Fatalf("input: %v columns, output: %v columns", len(payload), len(res))
		}
		for j, col := range res {
			if len(col) == 0 {
				t.Errorf("column %v resulted in an empty result", payload[j])
			}
		}
	})
}