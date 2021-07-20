package expr

import (
	"strings"
	"testing"
)

func FuzzExpressionParser(f *testing.F) {
	f.Add("1+2*3")
	f.Add("foo = bar")
	f.Add("foo * 2 > 3e2")
	f.Fuzz(func(t *testing.T, raw string) {
		if _, err := ParseStringExpr(raw); err != nil {
			t.Skip()
		}
	})
}

func FuzzSQLParser(f *testing.F) {
	f.Add("SELECT foo, bar, baz FROM bar")
	f.Add("SELECT * FROM bar")
	f.Add("SELECT * FROM bar ORDER BY bak")
	f.Add("SELECT bar, 1+2*baz FROM bar ORDER BY bak")

	f.Fuzz(func(t *testing.T, raw string) {
		if !strings.HasPrefix(strings.ToLower(raw), "select") {
			t.Skip()
		}
		if _, err := ParseQuerySQL(raw); err != nil {
			t.Skip()
		}
	})
}
