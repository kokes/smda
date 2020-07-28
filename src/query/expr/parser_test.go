package expr

import (
	"testing"
)

func TestBasicParsing(t *testing.T) {
	expressions := []string{
		"ahoy", "foo / bar", "2 * foo", "2+3*4", "count(foobar)", "bak = 'my literal'",
		"coalesce(foo, bar, 1) - 4", "nullif(baz, 'foo')", "nullif(bak, 103)",
		"round(1.234, 2)", "count(foo = true)", "bak != 3",
		"sum(foo > 3)", "sum(foo < 3)", "sum(foo >= 3)", "sum(foo <= 3)",
		"2 * (1 - foo)", "foo = true",
		"-2", "-2.4", // unary expressions
		"foo = 2 && bar = 3", "foo > 3 || foo < -2",
	}

	for _, expression := range expressions {
		if _, err := ParseStringExpr(expression); err != nil {
			t.Error(err)
		}
	}
}

// TODO: test that these expressions get parsed into what we'd expect
// func (expr *Expression) UnmarshalJSON(data []byte) error {
// expr.stringer
// MarshalJSON
