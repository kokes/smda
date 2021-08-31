package expr

import (
	"strings"
	"testing"
)

func FuzzExpressionParser(f *testing.F) {
	f.Add("1+2*3")
	f.Add("foo = bar")
	f.Add("foo * 2 > 3e2")

	// from parser_test.go
	f.Add("ahoy")
	f.Add("type")
	f.Add("for")
	f.Add("struct")
	f.Add("break")
	f.Add("func")
	f.Add("\"ahoy\"")
	f.Add("\"ahoy_world\"")
	f.Add("\"ahoy62\"")
	f.Add("\"hello world\"")
	f.Add("254")
	f.Add("254.678")
	f.Add("true")
	f.Add("TRUE")
	f.Add("True")
	f.Add("false")
	f.Add("FALSE")
	f.Add("'foo'")
	f.Add("'foo bar'")
	f.Add("'foo'' bar'")
	f.Add("null")
	f.Add("NULL")
	f.Add("NULl")
	f.Add("*")
	f.Add("-2")
	f.Add("-foo")
	f.Add("-\"Some column\"")
	f.Add("NOT foo")
	f.Add("NOT true")
	f.Add("-(foo*bar)")
	f.Add("+2")
	f.Add("+2.4")
	f.Add("4 * 2")
	f.Add("4 + foo")
	f.Add("4 - foo")
	f.Add("4 / foo")
	f.Add("4 + 3 + 2")
	f.Add("4 + 3 * 2")
	f.Add("2 * \"ahoy\"")
	f.Add("foo / bar")
	f.Add("2 * foo")
	f.Add("2 + 3*4")
	f.Add("-4 / foo")
	f.Add("foo in (1, 2)")
	f.Add("foo in (1, 2) = true")
	f.Add("foo not in (1, 2)")
	f.Add("4 + 3 > 5")
	f.Add("4 + 3 >= 5")
	f.Add("4 > 3 = true")
	f.Add("foo = 'bar'")
	f.Add("'bar' = foo")
	f.Add("3 != bak")
	f.Add("bak = 'my_literal'")
	f.Add("bak = 'my_li''ter''al'")
	f.Add("foo = true")
	f.Add("foo is true")
	f.Add("foo is not true")
	f.Add("foo and bar")
	f.Add("4 > 3 AND 5 = 1")
	f.Add("foo = 2 AND 3 = bar")
	f.Add("foo > 3 OR -2 <= bar")
	f.Add("2 * (4 + 3)")
	f.Add("(4 + 3) - 2*3")
	f.Add("2 * (1 - foo)")
	f.Add("foo = 'bar' AND bak = 'bar'")
	f.Add("1 < foo < 3")
	f.Add("bar < foo < bak")
	f.Add("sum(foo < 3)")
	f.Add("sum(foo >= 3)")
	f.Add("sum(foo <= 3)")
	f.Add("count()")
	f.Add("count(foobar)")
	f.Add("count(1, 2, 3)")
	f.Add("count(1, 2*3, 3)")
	f.Add("COUNT(foobar)")
	f.Add("Count(foobar)")
	f.Add("counT(foobar)")
	f.Add("coalesce(foo, bar, 1) - 4")
	f.Add("nullif(baz, 'foo')")
	f.Add("nullif(bak, 103)")
	f.Add("round(1.234, 2)")
	f.Add("count(foo = true)")
	f.Add("sum(foo > 3)")
	f.Add("foo as bar")
	f.Add("foo bar")
	f.Add("foo as Bar")
	f.Add("foo Bar")
	f.Add("foo as \"Bar\"")
	f.Add("foo \"Bar\"")
	f.Add("1+2 as bar")
	f.Add("1+2*3 as bar")
	f.Add("1+2*3 bar")

	f.Fuzz(func(t *testing.T, raw string) {
		if _, err := ParseStringExpr(raw); err != nil {
			t.Skip()
		}
	})
}
func FuzzExpressionsParser(f *testing.F) {
	f.Add("1+2*3, true, false")
	f.Add("foo = bar, foo > 1")
	f.Add("foo * 2 > 3e2")
	f.Add("foo, bar, baz")
	f.Fuzz(func(t *testing.T, raw string) {
		if _, err := ParseStringExprs(raw); err != nil {
			t.Skip()
		}
	})
}

func FuzzSQLParser(f *testing.F) {
	f.Add("SELECT foo, bar, baz FROM bar")
	f.Add("SELECT * FROM bar")
	f.Add("SELECT * FROM bar ORDER BY bak")
	f.Add("SELECT bar, 1+2*baz FROM bar ORDER BY bak")

	// from parser_test.go
	f.Add("WITH foo")
	f.Add("SELECT 1")
	f.Add("SELECT 'foo'")
	f.Add("SELECT 1+2*3")
	f.Add("SELECT foo FROM bar")
	f.Add("SELECT * FROM bar")
	f.Add("SELECT *, foo FROM bar")
	f.Add("SELECT foo, * FROM bar")
	f.Add("SELECT foo, *, foo FROM bar")
	f.Add("SELECT foo FROM bar@v020485a2686b8d38fe WHERE foo>2")
	f.Add("SELECT foo FROM bar WHERE 1=1 AND foo>bar")
	f.Add("SELECT foo FROM bar WHERE 1=1 AND foo>bar GROUP BY foo")
	f.Add("SELECT foo FROM bar GROUP BY foo")
	f.Add("SELECT foo FROM bar GROUP BY foo LIMIT 2")
	f.Add("SELECT foo FROM bar@v020485a2686b8d38fe LIMIT 200")
	f.Add("SELECT foo FROM bar GROUP BY foo ORDER BY foo, bar")
	f.Add("SELECT foo FROM bar GROUP BY foo ORDER BY foo ASC NULLS LAST, bar")
	f.Add("SELECT foo FROM bar GROUP BY foo ORDER BY foo ASC NULLS LAST, bar DESC NULLS FIRST")
	f.Add("SELECT foo FROM bar GROUP BY foo ORDER BY foo ASC NULLS FIRST, bar DESC NULLS FIRST")
	f.Add("SELECT foo FROM bar GROUP BY foo ORDER BY foo ASC NULLS LAST, bar DESC NULLS FIRST")
	f.Add("SELECT foo FROM bar GROUP BY foo ORDER BY foo ASC NULLS LAST, bar DESC NULLS FIRST LIMIT 3")
	f.Add("SELECT foo FROM bar@234")
	f.Add("SELECT foo FROM bar GROUP for 1")
	f.Add("SELECT foo FROM bar GROUP BY foo LIMIT foo")
	f.Add("SELECT foo FROM bar GROUP BY foo ORDER on foo")
	f.Add("SELECT foo FROM bar GROUP BY foo ORDER BY foo NULLS LIMIT 100")
	f.Add("SELECT foo FROM bar GROUP BY foo ORDER BY foo NULLS BY LIMIT 100")
	f.Add("SELECT foo FROM bar GROUP BY foo ORDER BY foo ASC NULLS LIMIT 100")
	f.Add("SELECT foo FROM bar GROUP BY foo ORDER BY foo DESC NULLS LIMIT 100")
	f.Add("SELECT r FROM J@v111111D1110000000011")

	f.Fuzz(func(t *testing.T, raw string) {
		if !strings.HasPrefix(strings.ToLower(raw), "select") {
			t.Skip()
		}
		if _, err := ParseQuerySQL(raw); err != nil {
			t.Skip()
		}
	})
}
