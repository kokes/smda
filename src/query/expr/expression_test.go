package expr

import (
	"reflect"
	"testing"
)

func stringifySlice(exprs []*Expression) []string {
	if len(exprs) == 0 {
		return nil
	}
	ret := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		ret = append(ret, expr.String())
	}
	return ret
}

func TestAggExpr(t *testing.T) {
	tests := []struct {
		raw      string
		expected []string
		err      error
	}{
		{"1", nil, nil},
		{"nullif(foo)", nil, nil},
		{"2*nullif(foo)", nil, nil},
		{"nullif(foo)*2", nil, nil},
		{"1 + nullif(foo) - bar", nil, nil},
		{"min(a)", []string{"min(a)"}, nil},
		{"min(a) + min(b)", []string{"min(a)", "min(b)"}, nil},
		{"4*min(a) + 3-min(b)", []string{"min(a)", "min(b)"}, nil},
		{"2*nullif(min(a) + 3*min(b))", []string{"min(a)", "min(b)"}, nil},
		{"min(a)*min(b)*min(c)", []string{"min(a)", "min(b)", "min(c)"}, nil},
		// nested aggexprs
		{"min(5*min(a))", nil, errNoNestedAggregations},
		{"sum(max(b))", nil, errNoNestedAggregations},
		{"1-sum(nullif(foo, max(bar)))", nil, errNoNestedAggregations},
	}
	for _, test := range tests {
		expr, err := ParseStringExpr(test.raw)
		if err != nil {
			t.Error(err)
			continue
		}
		res, err := AggExpr(expr)
		if err != test.err {
			t.Errorf("expecting %s to result in error %+v, got %+v instead", test.raw, test.err, err)
		}
		ress := stringifySlice(res)
		if !reflect.DeepEqual(ress, test.expected) {
			t.Errorf("expected %+v to have %+v as aggregating expressions, got %+v instead", test.raw, test.expected, ress)
		}
	}
}

func TestExprStringer(t *testing.T) {
	tests := []struct {
		raw      string
		expected string
	}{
		{"1+2+ 3", "1+2+3"},
		{"1+(2+ 3)", "1+(2+3)"},
		{"max( foo) - 3", "max(foo)-3"},
		{"2 * (foo-BAR)", "2*(foo-bar)"},
		{"(foo-BAR)*2", "(foo-bar)*2"},
		{"(foo-(3-BAR))*2", "(foo-(3-bar))*2"},
		{"foo = 'bar'", "foo='bar'"},
		{"not true", "NOT TRUE"},
		{"not  (1+2+ 3)", "NOT (1+2+3)"},
	}

	for _, test := range tests {
		parsed, err := ParseStringExpr(test.raw)
		if err != nil {
			t.Fatalf("expression %+v failed: %v", test.raw, err)
			continue
		}
		if parsed.String() != test.expected {
			t.Errorf("expecting %s to parse and then stringify into %s, got %s instead", test.raw, test.expected, parsed.String())
		}
	}
}

// func (expr *Expression) UnmarshalJSON(data []byte) error {
// MarshalJSON
