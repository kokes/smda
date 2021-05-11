package expr

import (
	"encoding/json"
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
		{"foo as bar", "foo AS bar"},
		{"1+2*3 as bar", "1+2*3 AS bar"},
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

func TestJSONMarshaling(t *testing.T) {
	tests := []struct {
		rawExpr  string
		jsonRepr string
	}{
		// {"", ""}, // TODO(next): make this fail with a meaningful error
		{"1", `"1"`},
		{"1-2", `"1-2"`},
		{"-4 + 5", `"-4+5"`},
		{"-(foo+bar)", `"-(foo+bar)"`},
		{"not (foo + 3)", `"NOT (foo+3)"`},
		{"sum(a+b)", `"sum(a+b)"`},
	}
	for _, test := range tests {
		expr, err := ParseStringExpr(test.rawExpr)
		if err != nil {
			t.Error(err)
			continue
		}
		ret, err := json.Marshal(expr)
		if err != nil {
			t.Error(err)
			continue
		}
		if !reflect.DeepEqual(string(ret), test.jsonRepr) {
			t.Errorf("expected %v to be parsed and JSON marshaled as %v, got %s instead", test.rawExpr, test.jsonRepr, ret)
		}

		var roundtripped Expression
		if err := json.Unmarshal(ret, &roundtripped); err != nil {
			t.Error(err)
			continue
		}
		// resetting some functions to make these comparable
		expr.evaler = nil
		expr.aggregatorFactory = nil
		roundtripped.evaler = nil
		roundtripped.aggregatorFactory = nil
		if !reflect.DeepEqual(expr, &roundtripped) {
			t.Errorf("expression %v failed our JSON roundtrip - expected %v, got %v", test.rawExpr, expr, roundtripped)
		}
	}
}

func TestJSONMarshalingLists(t *testing.T) {
	tests := []struct {
		rawExpr  string
		jsonRepr string
	}{
		{"1", `"1"`},
		{"1-2", `"1-2"`},
		{"foo, bar", `"foo, bar"`},
		{"1 + 2, foo, bar*bak", `"1+2, foo, bar*bak"`},
		{"1 + 2, (foo - bar)/4+3, bar*bak", `"1+2, (foo-bar)/4+3, bar*bak"`},
	}
	for _, test := range tests {
		exprs, err := ParseStringExprs(test.rawExpr)
		if err != nil {
			t.Error(err)
			continue
		}
		ret, err := json.Marshal(exprs)
		if err != nil {
			t.Error(err)
			continue
		}
		if !reflect.DeepEqual(string(ret), test.jsonRepr) {
			t.Errorf("expected %v to be parsed and JSON marshaled as %v, got %s instead", test.rawExpr, test.jsonRepr, ret)
		}

		var roundtripped ExpressionList
		if err := json.Unmarshal(ret, &roundtripped); err != nil {
			t.Error(err)
			continue
		}
		if len(exprs) != len(roundtripped) {
			t.Errorf("expected %d elements, got %d", len(exprs), len(roundtripped))
			continue
		}
		// resetting some functions to make these comparable
		for j := range exprs {
			exprs[j].evaler = nil
			exprs[j].aggregatorFactory = nil
			roundtripped[j].evaler = nil
			roundtripped[j].aggregatorFactory = nil
		}
		if !reflect.DeepEqual(exprs, roundtripped) {
			t.Errorf("expression %v failed our JSON roundtrip - expected %v, got %v", test.rawExpr, exprs, roundtripped)
		}
	}
}
