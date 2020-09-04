package column

import (
	"strings"
	"testing"
)

func TestAndOr(t *testing.T) {
	tests := []struct {
		fnc              func(Chunk, Chunk) (Chunk, error)
		c1, c2, expected string
	}{
		{EvalAnd, "t", "t", "t"},
		{EvalAnd, "t", "f", "f"},
		{EvalAnd, "f", "t", "f"},
		{EvalAnd, "f", "f", "f"},
		{EvalAnd, "f,f,f,t", "f,f,f,t", "f,f,f,t"},
		{EvalAnd, "t,t,t,t", "t,t,t,t", "t,t,t,t"},
		{EvalAnd, "t,f,f,t", "t,t,f,t", "t,f,f,t"},
		{EvalOr, "t", "t", "t"},
		{EvalOr, "t", "f", "t"},
		{EvalOr, "f", "t", "t"},
		{EvalOr, "f", "f", "f"},
		{EvalOr, "f,f,f,t", "f,f,f,t", "f,f,f,t"},
		{EvalOr, "t,t,t,t", "t,t,t,t", "t,t,t,t"},
		{EvalOr, "t,f,f,t", "t,t,f,t", "t,t,f,t"},
	}
	for _, test := range tests {
		c1, c2, expected := newChunkBools(), newChunkBools(), newChunkBools()
		if err := c1.AddValues(strings.Split(test.c1, ",")); err != nil {
			t.Error(err)
			continue
		}
		if err := c2.AddValues(strings.Split(test.c2, ",")); err != nil {
			t.Error(err)
			continue
		}
		if err := expected.AddValues(strings.Split(test.expected, ",")); err != nil {
			t.Error(err)
			continue
		}

		res, err := test.fnc(c1, c2)
		if err != nil {
			t.Error(err)
			continue
		}
		if !ChunksEqual(res, expected) {
			t.Errorf("expected AND of %v and %v to result in %v, got %v instead", test.c1, test.c2, test.expected, res)
		}
	}
}

func TestEqsSameType(t *testing.T) {
	tests := []struct {
		dtype            Dtype
		fnc              func(Chunk, Chunk) (Chunk, error)
		nrows            int
		c1, c2, expected string
	}{
		// eq
		{DtypeInt, EvalEq, 3, "1,2,3", "3,3,3", "f,f,t"},
		{DtypeFloat, EvalEq, 3, "1.2,2.2,3", "3,2.2,3.0", "f,t,t"},
		{DtypeBool, EvalEq, 4, "t,t,f,t", "f,t,f,f", "f,t,t,f"},
		{DtypeBool, EvalNeq, 4, "t,t,f,t", "f,t,f,f", "t,f,f,t"},
		{DtypeString, EvalEq, 3, "foo,bar,baz", "foo,bak,baz", "t,f,t"},
		// eq with nulls
		{DtypeInt, EvalEq, 3, "1,,3", "1,2,3", "t,,t"}, // once we implement ChunksEqual here, we can compare 1,,3 and 1,0,3
		// neq
		{DtypeInt, EvalNeq, 3, "1,2,3", "3,3,3", "t,t,f"},
		{DtypeFloat, EvalNeq, 3, "1,2.0,3.1", "3,2,3", "t,f,t"},

		// literals
		{DtypeInt, EvalEq, 3, "lit:1", "3,1,2", "f,t,f"},
		{DtypeInt, EvalEq, 3, "3,1,2", "lit:1", "f,t,f"},
		{DtypeInt, EvalNeq, 3, "lit:1", "3,1,2", "t,f,t"},
		{DtypeInt, EvalNeq, 3, "3,1,2", "lit:1", "t,f,t"},
		{DtypeInt, EvalGte, 3, "lit:2", "3,1,2", "f,t,t"},
		{DtypeFloat, EvalEq, 3, "3,1,2", "lit:1.0", "f,t,f"},
		{DtypeBool, EvalNeq, 3, "lit:t", "f,t,f", "t,f,t"},
		{DtypeBool, EvalGt, 2, "t,f", "lit:t", "f,f"},
		{DtypeString, EvalGt, 3, "lit:ahoy", "ahey,boo,a", "t,f,t"},
		{DtypeString, EvalLte, 3, "ahey,boo,a", "lit:ahoy", "t,f,t"},
		// all literals
		{DtypeInt, EvalEq, 3, "lit:1", "lit:2", "f,f,f"},
		{DtypeFloat, EvalEq, 3, "lit:1", "lit:2", "f,f,f"},
	}
	litPrefix := "lit:"
	for _, test := range tests {
		schema := Schema{Dtype: test.dtype}
		c1, c2, expected := NewChunkFromSchema(schema), NewChunkFromSchema(schema), NewChunkFromSchema(Schema{Dtype: DtypeBool})
		if strings.HasPrefix(test.c1, litPrefix) {
			c1 = NewChunkLiteralAuto(strings.TrimPrefix(test.c1, litPrefix), test.nrows)
		} else {
			if err := c1.AddValues(strings.Split(test.c1, ",")); err != nil {
				t.Error(err)
				continue
			}
		}
		if strings.HasPrefix(test.c2, litPrefix) {
			c2 = NewChunkLiteralAuto(strings.TrimPrefix(test.c2, litPrefix), test.nrows)
		} else {
			if err := c2.AddValues(strings.Split(test.c2, ",")); err != nil {
				t.Error(err)
				continue
			}
		}

		if err := expected.AddValues(strings.Split(test.expected, ",")); err != nil {
			t.Error(err)
			continue
		}

		res, err := test.fnc(c1, c2)
		if err != nil {
			t.Error(err)
			continue
		}
		if !ChunksEqual(res, expected) {
			t.Errorf("expected %v and %v to result in %v, got %v instead", test.c1, test.c2, test.expected, res)
		}
	}
}
