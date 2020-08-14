package column

import (
	"reflect"
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
		if !reflect.DeepEqual(res, expected) {
			t.Errorf("expected AND of %v and %v to result in %v, got %v instead", test.c1, test.c2, test.expected, res)
		}
	}
}

func TestEqsSameType(t *testing.T) {
	tests := []struct {
		dtype            Dtype
		fnc              func(Chunk, Chunk) (Chunk, error)
		c1, c2, expected string
	}{
		// eq
		{DtypeInt, EvalEq, "1,2,3", "3,3,3", "f,f,t"},
		{DtypeFloat, EvalEq, "1.2,2.2,3", "3,2.2,3.0", "f,t,t"},
		// cannot use reflect.DeepEqual on this, because we flip unused bits in here
		// will probably adapt ChunksEqual for this
		// {DtypeBool, EvalEq, "t,t,f,t", "f,t,f,f", "f,t,t,f"},
		{DtypeString, EvalEq, "foo,bar,baz", "foo,bak,baz", "t,f,t"},
		// eq with nulls
		{DtypeInt, EvalEq, "1,,3", "1,2,3", "t,,t"}, // once we implement ChunksEqual here, we can compare 1,,3 and 1,0,3
		// neq
		{DtypeInt, EvalNeq, "1,2,3", "3,3,3", "t,t,f"},
		{DtypeFloat, EvalNeq, "1,2.0,3.1", "3,2,3", "t,f,t"},
	}
	for _, test := range tests {
		schema := Schema{Dtype: test.dtype}
		c1, c2, expected := NewChunkFromSchema(schema), NewChunkFromSchema(schema), NewChunkFromSchema(Schema{Dtype: DtypeBool})
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
		if !reflect.DeepEqual(res, expected) {
			t.Errorf("expected %v and %v to result in %v, got %v instead", test.c1, test.c2, test.expected, res)
		}
	}
}
