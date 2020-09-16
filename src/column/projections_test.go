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

func TestComparisons(t *testing.T) {
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
		{DtypeInt, EvalEq, 3, "lit:1", "lit:2", "lit:f"},
		{DtypeFloat, EvalEq, 3, "lit:1", "lit:2", "lit:f"},
	}
	litPrefix := "lit:"
	for _, test := range tests {
		// TODO: abstract out all of this into a chunks function helper (it's used elsewhere as well)
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

		if strings.HasPrefix(test.expected, litPrefix) {
			expected = NewChunkLiteralAuto(strings.TrimPrefix(test.expected, litPrefix), test.nrows)
		} else {
			if err := expected.AddValues(strings.Split(test.expected, ",")); err != nil {
				t.Error(err)
				continue
			}
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

func TestAlgebraicExpressions(t *testing.T) {
	tests := []struct {
		fnc              func(Chunk, Chunk) (Chunk, error)
		nrows            int
		dt1, dt2, dte    Dtype
		c1, c2, expected string
	}{
		// TODO: test extreme values and overflows
		// TODO: test those cases that are not supported (e.g. add ints and strings)
		// TODO: test nullable columns
		// no nullables, no literals
		{EvalAdd, 3, DtypeInt, DtypeInt, DtypeInt, "1,2,3", "4,5,6", "5,7,9"},
		{EvalAdd, 3, DtypeFloat, DtypeFloat, DtypeFloat, "1.4,2.4,3.5", "4.1,5.5,6.1", "5.5,7.9,9.6"},
		{EvalAdd, 3, DtypeInt, DtypeFloat, DtypeFloat, "1,2,3", "4.1,5.5,6.1", "5.1,7.5,9.1"},
		{EvalAdd, 3, DtypeFloat, DtypeInt, DtypeFloat, "4.1,5.5,6.1", "1,2,3", "5.1,7.5,9.1"},
		{EvalSubtract, 3, DtypeInt, DtypeInt, DtypeInt, "1,2,3", "1,10,2", "0,-8,1"},
		{EvalSubtract, 3, DtypeFloat, DtypeFloat, DtypeFloat, "1.2,2.3,3.3", "2.4,3.8,11.1", "-1.2,-1.5,-7.8"},
		{EvalSubtract, 3, DtypeInt, DtypeFloat, DtypeFloat, "1,2,3", "2.4,3.9,11.1", "-1.4,-1.9,-8.1"},
		{EvalSubtract, 3, DtypeFloat, DtypeInt, DtypeFloat, "2.4,3.9,11.1", "1,2,3", "1.4,1.9,8.1"},
		{EvalDivide, 2, DtypeInt, DtypeInt, DtypeFloat, "1,2", "2,8", "0.5,0.25"},
		{EvalDivide, 2, DtypeFloat, DtypeFloat, DtypeFloat, "1,2.2", "2.19,8.3", "0.4566210045662101,0.26506024096385544"},
		{EvalDivide, 2, DtypeInt, DtypeFloat, DtypeFloat, "1,2", "2.19,8.3", "0.4566210045662101,0.24096385542168672"},
		{EvalDivide, 2, DtypeFloat, DtypeInt, DtypeFloat, "1.2,3.4", "12,19", "0.09999999999999999,0.17894736842105263"},
		{EvalMultiply, 3, DtypeInt, DtypeInt, DtypeInt, "1,2,3", "100,200,300", "100,400,900"},
		{EvalMultiply, 3, DtypeFloat, DtypeFloat, DtypeFloat, "1.444,2.132,3.4124", "123.123,22.223,4.123", "177.789612,47.379436,14.0693252"},
		{EvalMultiply, 3, DtypeInt, DtypeFloat, DtypeFloat, "11,2,39", "123.123,22.223,4.123", "1354.353,44.446,160.797"},
		{EvalMultiply, 3, DtypeFloat, DtypeInt, DtypeFloat, "123.123,22.223,4.123", "11,2,39", "1354.353,44.446,160.797"},

		// literals, TODO: test more of these - both a single literal (both the first and second place) and both literals
		{EvalAdd, 3, DtypeInt, DtypeInt, DtypeInt, "lit:34", "4,5,6", "38,39,40"},
		{EvalAdd, 3, DtypeInt, DtypeInt, DtypeInt, "4,5,6", "lit:34", "38,39,40"},
		{EvalAdd, 3, DtypeInt, DtypeInt, DtypeInt, "lit:34", "lit:33", "lit:67"},
	}
	litPrefix := "lit:" // TODO: replace all this with said helper
	for _, test := range tests {
		c1, c2, expected := NewChunkFromSchema(Schema{Dtype: test.dt1}), NewChunkFromSchema(Schema{Dtype: test.dt2}), NewChunkFromSchema(Schema{Dtype: test.dte})
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

		if strings.HasPrefix(test.expected, litPrefix) {
			expected = NewChunkLiteralAuto(strings.TrimPrefix(test.expected, litPrefix), test.nrows)
		} else {
			if err := expected.AddValues(strings.Split(test.expected, ",")); err != nil {
				t.Error(err)
				continue
			}
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

// TODO: eval{add,subtract,divide,multiply}
