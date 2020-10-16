package column

import (
	"strings"
	"testing"
)

func prepColumns(nrows int, dtype1, dtype2, dtype3 Dtype, rawData1, rawData2, rawData3 string) (Chunk, Chunk, Chunk, error) {
	litPrefix := "lit:"
	c1, c2, expected := NewChunkFromSchema(Schema{Dtype: dtype1}), NewChunkFromSchema(Schema{Dtype: dtype2}), NewChunkFromSchema(Schema{Dtype: dtype3})
	var err error
	if strings.HasPrefix(rawData1, litPrefix) {
		c1, err = NewChunkLiteralAuto(strings.TrimPrefix(rawData1, litPrefix), nrows)
		if err != nil {
			return nil, nil, nil, err
		}
	} else {
		if err := c1.AddValues(strings.Split(rawData1, ",")); err != nil {
			return nil, nil, nil, err
		}
	}
	if strings.HasPrefix(rawData2, litPrefix) {
		c2, err = NewChunkLiteralAuto(strings.TrimPrefix(rawData2, litPrefix), nrows)
		if err != nil {
			return nil, nil, nil, err
		}
	} else {
		if err := c2.AddValues(strings.Split(rawData2, ",")); err != nil {
			return nil, nil, nil, err
		}
	}

	if strings.HasPrefix(rawData3, litPrefix) {
		expected, err = NewChunkLiteralAuto(strings.TrimPrefix(rawData3, litPrefix), nrows)
		if err != nil {
			return nil, nil, nil, err
		}
	} else {
		if err := expected.AddValues(strings.Split(rawData3, ",")); err != nil {
			return nil, nil, nil, err
		}
	}
	return c1, c2, expected, nil
}

func TestAndOr(t *testing.T) {
	tests := []struct {
		fnc              func(Chunk, Chunk) (Chunk, error)
		nrows            int
		c1, c2, expected string
	}{
		{EvalAnd, 3, "t", "t", "t"},
		{EvalAnd, 3, "t", "f", "f"},
		{EvalAnd, 3, "f", "t", "f"},
		{EvalAnd, 3, "f", "f", "f"},
		{EvalAnd, 4, "f,f,f,t", "f,f,f,t", "f,f,f,t"},
		{EvalAnd, 4, "t,t,t,t", "t,t,t,t", "t,t,t,t"},
		{EvalAnd, 4, "t,f,f,t", "t,t,f,t", "t,f,f,t"},
		{EvalOr, 3, "t", "t", "t"},
		{EvalOr, 3, "t", "f", "t"},
		{EvalOr, 3, "f", "t", "t"},
		{EvalOr, 3, "f", "f", "f"},
		{EvalOr, 4, "f,f,f,t", "f,f,f,t", "f,f,f,t"},
		{EvalOr, 4, "t,t,t,t", "t,t,t,t", "t,t,t,t"},
		{EvalOr, 4, "t,f,f,t", "t,t,f,t", "t,t,f,t"},

		// literals
		{EvalAnd, 3, "lit:t", "t,f,t", "t,f,t"},
		{EvalOr, 3, "lit:t", "t,f,t", "t,t,t"},
		{EvalOr, 3, "lit:t", "lit:f", "lit:t"},
		{EvalOr, 3, "lit:f", "lit:f", "lit:f"},
		{EvalAnd, 3, "lit:f", "lit:t", "lit:f"},
		{EvalAnd, 3, "lit:t", "lit:t", "lit:t"},
	}
	for _, test := range tests {
		c1, c2, expected, err := prepColumns(test.nrows, DtypeBool, DtypeBool, DtypeBool, test.c1, test.c2, test.expected)
		if err != nil {
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
		dtype1, dtype2   Dtype
		fnc              func(Chunk, Chunk) (Chunk, error)
		nrows            int
		c1, c2, expected string
	}{
		// eq
		{DtypeInt, DtypeInt, EvalEq, 3, "1,2,3", "3,3,3", "f,f,t"},
		{DtypeFloat, DtypeFloat, EvalEq, 3, "1.2,2.2,3", "3,2.2,3.0", "f,t,t"},
		{DtypeBool, DtypeBool, EvalEq, 4, "t,t,f,t", "f,t,f,f", "f,t,t,f"},
		{DtypeBool, DtypeBool, EvalNeq, 4, "t,t,f,t", "f,t,f,f", "t,f,f,t"},
		{DtypeString, DtypeString, EvalEq, 3, "foo,bar,baz", "foo,bak,baz", "t,f,t"},
		// eq with nulls
		{DtypeInt, DtypeInt, EvalEq, 3, "1,,3", "1,2,3", "t,,t"}, // once we implement ChunksEqual here, we can compare 1,,3 and 1,0,3
		// neq
		{DtypeInt, DtypeInt, EvalNeq, 3, "1,2,3", "3,3,3", "t,t,f"},
		{DtypeFloat, DtypeFloat, EvalNeq, 3, "1,2.0,3.1", "3,2,3", "t,f,t"},

		// literals
		{DtypeInt, DtypeInt, EvalEq, 3, "lit:1", "3,1,2", "f,t,f"},
		{DtypeInt, DtypeInt, EvalEq, 3, "3,1,2", "lit:1", "f,t,f"},
		{DtypeInt, DtypeInt, EvalNeq, 3, "lit:1", "3,1,2", "t,f,t"},
		{DtypeInt, DtypeInt, EvalNeq, 3, "3,1,2", "lit:1", "t,f,t"},
		{DtypeInt, DtypeInt, EvalGte, 3, "lit:2", "3,1,2", "f,t,t"},
		{DtypeFloat, DtypeFloat, EvalEq, 3, "3,1,2", "lit:1.0", "f,t,f"},
		{DtypeBool, DtypeBool, EvalNeq, 3, "lit:t", "f,t,f", "t,f,t"},
		{DtypeBool, DtypeBool, EvalGt, 2, "t,f", "lit:t", "f,f"},
		{DtypeString, DtypeString, EvalGt, 3, "lit:ahoy", "ahey,boo,a", "t,f,t"},
		{DtypeString, DtypeString, EvalLte, 3, "ahey,boo,a", "lit:ahoy", "t,f,t"},
		// all literals
		{DtypeInt, DtypeInt, EvalEq, 3, "lit:1", "lit:2", "lit:f"},
		{DtypeFloat, DtypeFloat, EvalEq, 3, "lit:1", "lit:2", "lit:f"},

		{DtypeInt, DtypeFloat, EvalEq, 3, "1,2,3", "1.2,2.0,3", "f,t,t"},
		{DtypeFloat, DtypeInt, EvalEq, 3, "1.2,2.0,3", "1,2,3", "f,t,t"},
		{DtypeFloat, DtypeInt, EvalEq, 3, "lit:2", "1,2,3", "f,t,f"},
		{DtypeFloat, DtypeInt, EvalEq, 3, "1,2,3", "lit:2", "f,t,f"},
		{DtypeFloat, DtypeInt, EvalEq, 3, "lit:3.4", "lit:3", "lit:f"},
		{DtypeFloat, DtypeInt, EvalEq, 3, "lit:3", "lit:3", "lit:t"},
		{DtypeInt, DtypeFloat, EvalEq, 3, "lit:2", "1,2,3", "f,t,f"},
		{DtypeInt, DtypeFloat, EvalEq, 3, "lit:3", "lit:3.4", "lit:f"},
		{DtypeInt, DtypeFloat, EvalEq, 3, "lit:3", "lit:3", "lit:t"},

		{DtypeInt, DtypeFloat, EvalNeq, 3, "1,2,3", "1.2,2.0,3", "t,f,f"},
		{DtypeFloat, DtypeInt, EvalNeq, 3, "1.2,2.0,3", "1,2,3", "t,f,f"},
	}
	for _, test := range tests {
		c1, c2, expected, err := prepColumns(test.nrows, test.dtype1, test.dtype2, DtypeBool, test.c1, test.c2, test.expected)
		if err != nil {
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
		// no nullables
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

		// literals
		{EvalAdd, 3, DtypeInt, DtypeInt, DtypeInt, "lit:34", "4,5,6", "38,39,40"},
		{EvalAdd, 3, DtypeInt, DtypeInt, DtypeInt, "4,5,6", "lit:34", "38,39,40"},
		{EvalAdd, 3, DtypeInt, DtypeInt, DtypeInt, "lit:34", "lit:33", "lit:67"},
		{EvalAdd, 3, DtypeInt, DtypeFloat, DtypeFloat, "lit:34", "4,5.5,6.2", "38,39.5,40.2"},
		{EvalAdd, 3, DtypeFloat, DtypeInt, DtypeFloat, "4,5.5,6.2", "lit:34", "38,39.5,40.2"},
		{EvalAdd, 3, DtypeFloat, DtypeFloat, DtypeFloat, "lit:34.3", "lit:33.1", "lit:67.4"},
		{EvalSubtract, 3, DtypeInt, DtypeInt, DtypeInt, "lit:34", "4,5,6", "30,29,28"},
		{EvalSubtract, 3, DtypeInt, DtypeInt, DtypeInt, "4,5,6", "lit:34", "-30,-29,-28"},
		{EvalSubtract, 3, DtypeInt, DtypeInt, DtypeInt, "lit:34", "lit:33", "lit:1"},
		{EvalSubtract, 3, DtypeInt, DtypeFloat, DtypeFloat, "lit:34", "4,5.5,6.2", "30,28.5,27.8"},
		{EvalSubtract, 3, DtypeFloat, DtypeInt, DtypeFloat, "4,5.5,6.2", "lit:34", "-30,-28.5,-27.8"},
		{EvalSubtract, 3, DtypeFloat, DtypeFloat, DtypeFloat, "lit:35", "lit:33.5", "lit:1.5"},
		{EvalDivide, 3, DtypeInt, DtypeInt, DtypeFloat, "lit:34", "4,5,8", "8.5,6.8,4.25"},
		{EvalDivide, 3, DtypeInt, DtypeInt, DtypeFloat, "4,5,6", "lit:35", "0.11428571428571428,0.14285714285714285,0.17142857142857143"},
		{EvalDivide, 3, DtypeInt, DtypeInt, DtypeFloat, "lit:34", "lit:33", "lit:1.0303030303030303"},
		{EvalDivide, 3, DtypeInt, DtypeFloat, DtypeFloat, "lit:34", "4,5.5,6.2", "8.5,6.181818181818182,5.483870967741935"},
		{EvalDivide, 3, DtypeFloat, DtypeInt, DtypeFloat, "4,5.5,6.2", "lit:34", "0.11764705882352941,0.16176470588235295,0.1823529411764706"},
		{EvalDivide, 3, DtypeFloat, DtypeFloat, DtypeFloat, "lit:35", "lit:33.5", "lit:1.044776119402985"},
		{EvalMultiply, 3, DtypeInt, DtypeInt, DtypeInt, "lit:34", "4,5,8", "136,170,272"},
		{EvalMultiply, 3, DtypeInt, DtypeInt, DtypeInt, "4,5,6", "lit:35", "140,175,210"},
		{EvalMultiply, 3, DtypeInt, DtypeInt, DtypeFloat, "lit:34", "lit:33", "lit:1122"},
		{EvalMultiply, 3, DtypeInt, DtypeFloat, DtypeFloat, "lit:34", "4,5.5,6.2", "136,187,210.8"},
		{EvalMultiply, 3, DtypeFloat, DtypeInt, DtypeFloat, "4,5.5,6.2", "lit:34", "136,187,210.8"},
		{EvalMultiply, 3, DtypeFloat, DtypeFloat, DtypeFloat, "lit:35", "lit:33.5", "lit:1172.5"},
	}
	for _, test := range tests {
		c1, c2, expected, err := prepColumns(test.nrows, test.dt1, test.dt2, test.dte, test.c1, test.c2, test.expected)
		if err != nil {
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
