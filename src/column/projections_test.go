package column

import (
	"errors"
	"strings"
	"testing"
)

var litPrefix = "lit:"

func prepColumn(nrows int, dtype Dtype, rawData string) (Chunk, error) {
	c := NewChunkFromSchema(Schema{Dtype: dtype})
	var err error
	if strings.HasPrefix(rawData, litPrefix) {
		c, err = NewChunkLiteralTyped(strings.TrimPrefix(rawData, litPrefix), dtype, nrows)
		if err != nil {
			return nil, err
		}
	} else {
		if err := c.AddValues(strings.Split(rawData, ",")); err != nil {
			return nil, err
		}
	}
	return c, err
}

func prepColumns(nrows int, dtype1, dtype2, dtype3 Dtype, rawData1, rawData2, rawData3 string) (Chunk, Chunk, Chunk, error) {
	c1, err := prepColumn(nrows, dtype1, rawData1)
	if err != nil {
		return nil, nil, nil, err
	}
	c2, err := prepColumn(nrows, dtype2, rawData2)
	if err != nil {
		return nil, nil, nil, err
	}
	expected, err := prepColumn(nrows, dtype3, rawData3)
	if err != nil {
		return nil, nil, nil, err
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
			t.Errorf("expected AND of %+v and %+v to result in %+v, got %+v instead", test.c1, test.c2, test.expected, res)
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
		{DtypeInt, DtypeInt, EvalEq, 3, "1,,3", "1,2,3", "t,,t"},
		{DtypeInt, DtypeInt, EvalEq, 3, "1,,3", "1,0,3", "t,,t"},
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
		{DtypeDate, DtypeDate, EvalEq, 3, "2020-02-22,1977-12-31,1901-02-28", "lit:1977-12-31", "f,t,f"},
		{DtypeDate, DtypeDate, EvalEq, 3, "lit:1977-12-31", "lit:1977-12-31", "lit:t"},
		{DtypeDatetime, DtypeDatetime, EvalEq, 2, "2020-02-22 12:34:56,1980-12-22 00:01:02", "1980-12-22 00:01:02,1980-12-22 00:01:02", "f,t"},

		{DtypeInt, DtypeFloat, EvalNeq, 3, "1,2,3", "1.2,2.0,3", "t,f,f"},
		{DtypeFloat, DtypeInt, EvalNeq, 3, "1.2,2.0,3", "1,2,3", "t,f,f"},
		{DtypeDate, DtypeDate, EvalNeq, 3, "2020-02-22,1977-12-31,1901-02-28", "lit:1977-12-31", "t,f,t"},
		{DtypeDatetime, DtypeDatetime, EvalNeq, 2, "2020-02-22 12:34:56,1980-12-22 00:01:02", "1980-12-22 00:01:02,1980-12-22 00:01:02", "t,f"},
		{DtypeDate, DtypeDate, EvalGt, 3, "2020-02-22,1977-12-31,1901-02-28", "lit:1977-12-31", "t,f,f"},
		{DtypeDate, DtypeDate, EvalGte, 3, "2020-02-22,1977-12-31,1901-02-28", "lit:1977-12-31", "t,t,f"},
		{DtypeDatetime, DtypeDatetime, EvalLt, 2, "1920-02-22 12:34:56,1980-12-22 00:01:02", "1980-12-22 00:01:02,1980-12-22 00:01:02", "t,f"},
		{DtypeDatetime, DtypeDatetime, EvalLte, 2, "1920-02-22 12:34:56,1980-12-22 00:01:02", "1980-12-22 00:01:02,1980-12-22 00:01:02", "t,t"},
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
			t.Errorf("expected %+v and %+v to result in %+v, got %+v instead", test.c1, test.c2, test.expected, res)
		}
	}
}

func TestAlgebraicExpressions(t *testing.T) {
	tests := []struct {
		fnc              func(Chunk, Chunk) (Chunk, error)
		nrows            int
		dt1, dt2, dte    Dtype
		c1, c2, expected string
		err              error
	}{
		// not compatible
		{EvalAdd, 3, DtypeInt, DtypeString, DtypeInt, "1,2,3", "4,5,6", "5,7,9", errProjectionNotSupported},
		{EvalAdd, 3, DtypeInt, DtypeBool, DtypeInt, "1,2,3", "t,t,f", "5,7,9", errProjectionNotSupported},
		{EvalAdd, 3, DtypeDate, DtypeInt, DtypeInt, "2020-02-22,2021-01-31,1970-08-23", "1,2,3", "5,7,9", errProjectionNotSupported},

		// no nullables
		{EvalAdd, 3, DtypeFloat, DtypeFloat, DtypeFloat, "1.4,2.4,3.5", "4.1,5.5,6.1", "5.5,7.9,9.6", nil},
		{EvalAdd, 3, DtypeInt, DtypeInt, DtypeInt, "1,2,3", "4,5,6", "5,7,9", nil},
		{EvalAdd, 3, DtypeInt, DtypeFloat, DtypeFloat, "1,2,3", "4.1,5.5,6.1", "5.1,7.5,9.1", nil},
		{EvalAdd, 3, DtypeFloat, DtypeInt, DtypeFloat, "4.1,5.5,6.1", "1,2,3", "5.1,7.5,9.1", nil},
		{EvalSubtract, 3, DtypeInt, DtypeInt, DtypeInt, "1,2,3", "1,10,2", "0,-8,1", nil},
		{EvalSubtract, 3, DtypeFloat, DtypeFloat, DtypeFloat, "1.2,2.3,3.3", "2.4,3.8,11.1", "-1.2,-1.5,-7.8", nil},
		{EvalSubtract, 3, DtypeInt, DtypeFloat, DtypeFloat, "1,2,3", "2.4,3.9,11.1", "-1.4,-1.9,-8.1", nil},
		{EvalSubtract, 3, DtypeFloat, DtypeInt, DtypeFloat, "2.4,3.9,11.1", "1,2,3", "1.4,1.9,8.1", nil},
		{EvalDivide, 2, DtypeInt, DtypeInt, DtypeInt, "1,2", "2,8", "0,0", nil},
		{EvalDivide, 2, DtypeFloat, DtypeFloat, DtypeFloat, "1,2.2", "2.19,8.3", "0.4566210045662101,0.26506024096385544", nil},
		{EvalDivide, 2, DtypeInt, DtypeFloat, DtypeFloat, "1,2", "2.19,8.3", "0.4566210045662101,0.24096385542168672", nil},
		{EvalDivide, 2, DtypeFloat, DtypeInt, DtypeFloat, "1.2,3.4", "12,19", "0.09999999999999999,0.17894736842105263", nil},
		{EvalMultiply, 3, DtypeInt, DtypeInt, DtypeInt, "1,2,3", "100,200,300", "100,400,900", nil},
		{EvalMultiply, 3, DtypeFloat, DtypeFloat, DtypeFloat, "1.444,2.132,3.4124", "123.123,22.223,4.123", "177.789612,47.379436,14.0693252", nil},
		{EvalMultiply, 3, DtypeInt, DtypeFloat, DtypeFloat, "11,2,39", "123.123,22.223,4.123", "1354.353,44.446,160.797", nil},
		{EvalMultiply, 3, DtypeFloat, DtypeInt, DtypeFloat, "123.123,22.223,4.123", "11,2,39", "1354.353,44.446,160.797", nil},
		// nulls
		{EvalAdd, 3, DtypeInt, DtypeInt, DtypeInt, "1,2,3", "4,,6", "5,,9", nil},
		{EvalAdd, 3, DtypeInt, DtypeInt, DtypeInt, "1,2,3", ",,", ",,", nil},
		{EvalAdd, 3, DtypeInt, DtypeInt, DtypeInt, ",,", "4,5,6", ",,", nil},
		{EvalAdd, 3, DtypeFloat, DtypeFloat, DtypeFloat, "1.4,,3.5", "4.1,5.5,", "5.5,,", nil},
		{EvalDivide, 2, DtypeFloat, DtypeInt, DtypeFloat, "1.2,3.4", "12,", "0.09999999999999999,", nil},
		{EvalDivide, 2, DtypeFloat, DtypeInt, DtypeFloat, "1.2,", "12,12", "0.09999999999999999,", nil},
		{EvalDivide, 2, DtypeFloat, DtypeFloat, DtypeFloat, "1,2.2", ",8.3", ",0.26506024096385544", nil},
		{EvalDivide, 2, DtypeFloat, DtypeFloat, DtypeFloat, ",2.2", "0,8.3", ",0.26506024096385544", nil},
		{EvalAdd, 3, DtypeInt, DtypeInt, DtypeInt, "lit:34", "4,,6", "38,,40", nil},
		// TODO: we don't have nullable typed literals (so 4 > NULL will fail)
		// {EvalAdd, 3, DtypeInt, DtypeInt, DtypeInt, "lit:34", "lit:", "lit:", nil},

		// overflows
		{EvalAdd, 1, DtypeInt, DtypeInt, DtypeInt, "9223372036854775807", "0", "9223372036854775807", nil},
		{EvalAdd, 1, DtypeInt, DtypeInt, DtypeInt, "9223372036854775807", "1", "-9223372036854775808", nil},
		{EvalMultiply, 1, DtypeInt, DtypeInt, DtypeInt, "9223372036854775802", "4", "-24", nil},
		{EvalSubtract, 1, DtypeInt, DtypeInt, DtypeInt, "-9223372036854775808", "2", "9223372036854775806", nil},
		{EvalMultiply, 1, DtypeInt, DtypeInt, DtypeInt, "-9223372036854775808", "7", "-9223372036854775808", nil},

		// literals
		{EvalAdd, 3, DtypeInt, DtypeInt, DtypeInt, "lit:34", "4,5,6", "38,39,40", nil},
		{EvalAdd, 3, DtypeInt, DtypeInt, DtypeInt, "4,5,6", "lit:34", "38,39,40", nil},
		{EvalAdd, 3, DtypeInt, DtypeInt, DtypeInt, "lit:34", "lit:33", "lit:67", nil},
		{EvalAdd, 3, DtypeInt, DtypeFloat, DtypeFloat, "lit:34", "4,5.5,6.2", "38,39.5,40.2", nil},
		{EvalAdd, 3, DtypeFloat, DtypeInt, DtypeFloat, "4,5.5,6.2", "lit:34", "38,39.5,40.2", nil},
		{EvalAdd, 3, DtypeFloat, DtypeFloat, DtypeFloat, "lit:34.3", "lit:33.1", "lit:67.4", nil},
		{EvalSubtract, 3, DtypeInt, DtypeInt, DtypeInt, "lit:34", "4,5,6", "30,29,28", nil},
		{EvalSubtract, 3, DtypeInt, DtypeInt, DtypeInt, "4,5,6", "lit:34", "-30,-29,-28", nil},
		{EvalSubtract, 3, DtypeInt, DtypeInt, DtypeInt, "lit:34", "lit:33", "lit:1", nil},
		{EvalSubtract, 3, DtypeInt, DtypeFloat, DtypeFloat, "lit:34", "4,5.5,6.2", "30,28.5,27.8", nil},
		{EvalSubtract, 3, DtypeFloat, DtypeInt, DtypeFloat, "4,5.5,6.2", "lit:34", "-30,-28.5,-27.8", nil},
		{EvalSubtract, 3, DtypeFloat, DtypeFloat, DtypeFloat, "lit:35", "lit:33.5", "lit:1.5", nil},
		{EvalDivide, 3, DtypeInt, DtypeInt, DtypeInt, "lit:34", "4,5,8", "8,6,4", nil},
		{EvalDivide, 3, DtypeInt, DtypeInt, DtypeInt, "4,5,6", "lit:35", "0,0,0", nil},
		{EvalDivide, 3, DtypeInt, DtypeInt, DtypeInt, "lit:34", "lit:33", "lit:1", nil},
		{EvalDivide, 3, DtypeInt, DtypeFloat, DtypeFloat, "lit:34", "4,5.5,6.2", "8.5,6.181818181818182,5.483870967741935", nil},
		{EvalDivide, 3, DtypeFloat, DtypeInt, DtypeFloat, "4,5.5,6.2", "lit:34", "0.11764705882352941,0.16176470588235295,0.1823529411764706", nil},
		{EvalDivide, 3, DtypeFloat, DtypeFloat, DtypeFloat, "lit:35", "lit:33.5", "lit:1.044776119402985", nil},
		{EvalMultiply, 3, DtypeInt, DtypeInt, DtypeInt, "lit:34", "4,5,8", "136,170,272", nil},
		{EvalMultiply, 3, DtypeInt, DtypeInt, DtypeInt, "4,5,6", "lit:35", "140,175,210", nil},
		{EvalMultiply, 3, DtypeInt, DtypeInt, DtypeInt, "lit:34", "lit:33", "lit:1122", nil},
		{EvalMultiply, 3, DtypeInt, DtypeFloat, DtypeFloat, "lit:34", "4,5.5,6.2", "136,187,210.8", nil},
		{EvalMultiply, 3, DtypeFloat, DtypeInt, DtypeFloat, "4,5.5,6.2", "lit:34", "136,187,210.8", nil},
		{EvalMultiply, 3, DtypeFloat, DtypeFloat, DtypeFloat, "lit:35", "lit:33.5", "lit:1172.5", nil},
	}
	for _, test := range tests {
		c1, c2, expected, err := prepColumns(test.nrows, test.dt1, test.dt2, test.dte, test.c1, test.c2, test.expected)
		if err != nil {
			t.Error(err)
			continue
		}

		res, err := test.fnc(c1, c2)
		if !errors.Is(err, test.err) {
			t.Errorf("expecting expression to result in %v, got %v instead", test.err, err)
			continue
		}
		if test.err == nil && !ChunksEqual(res, expected) {
			t.Errorf("expected %+v and %+v to result in %+v, got %+v instead", test.c1, test.c2, test.expected, res)
		}
	}
}

func TestNot(t *testing.T) {
	tests := []struct {
		nrows        int
		c1, expected string
	}{
		{3, "t,f,t", "f,t,f"},
		{3, "f,t,f", "t,f,t"},
		{3, "t,t,t", "f,f,f"},
		{3, "f,f,f", "t,t,t"},
		{3, "t,,t", "f,,f"},
		{3, "f,,", "t,,"},

		// literals
		{3, "lit:f", "lit:t"},
		{3, "lit:t", "lit:f"},
	}
	for _, test := range tests {
		c, err := prepColumn(test.nrows, DtypeBool, test.c1)
		if err != nil {
			t.Error(err)
			continue
		}
		exp, err := prepColumn(test.nrows, DtypeBool, test.expected)
		if err != nil {
			t.Error(err)
			continue
		}

		ca, err := EvalNot(c)
		if err != nil {
			t.Error(err)
			continue
		}

		if !ChunksEqual(ca, exp) {
			t.Errorf("expected NOT %v to result in %+v, got %+v instead", test.c1, test.expected, ca)
		}
	}
}
