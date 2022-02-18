package column

import (
	"errors"
	"fmt"

	"github.com/kokes/smda/src/bitmap"
)

var errCannotCastType = errors.New("cannot cast from this type")
var errCannotCastToType = errors.New("cannot cast to this type")

func (rc *Chunk) cast(dtype Dtype) (*Chunk, error) {
	if rc.dtype != DtypeInt {
		// TODO(next): test this
		return nil, errCannotCastType
	}
	switch dtype {
	case DtypeInt:
		return rc, nil // 1) noop, 2) NOT copying, issue?
	case DtypeFloat:
		if rc.IsLiteral {
			val := float64(rc.storage.ints[0])
			return NewChunkLiteralFloats(val, rc.Len()), nil
		}
		data := make([]float64, rc.Len())
		for j := 0; j < rc.Len(); j++ {
			data[j] = float64(rc.storage.ints[j]) // perhaps use nthValue?
		}
		nulls := bitmap.Clone(rc.Nullability)
		return NewChunkFloatsFromSlice(data, nulls), nil
	default:
		return nil, fmt.Errorf("%w: %v to %v", errCannotCastToType, rc.dtype, dtype)
	}
}
