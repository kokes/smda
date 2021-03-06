package column

import (
	"errors"
	"fmt"

	"github.com/kokes/smda/src/bitmap"
)

var errCannotCastToType = errors.New("cannot cast to this type")

func (rc *ChunkInts) cast(dtype Dtype) (Chunk, error) {
	switch dtype {
	case DtypeInt:
		return rc, nil // 1) noop, 2) NOT copying, issue?
	case DtypeFloat:
		if rc.IsLiteral {
			val := float64(rc.data[0])
			return NewChunkLiteralFloats(val, rc.Len()), nil
		}
		data := make([]float64, rc.Len())
		for j := 0; j < rc.Len(); j++ {
			data[j] = float64(rc.data[j]) // perhaps use nthValue?
		}
		// ARCH: a case for bitmap.Clone(bm)?
		var nulls *bitmap.Bitmap
		if rc.Nullability != nil {
			nulls = rc.Nullability.Clone()
		}
		return NewChunkFloatsFromSlice(data, nulls), nil
	default:
		return nil, fmt.Errorf("%w: %v to %v", errCannotCastToType, rc.Dtype(), dtype)
	}
}
