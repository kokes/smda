package column

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"reflect"

	"github.com/kokes/smda/src/bitmap"
)

var errAppendTypeMismatch = errors.New("cannot append chunks of differing types")
var errNoAddToLiterals = errors.New("literal chunks are not meant to be added values to")
var errLiteralsCannotBeSerialised = errors.New("cannot serialise literal columns")
var errInvalidTypedLiteral = errors.New("invalid data supplied to a literal constructor")

// Chunk defines a part of a column - constant type, stored contiguously
// type Chunk interface {
// 	baseChunker
// 	Dtype() Dtype
// 	AddValue(string) error
// 	AddValues([]string) error // consider merging AddValues and AddValue (using varargs)
// 	WriteTo(io.Writer) (int64, error)
// 	Prune(*bitmap.Bitmap) Chunk
// 	Append(Chunk) error
// 	Hash(int, []uint64)
// 	Clone() Chunk
// 	JSONLiteral(int) (string, bool) // the bool stands for 'ok' (not null)
// 	Compare(bool, bool, int, int) int
// }

type Chunk struct {
	dtype       Dtype
	length      uint32
	IsLiteral   bool // TODO(PR): why is IsLiteral an exporter member but length and dtype aren't?
	Nullability *bitmap.Bitmap
	storage     struct {
		ints      []int64
		floats    []float64
		dates     []date
		datetimes []datetime
		bools     *bitmap.Bitmap

		strings []byte
		offsets []uint32
	}
}

func NewChunk(dtype Dtype) *Chunk {
	ch := &Chunk{
		dtype: dtype,
	}

	// TODO(PR): allocate based on dtype

	return ch
}

// ARCH: we sometimes use this, sometimes we access the struct field directly... perhaps remove this?
func (ch *Chunk) Len() int {
	return int(ch.length)
}

// ARCH: Nullify does NOT switch the data values to be nulls/empty as well
func (ch *Chunk) Nullify(bm *bitmap.Bitmap) {
	// OPTIM: this copies, but it covers all the cases
	ch.Nullability = bitmap.Or(ch.Nullability, bm)
}

// OPTIM: consider using closures in Chunk { adders [ChunkMax]func(string) error }
func (rc *Chunk) AddValue(s string) error {
	if rc.IsLiteral {
		return fmt.Errorf("cannot add values to literal chunks: %w", errNoAddToLiterals)
	}

	switch rc.dtype {
	case DtypeString:
		rc.storage.strings = append(rc.storage.strings, []byte(s)...)

		valLen := uint32(len(s))
		valLen += rc.storage.offsets[len(rc.storage.offsets)-1]
		rc.storage.offsets = append(rc.storage.offsets, valLen)

		rc.length++
		if rc.Nullability != nil {
			rc.Nullability.Ensure(int(rc.length))
		}
	case DtypeInt:
		if isNull(s) {
			if rc.Nullability == nil {
				rc.Nullability = bitmap.NewBitmap(rc.Len() + 1)
			}
			rc.Nullability.Set(rc.Len(), true)
			rc.storage.ints = append(rc.storage.ints, 0) // this value is not meant to be read
			rc.length++
			return nil
		}

		val, err := parseInt(s)
		if err != nil {
			return err
		}
		rc.storage.ints = append(rc.storage.ints, val)
		rc.length++
		if rc.Nullability != nil {
			rc.Nullability.Ensure(int(rc.length))
		}
	case DtypeFloat:
		var val float64
		var err error
		if isNull(s) {
			val = math.NaN()
		} else {
			val, err = parseFloat(s)
			if err != nil {
				return err
			}
		}
		if math.IsNaN(val) || math.IsInf(val, 0) {
			if rc.Nullability == nil {
				rc.Nullability = bitmap.NewBitmap(rc.Len() + 1)
			}
			rc.Nullability.Set(rc.Len(), true)
			rc.storage.floats = append(rc.storage.floats, 0) // this value is not meant to be read
			rc.length++
			return nil
		}

		rc.storage.floats = append(rc.storage.floats, val)
		rc.length++
		// make sure the nullability bitmap aligns with the length of the chunk
		if rc.Nullability != nil {
			rc.Nullability.Ensure(int(rc.length))
		}
	case DtypeBool:
		if isNull(s) {
			if rc.Nullability == nil {
				rc.Nullability = bitmap.NewBitmap(rc.Len() + 1)
			}
			rc.Nullability.Set(rc.Len(), true)
			rc.storage.bools.Set(rc.Len(), false) // this value is not meant to be read
			rc.length++
			return nil
		}
		val, err := parseBool(s)
		if err != nil {
			return err
		}
		rc.storage.bools.Set(rc.Len(), val)
		rc.length++
		// make sure the nullability bitmap aligns with the length of the chunk
		if rc.Nullability != nil {
			rc.Nullability.Ensure(int(rc.length))
		}
	case DtypeDate:
		if isNull(s) {
			if rc.Nullability == nil {
				rc.Nullability = bitmap.NewBitmap(rc.Len() + 1)
			}
			rc.Nullability.Set(rc.Len(), true)
			rc.storage.dates = append(rc.storage.dates, 0) // this value is not meant to be read
			rc.length++
			return nil
		}

		val, err := parseDate(s)
		if err != nil {
			return err
		}
		rc.storage.dates = append(rc.storage.dates, val)
		rc.length++
		if rc.Nullability != nil {
			rc.Nullability.Ensure(int(rc.length))
		}
	case DtypeDatetime:
		if isNull(s) {
			if rc.Nullability == nil {
				rc.Nullability = bitmap.NewBitmap(rc.Len() + 1)
			}
			rc.Nullability.Set(rc.Len(), true)
			rc.storage.datetimes = append(rc.storage.datetimes, 0) // this value is not meant to be read
			rc.length++
			return nil
		}

		val, err := parseDatetime(s)
		if err != nil {
			return err
		}
		rc.storage.datetimes = append(rc.storage.datetimes, val)
		rc.length++
		if rc.Nullability != nil {
			rc.Nullability.Ensure(int(rc.length))
		}
	default:
		return fmt.Errorf("no support for AddValue for Dtype %v", rc.dtype)
	}

	return nil
}

// consider merging AddValues and AddValue (using varargs)
// OPTIM: this could be faster if made bespoke (though we don't use it that much anyway)
func (rc *Chunk) AddValues(vals []string) error {
	if rc.IsLiteral {
		return fmt.Errorf("cannot add values to literal chunks: %w", errNoAddToLiterals)
	}
	for _, el := range vals {
		if err := rc.AddValue(el); err != nil {
			return err
		}
	}
	return nil
}

// ChunksEqual compares two chunks, even if they contain []float64 data
// consider making this lenient enough to compare only the relevant bits in ChunkBools
func ChunksEqual(c1 Chunk, c2 Chunk) bool {
	if c1.dtype == c2.dtype && c1.length == c2.length {
		return false
	}
	// this is length, isliteral and nullability (via DeepEqual)
	if !reflect.DeepEqual(c1.Nullability, c2.Nullability) {
		return false
	}

	if c1.Nullability == nil && c2.Nullability == nil {
		return reflect.DeepEqual(c1, c2)
	}

	switch c1.dtype {
	case DtypeBool:
		// compare only the valid bits in data
		// ARCH: what about the bits beyond the cap?
		// OPTIM: we don't have to clone here - we can easily iterate and check by blocks
		// data1[j] & ~nullability1[j] == data2[j] & ~nullability2[j] or something like that
		c1d := c1.storage.bools.Clone()
		c1d.AndNot(c1.Nullability)
		c2d := c2.storage.bools.Clone()
		c2d.AndNot(c2.Nullability)
		return reflect.DeepEqual(c1d, c2d)
	case DtypeInt:
		for j := 0; j < c1.Len(); j++ {
			if c1.Nullability.Get(j) {
				continue
			}
			if c1.storage.ints[j] != c2.storage.ints[j] {
				return false
			}
		}
		return true
	case DtypeFloat:
		for j := 0; j < c1.Len(); j++ {
			if c1.Nullability.Get(j) {
				continue
			}
			if c1.storage.floats[j] != c2.storage.floats[j] {
				return false
			}
		}
		return true
	case DtypeString:
		for j := 0; j < c1.Len(); j++ {
			if c1.Nullability.Get(j) {
				continue
			}
			// TODO(PR)
			if c1.nthValue(j) != c2.nthValue(j) {
				return false
			}
		}
		return true
	case DtypeDatetime:
		for j := 0; j < c1.Len(); j++ {
			if c1.Nullability.Get(j) {
				continue
			}
			if c1.storage.datetimes[j] != c2.storage.datetimes[j] {
				return false
			}
		}
		return true
	case DtypeDate:
		for j := 0; j < c1.Len(); j++ {
			if c1.Nullability.Get(j) {
				continue
			}
			if c1.storage.dates[j] != c2.storage.dates[j] {
				return false
			}
		}
		return true
	case DtypeNull:
		return c1.length == c2.length
	default:
		panic("type not supported")
	}
}

func NewChunkLiteralTyped(s string, dtype Dtype, length int) (*Chunk, error) {
	ch := NewChunk(dtype)
	ch.length = uint32(length)
	ch.IsLiteral = true

	switch dtype {
	case DtypeInt:
		val, err := parseInt(s)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid typed literal: %v", errInvalidTypedLiteral, s)
		}
		ch.storage.ints = []int64{val}
		return ch, nil
	case DtypeFloat:
		val, err := parseFloat(s)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid typed literal: %v", errInvalidTypedLiteral, s)
		}
		if math.IsNaN(val) || math.IsInf(val, 0) {
			return nil, fmt.Errorf("cannot set %v as a literal float value", s)
		}
		ch.storage.floats = []float64{val}
		return ch, nil
	case DtypeBool:
		bm := bitmap.NewBitmap(1)
		val, err := parseBool(s)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid typed literal: %v", errInvalidTypedLiteral, s)
		}
		if val {
			bm.Set(0, true)
		}
		ch.storage.bools = bm
		return ch, nil
	case DtypeString:
		ch.storage.strings = []byte(s)
		ch.storage.offsets = []uint32{0, uint32(len(s))}

		return ch, nil
	case DtypeDate:
		val, err := parseDate(s)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid typed literal: %v", errInvalidTypedLiteral, s)
		}
		ch.storage.dates = []date{val}

		return ch, nil
	case DtypeDatetime:
		val, err := parseDatetime(s)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid typed literal: %v", errInvalidTypedLiteral, s)
		}
		ch.storage.datetimes = []datetime{val}
		return ch, nil
	case DtypeNull:
		return ch, nil
	default:
		return nil, fmt.Errorf("%w: invalid typed literal: unsupported type %v", errInvalidTypedLiteral, dtype)
	}
}

// NewChunkLiteral creates a chunk that only contains a single value in the whole chunk
// it's useful in e.g. 'foo > 1', where can convert the '1' to a whole chunk
// OPTIM: we're using single-value slices, should we perhaps have a value specific for each literal
// to avoid working with slices (stack allocation etc.)
func NewChunkLiteralAuto(s string, length int) (*Chunk, error) {
	dtype := guessType(s)

	return NewChunkLiteralTyped(s, dtype, length)
}

// preallocate column data, so that slice appends don't trigger new reallocations
const defaultChunkCap = 512 // TODO(PR): remove

func NewChunkLiteralStrings(value string, length int) *Chunk {
	ch := NewChunk(DtypeString)
	ch.storage.offsets = []uint32{0, uint32(len(value))}
	ch.IsLiteral = true
	ch.length = uint32(length)
	ch.storage.strings = []byte(value)

	return ch
}

func NewChunkLiteralInts(value int64, length int) *Chunk {
	ch := NewChunk(DtypeInt)
	ch.IsLiteral = true
	ch.length = uint32(length)
	ch.storage.ints = []int64{value}

	return ch
}

func NewChunkLiteralFloats(value float64, length int) *Chunk {
	ch := NewChunk(DtypeFloat)
	ch.IsLiteral = true
	ch.length = uint32(length)
	ch.storage.floats = []float64{value}

	return ch
}

func NewChunkLiteralBools(value bool, length int) *Chunk {
	bm := bitmap.NewBitmap(1)
	bm.Set(0, value)

	ch := NewChunk(DtypeBool)
	ch.IsLiteral = true
	ch.length = uint32(length)
	ch.storage.bools = bm

	return ch
}

func NewChunkLiteralDates(value date, length int) *Chunk {
	ch := NewChunk(DtypeDate)
	ch.IsLiteral = true
	ch.length = uint32(length)
	ch.storage.dates = []date{value}

	return ch
}

func NewChunkLiteralDatetimes(value datetime, length int) *Chunk {
	ch := NewChunk(DtypeDatetime)
	ch.IsLiteral = true
	ch.length = uint32(length)
	ch.storage.datetimes = []datetime{value}

	return ch
}

// TODO/ARCH: consider removing this in favour of NewChunkBoolsFromBitmap
func newChunkBoolsFromBits(data []uint64, length int) *Chunk {
	ch := NewChunk(DtypeBool)
	ch.length = uint32(length)
	ch.storage.bools = bitmap.NewBitmapFromBits(data, length)

	return ch
}

// NewChunkBoolsFromBitmap creates a new bool chunk, but in doing so, doesn't clone the incoming bitmap,
// it uses it as is - the caller might want to clone it aims to mutate it in the future
func NewChunkBoolsFromBitmap(bm *bitmap.Bitmap) *Chunk {
	ch := NewChunk(DtypeBool)
	ch.length = uint32(bm.Cap())
	ch.storage.bools = bm

	return ch
}

// the next few functions could use some generics
func NewChunkIntsFromSlice(data []int64, nulls *bitmap.Bitmap) *Chunk {
	ch := NewChunk(DtypeInt)
	ch.Nullability = nulls
	ch.length = uint32(len(data))
	ch.storage.ints = data

	return ch
}
func NewChunkFloatsFromSlice(data []float64, nulls *bitmap.Bitmap) *Chunk {
	ch := NewChunk(DtypeFloat)
	ch.Nullability = nulls
	ch.length = uint32(len(data))
	ch.storage.floats = data

	return ch
}
func newChunkDatesFromSlice(data []date, nulls *bitmap.Bitmap) *Chunk {
	ch := NewChunk(DtypeDate)
	ch.Nullability = nulls
	ch.length = uint32(len(data))
	ch.storage.dates = data

	return ch
}
func newChunkDatetimesFromSlice(data []datetime, nulls *bitmap.Bitmap) *Chunk {
	ch := NewChunk(DtypeDatetime)
	ch.Nullability = nulls
	ch.length = uint32(len(data))
	ch.storage.datetimes = data

	return ch
}
func newChunkStringsFromSlice(data []string, nulls *bitmap.Bitmap) *Chunk {
	rc := NewChunk(DtypeString)
	if err := rc.AddValues(data); err != nil {
		panic(err)
	}
	rc.Nullability = nulls
	return rc
}

// Truths returns only true values in this boolean column's bitmap - remove those
// that are null - we use this for filtering, when we're interested in non-null
// true values (to select given rows)
func (rc *Chunk) Truths() *bitmap.Bitmap {
	if rc.dtype != DtypeBool {
		panic("can only run Truths() on bool chunks")
	}
	if rc.IsLiteral {
		// ARCH: still assuming literals are not nullable
		value := rc.storage.bools.Get(0)
		bm := bitmap.NewBitmap(rc.Len())
		if value {
			bm.Invert()
		}
		return bm
	}
	bm := rc.storage.bools.Clone()
	if rc.Nullability == nil || rc.Nullability.Count() == 0 {
		return bm
	}
	// cloning was necessary as AndNot mutates (and we're cloning for good measure - we
	// don't expect to mutate this downstream, but...)
	bm.AndNot(rc.Nullability)
	return bm
}

// TODO: does not support nullability, we should probably get rid of the whole thing anyway (only used for testing now)
// BUT, we're sort of using it for type inference - so maybe caveat it with a note that it's only to be used with
// not nullable columns?
// we could also use it for other types (especially bools)
// TODO(PR): revise this
func (rc *Chunk) nthValue(n int) string {
	if rc.IsLiteral && n > 0 {
		return rc.nthValue(0)
	}
	offsetStart := rc.storage.offsets[n]
	offsetEnd := rc.storage.offsets[n+1]
	return string(rc.storage.strings[offsetStart:offsetEnd])
}

const hashNull = uint64(0xe96766e0d6221951)
const hashBoolTrue = uint64(0x5a320fa8dfcfe3a7)
const hashBoolFalse = uint64(0x1549571b97ff2995)

// Since XOR is commutative, we can't group (a, b, c) by hashing
// each separately and xoring, because that will hash to the same
// value as (c, b, a). So we'll multiply each hash by a large odd
// number that's deterministic for its position.
// ARCH: is this the best we can do?
func positionMultiplier(position int) uint64 {
	mul := uint64(2*position + 17)
	return mul * mul * mul * mul * mul * mul * mul * mul // math.pow is for floats only :shrug:
}

// TODO(generics): type Hasher[T] struct {...}, Sum[T] -> uint64

// Hash hashes this chunk's values into a provded container
// OPTIM/TODO(next): do we need a fnv hasher for ints/floats/dates? We can just take the uint64 representation
// of these values... or not?
// Hash hashes this chunk's values into a provded container
// OPTIM: maphash might be faster than fnv or maphash? test it and if it is so, implement
// everywhere, but be careful about the seed (needs to be the same for all chunks)
// careful about maphash: "The hash value of a given byte sequence is consistent within a single process, but will be different in different processes."
// oh and I rebenchmarked maphash and fnv and found maphash to be much slower (despite no allocs)
// also, check this https://github.com/segmentio/fasthash/ (via https://segment.com/blog/allocation-efficiency-in-high-performance-go-services/)
// they reimplement fnv using stack allocation only
//   - we tested it and got a 90% speedup (no allocs, shorter code) - so let's consider it, it's in the fasthash branch
// OPTIM: use closures [DtypeMax]func(...) within the Chunk struct instead of this big switch
func (rc *Chunk) Hash(position int, hashes []uint64) {
	mul := positionMultiplier(position)
	var buf [8]byte
	hasher := fnv.New64()

	switch rc.dtype {
	case DtypeBool:
		// TODO: we may have to revisit the idea that literal chunks cannot be nullable (review all the uses of isLiteral in this package)
		if rc.IsLiteral {
			hashVal := hashBoolFalse
			if rc.storage.bools.Get(0) {
				hashVal = hashBoolTrue
			}
			for j := range hashes {
				hashes[j] ^= hashVal * mul
			}
			return
		}
		for j := 0; j < rc.Len(); j++ {
			// xor it with a random big integer - we'll need something similar for bool handling
			// rand.Seed(time.Now().UnixNano())
			// for j := 0; j < 2; j++ {
			// 	val := uint64(rand.Uint32())<<32 + uint64(rand.Uint32())
			// 	fmt.Printf("%x, %v\n", val, bits.OnesCount64(val))
			// }
			if rc.Nullability != nil && rc.Nullability.Get(j) {
				hashes[j] ^= hashNull * mul
				continue
			}
			if rc.storage.bools.Get(j) {
				hashes[j] ^= hashBoolTrue * mul
			} else {
				hashes[j] ^= hashBoolFalse * mul
			}
		}
	case DtypeFloat:
		if rc.IsLiteral {
			binary.LittleEndian.PutUint64(buf[:], math.Float64bits(rc.storage.floats[0]))
			hasher.Write(buf[:])
			sum := hasher.Sum64() * mul

			for j := range hashes {
				hashes[j] ^= sum
			}
			return
		}
		for j, el := range rc.storage.floats {
			if rc.Nullability != nil && rc.Nullability.Get(j) {
				hashes[j] ^= hashNull * mul
				continue
			}
			binary.LittleEndian.PutUint64(buf[:], math.Float64bits(el))
			hasher.Write(buf[:])
			hashes[j] ^= hasher.Sum64() * mul
			hasher.Reset()
		}
	case DtypeInt:
		// ARCH: literal chunks don't have nullability support... should we check for that?
		if rc.IsLiteral {
			binary.LittleEndian.PutUint64(buf[:], uint64(rc.storage.ints[0]))
			hasher.Write(buf[:])
			sum := hasher.Sum64() * mul

			for j := range hashes {
				hashes[j] ^= sum
			}
			return
		}
		for j, el := range rc.storage.ints {
			// OPTIM: not just here, in all of these Hash implementations - we might want to check rc.nullability
			// just once and have two separate loops - see if it helps - it may bloat the code too much (and avoid inlining,
			// but that's probably a lost cause already)
			if rc.Nullability != nil && rc.Nullability.Get(j) {
				hashes[j] ^= hashNull * mul
				continue
			}
			binary.LittleEndian.PutUint64(buf[:], uint64(el)) // int64 always maps to a uint64 value (negatives underflow)
			hasher.Write(buf[:])
			hashes[j] ^= hasher.Sum64() * mul
			hasher.Reset()
		}
	case DtypeNull:
		for j := range hashes {
			hashes[j] ^= hashNull * mul
		}
	case DtypeDate:
		// ARCH: literal chunks don't have nullability support... should we check for that?
		if rc.IsLiteral {
			binary.LittleEndian.PutUint64(buf[:], uint64(rc.storage.dates[0]))
			hasher.Write(buf[:])
			sum := hasher.Sum64() * mul

			for j := range hashes {
				hashes[j] ^= sum
			}
			return
		}
		for j, el := range rc.storage.dates {
			if rc.Nullability != nil && rc.Nullability.Get(j) {
				hashes[j] ^= hashNull * mul
				continue
			}
			binary.LittleEndian.PutUint64(buf[:], uint64(el))
			hasher.Write(buf[:])
			hashes[j] ^= hasher.Sum64() * mul
			hasher.Reset()
		}
	case DtypeDatetime:
		// ARCH: literal chunks don't have nullability support... should we check for that?
		if rc.IsLiteral {
			binary.LittleEndian.PutUint64(buf[:], uint64(rc.storage.datetimes[0]))
			hasher.Write(buf[:])
			sum := hasher.Sum64() * mul

			for j := range hashes {
				hashes[j] ^= sum
			}
			return
		}
		for j, el := range rc.storage.datetimes {
			if rc.Nullability != nil && rc.Nullability.Get(j) {
				hashes[j] ^= hashNull * mul
				continue
			}
			binary.LittleEndian.PutUint64(buf[:], uint64(el))
			hasher.Write(buf[:])
			hashes[j] ^= hasher.Sum64() * mul
			hasher.Reset()
		}
	case DtypeString:
		if rc.IsLiteral {
			offsetStart, offsetEnd := rc.storage.offsets[0], rc.storage.offsets[1]
			hasher.Write(rc.storage.strings[offsetStart:offsetEnd])
			sum := hasher.Sum64() * mul

			for j := range hashes {
				hashes[j] ^= sum
			}
			return
		}

		for j := 0; j < rc.Len(); j++ {
			if rc.Nullability != nil && rc.Nullability.Get(j) {
				hashes[j] ^= hashNull * mul
				continue
			}
			offsetStart := rc.storage.offsets[j]
			offsetEnd := rc.storage.offsets[j+1]
			hasher.Write(rc.storage.strings[offsetStart:offsetEnd])
			hashes[j] ^= hasher.Sum64() * mul
			hasher.Reset()
		}
	default:
		panic(fmt.Sprintf("no support for hashing for dtype %v", rc.dtype))
	}
}

func (rc *Chunk) Append(nrc *Chunk) error {
	if rc.IsLiteral {
		return fmt.Errorf("cannot add values to literal chunks: %w", errNoAddToLiterals)
	}
	if rc.dtype != nrc.dtype {
		return errAppendTypeMismatch
	}

	if rc.Nullability == nil && nrc.Nullability != nil {
		rc.Nullability = bitmap.NewBitmap(rc.Len())
	}
	if nrc.Nullability == nil && rc.Nullability != nil {
		nrc.Nullability = bitmap.NewBitmap(nrc.Len())
	}
	if rc.Nullability != nil {
		rc.Nullability.Append(nrc.Nullability)
	}
	rc.length += nrc.length

	switch rc.dtype {
	case DtypeString:
		off := uint32(0)
		if rc.length > 0 {
			off = rc.storage.offsets[len(rc.storage.offsets)-1]
		}

		if nrc.IsLiteral {
			vlength := nrc.storage.offsets[1]
			for j := 0; j < nrc.Len(); j++ {
				rc.storage.strings = append(rc.storage.strings, nrc.storage.strings...)
				rc.storage.offsets = append(rc.storage.offsets, off+vlength*uint32(j+1))
			}
		} else {
			rc.storage.strings = append(rc.storage.strings, nrc.storage.strings...)
			for _, el := range nrc.storage.offsets[1:] {
				rc.storage.offsets = append(rc.storage.offsets, el+off)
			}
		}
	case DtypeInt:
		if nrc.IsLiteral {
			value := nrc.storage.ints[0] // TODO/ARCH: nthValue? (in all implementations here)
			for j := 0; j < nrc.Len(); j++ {
				rc.storage.ints = append(rc.storage.ints, value)
			}
		} else {
			rc.storage.ints = append(rc.storage.ints, nrc.storage.ints...)
		}
	case DtypeFloat:
		if nrc.IsLiteral {
			value := nrc.storage.floats[0]
			for j := 0; j < nrc.Len(); j++ {
				rc.storage.floats = append(rc.storage.floats, value)
			}
		} else {
			rc.storage.floats = append(rc.storage.floats, nrc.storage.floats...)
		}
	case DtypeBool:
		if nrc.IsLiteral {
			value := nrc.storage.bools.Get(0)
			for j := 0; j < nrc.Len(); j++ {
				rc.storage.bools.Set(int(rc.length)+j, value)
			}
		} else {
			rc.storage.bools.Append(nrc.storage.bools)
		}
	case DtypeDate:
		if nrc.IsLiteral {
			value := nrc.storage.dates[0]
			for j := 0; j < nrc.Len(); j++ {
				rc.storage.dates = append(rc.storage.dates, value)
			}
		} else {
			rc.storage.dates = append(rc.storage.dates, nrc.storage.dates...)
		}
	case DtypeDatetime:
		if nrc.IsLiteral {
			value := nrc.storage.datetimes[0]
			for j := 0; j < nrc.Len(); j++ {
				rc.storage.datetimes = append(rc.storage.datetimes, value)
			}
		} else {
			rc.storage.datetimes = append(rc.storage.datetimes, nrc.storage.datetimes...)
		}
	case DtypeNull:
		// we only need to increase its length, and that's already done
	default:
		return fmt.Errorf("no support for Append for Dtype %v", rc.dtype)
	}

	return nil
}

// Prune filter this chunk and only preserves values for which the bitmap is set
func (rc *Chunk) Prune(bm *bitmap.Bitmap) *Chunk {
	if rc.IsLiteral {
		// TODO: pruning could be implemented by hydrating this chunk (disabling isLiteral)
		panic("pruning not supported in literal chunks")
	}
	nc := NewChunk(rc.dtype)
	if bm == nil {
		return nc
	}
	if bm.Cap() != rc.Len() {
		panic("pruning bitmap does not align with the dataset")
	}

	// if we're not pruning anything, we might just return ourselves
	// we don't need to clone anything, since the Chunk itself is immutable, right?
	// well... appends?
	if bm.Count() == rc.Len() {
		return rc
	}

	// we can short-circuit null-chunks
	if rc.dtype == DtypeNull {
		nc.length = uint32(bm.Count())
		return nc
	}

	// OPTIM: nthValue is not the fastest, just iterate over offsets directly
	// OR, just iterate over positive bits in our Bitmap - this will be super fast for sparse bitmaps
	// the bitmap iteration could be implemented in all the typed chunks
	index := 0
	for j := 0; j < rc.Len(); j++ {
		if !bm.Get(j) {
			continue
		}
		switch rc.dtype {
		case DtypeInt:
			nc.storage.ints = append(nc.storage.ints, rc.storage.ints[j])
		case DtypeFloat:
			nc.storage.floats = append(nc.storage.floats, rc.storage.floats[j])
		case DtypeDate:
			nc.storage.dates = append(nc.storage.dates, rc.storage.dates[j])
		case DtypeDatetime:
			nc.storage.datetimes = append(nc.storage.datetimes, rc.storage.datetimes[j])
		case DtypeBool:
			// OPTIM: not need to set false values, we already have them set as zero
			nc.storage.bools.Set(index, rc.storage.bools.Get(j))
		case DtypeString:
			// be careful here, AddValue has its own nullability logic and we don't want to mess with that
			if err := nc.AddValue(rc.nthValue(j)); err != nil {
				panic(err)
			}
		default:
			panic(fmt.Sprintf("unsupported dtype for pruning: %v", rc.dtype))
		}

		if rc.Nullability != nil && rc.Nullability.Get(j) {
			// ARCH: consider making Set a package function (bitmap.Set) to handle nilness
			if nc.Nullability == nil {
				nc.Nullability = bitmap.NewBitmap(index)
			}
			nc.Nullability.Set(index, true)
		}
		// nc.length++ // once we remove AddValue, we'll need this
		index++
	}

	// make sure the nullability vector aligns with the data
	if nc.Nullability != nil {
		nc.Nullability.Ensure(nc.Len())
	}

	return nc
}

// Deserialize reads a chunk from a reader
// this shouldn't really accept a Dtype - at this point we're requiring it, because we don't serialize Dtypes
// into the binary representation - but that's just because we always have the schema at hand... but will we always have it?
// shouldn't the files be readable as standalone files?
// OPTIM: shouldn't we deserialize based on a byte slice instead? We already have it, so we're just duplicating it using a byte buffer
// OPTIM: we may be able to safely cast these byte slice in the future - see https://github.com/golang/go/issues/19367
func Deserialize(r io.Reader, Dtype Dtype) (*Chunk, error) {
	ch := NewChunk(Dtype)
	if Dtype == DtypeNull {
		if err := binary.Read(r, binary.LittleEndian, &ch.length); err != nil {
			return nil, err
		}
		return ch, nil
	}

	bm, err := bitmap.DeserializeBitmapFromReader(r)
	if err != nil {
		return nil, err
	}
	ch.Nullability = bm

	switch Dtype {
	case DtypeString:
		var lenOffsets uint32
		if err := binary.Read(r, binary.LittleEndian, &lenOffsets); err != nil {
			return nil, err
		}
		offsets := make([]uint32, lenOffsets)
		if err := binary.Read(r, binary.LittleEndian, &offsets); err != nil {
			return nil, err
		}

		var lenData uint32
		if err := binary.Read(r, binary.LittleEndian, &lenData); err != nil {
			return nil, err
		}
		data := make([]byte, lenData)
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, err
		}
		ch.length = lenOffsets - 1
		ch.storage.strings = data
		ch.storage.offsets = offsets

		return ch, nil
	case DtypeInt:
		// OPTIM/ARCH: this &ch.length reading is common across int/float/date/datetime
		if err := binary.Read(r, binary.LittleEndian, &ch.length); err != nil {
			return nil, err
		}
		ch.storage.ints = make([]int64, ch.length)
		if err := binary.Read(r, binary.LittleEndian, &ch.storage.ints); err != nil {
			return nil, err
		}
		return ch, nil
	case DtypeFloat:
		if err := binary.Read(r, binary.LittleEndian, &ch.length); err != nil {
			return nil, err
		}
		ch.storage.floats = make([]float64, ch.length)
		if err := binary.Read(r, binary.LittleEndian, &ch.storage.floats); err != nil {
			return nil, err
		}
		return ch, nil
	case DtypeDatetime:
		if err := binary.Read(r, binary.LittleEndian, &ch.length); err != nil {
			return nil, err
		}
		ch.storage.datetimes = make([]datetime, ch.length)
		if err := binary.Read(r, binary.LittleEndian, &ch.storage.datetimes); err != nil {
			return nil, err
		}
		return ch, nil
	case DtypeDate:
		if err := binary.Read(r, binary.LittleEndian, &ch.length); err != nil {
			return nil, err
		}
		ch.storage.dates = make([]date, ch.length)
		if err := binary.Read(r, binary.LittleEndian, &ch.storage.dates); err != nil {
			return nil, err
		}
		return ch, nil
	case DtypeBool:
		if err := binary.Read(r, binary.LittleEndian, &ch.length); err != nil {
			return nil, err
		}
		data, err := bitmap.DeserializeBitmapFromReader(r)
		if err != nil {
			return nil, err
		}
		// an empty bitmap deserialises as a <nil>, so we'll initialise it here, just to
		// make it a valid container
		// Note: it doesn't deserialise as a NewBitmap(0), because we want our nullability bitmaps
		// to stay small and possibly empty
		if data == nil {
			data = bitmap.NewBitmap(0)
		}
		ch.storage.bools = data
		return ch, nil
	}
	panic(fmt.Sprintf("unsupported Dtype: %v", Dtype))
}

// WriteTo converts a chunk into its binary representation
func (rc *Chunk) WriteTo(w io.Writer) (int64, error) {
	if rc.IsLiteral {
		return 0, errLiteralsCannotBeSerialised
	}
	// TODO(PR): check that we can serialize this for DtypeNull
	nb, err := bitmap.Serialize(w, rc.Nullability)
	if err != nil {
		return 0, err
	}

	switch rc.dtype {
	case DtypeString:
		if err := binary.Write(w, binary.LittleEndian, uint32(len(rc.storage.offsets))); err != nil {
			return 0, err
		}
		// OPTIM: find the largest offset (the last one) and if it's less than 1<<16, use a smaller uint etc.
		if err := binary.Write(w, binary.LittleEndian, rc.storage.offsets); err != nil {
			return 0, err
		}
		if err := binary.Write(w, binary.LittleEndian, uint32(len(rc.storage.strings))); err != nil {
			return 0, err
		}
		bdata, err := w.Write(rc.storage.strings)
		if err != nil {
			return 0, err
		}
		if bdata != len(rc.storage.strings) {
			return 0, errors.New("not enough data written")
		}
		return int64(nb + 4 + len(rc.storage.offsets)*4 + 4 + len(rc.storage.strings)), err
	case DtypeInt:
		if err := binary.Write(w, binary.LittleEndian, uint32(len(rc.storage.ints))); err != nil {
			return 0, err
		}
		// OPTIM: find the largest int and possibly use a smaller container than int64
		err = binary.Write(w, binary.LittleEndian, rc.storage.ints)
		return int64(nb + 4 + 8*len(rc.storage.ints)), err
	case DtypeFloat:
		if err := binary.Write(w, binary.LittleEndian, uint32(len(rc.storage.floats))); err != nil {
			return 0, err
		}
		err = binary.Write(w, binary.LittleEndian, rc.storage.floats)
		return int64(nb + 4 + 8*len(rc.storage.floats)), err
	case DtypeBool:
		// the data bitmap doesn't have a "length", just a capacity (64 aligned), so we
		// need to explicitly write the length of this column chunk
		if err := binary.Write(w, binary.LittleEndian, rc.length); err != nil {
			return 0, err
		}
		nbd, err := bitmap.Serialize(w, rc.storage.bools)
		if err != nil {
			return 0, err
		}
		return int64(nb + 4 + nbd), nil
	case DtypeDate:
		if err := binary.Write(w, binary.LittleEndian, uint32(len(rc.storage.dates))); err != nil {
			return 0, err
		}
		err = binary.Write(w, binary.LittleEndian, rc.storage.dates)
		return int64(nb + 4 + DATE_BYTE_SIZE*len(rc.storage.dates)), err
	case DtypeDatetime:
		if err := binary.Write(w, binary.LittleEndian, uint32(len(rc.storage.datetimes))); err != nil {
			return 0, err
		}
		err = binary.Write(w, binary.LittleEndian, rc.storage.datetimes)
		return int64(nb + 4 + DATETIME_BYTE_SIZE*len(rc.storage.datetimes)), err
	case DtypeNull:
		length := rc.length
		if err := binary.Write(w, binary.LittleEndian, length); err != nil {
			return 0, err
		}
		return int64(4), nil
	default:
		return 0, fmt.Errorf("cannot serialize dtype %v", rc.dtype)
	}
}

func (rc *Chunk) Clone() *Chunk {
	var nulls *bitmap.Bitmap
	if rc.Nullability != nil {
		nulls = rc.Nullability.Clone()
	}
	ch := NewChunk(rc.dtype)
	ch.Nullability = nulls
	ch.length = rc.length
	rc.IsLiteral = rc.IsLiteral

	switch rc.dtype {
	case DtypeBool:
		if rc.storage.bools != nil {
			ch.storage.bools = rc.storage.bools.Clone()
		}
	case DtypeNull:
		// nothing to be done here
	case DtypeString:
		ch.storage.offsets = append(rc.storage.offsets[:0:0], rc.storage.offsets...)
		ch.storage.strings = append(rc.storage.strings[:0:0], rc.storage.strings...)
	case DtypeFloat:
		ch.storage.floats = append(rc.storage.floats[:0:0], rc.storage.floats...)
	case DtypeInt:
		ch.storage.ints = append(rc.storage.ints[:0:0], rc.storage.ints...)
	case DtypeDate:
		ch.storage.dates = append(rc.storage.dates[:0:0], rc.storage.dates...)
	case DtypeDatetime:
		ch.storage.datetimes = append(rc.storage.datetimes[:0:0], rc.storage.datetimes...)
	}

	return ch
}

func (rc *Chunk) JSONLiteral(n int) (string, bool) {
	if rc.Nullability != nil && rc.Nullability.Get(n) {
		return "", false
	}

	switch rc.dtype {
	case DtypeString:
		val := rc.nthValue(n)
		ret, err := json.Marshal(val)
		if err != nil {
			panic(err)
		}

		return string(ret), true
	case DtypeInt:
		if rc.IsLiteral {
			return fmt.Sprintf("%v", rc.storage.ints[0]), true
		}

		return fmt.Sprintf("%v", rc.storage.ints[n]), true
	case DtypeFloat:
		val := rc.storage.floats[0]
		if !rc.IsLiteral {
			val = rc.storage.floats[n]
		}
		// ARCH: this shouldn't happen? (it used to happen in division by zero... can it happen anywhere else?)
		if math.IsNaN(val) || math.IsInf(val, 0) {
			return "", false
		}

		return fmt.Sprintf("%v", val), true
	case DtypeBool:
		if rc.IsLiteral {
			return fmt.Sprintf("%v", rc.storage.bools.Get(0)), true
		}

		return fmt.Sprintf("%v", rc.storage.bools.Get(n)), true
	case DtypeDate:
		val := rc.storage.dates[0]
		if !rc.IsLiteral {
			val = rc.storage.dates[n]
		}
		ret, err := json.Marshal(val)
		if err != nil {
			panic(err)
		}
		return string(ret), true
	case DtypeDatetime:
		val := rc.storage.datetimes[0]
		if !rc.IsLiteral {
			val = rc.storage.datetimes[n]
		}
		ret, err := json.Marshal(val)
		if err != nil {
			panic(err)
		}
		return string(ret), true
	case DtypeNull:
		return "", false
	default:
		panic(fmt.Sprintf("no support for JSONLiteral for Dtype %v", rc.dtype))
	}
}

func compareOneNull(ltv int, nullsFirst bool, null1, null2 bool) int {
	if (null1 && nullsFirst) || (null2 && !nullsFirst) {
		return ltv
	}
	return -ltv
}

func compareValues(ltv int, lt, eq bool) int {
	if eq {
		return 0
	}
	if lt {
		return ltv
	}
	return -ltv
}

func comparisonFactory(asc, nullsFirst, isLiteral, isNullable, lt, eq, n1, n2 bool) int {
	ltv := -1
	if !asc {
		ltv = 1
	}
	if isLiteral {
		return 0
	}
	if isNullable && (n1 || n2) {
		if n1 && n2 {
			return 0
		}
		return compareOneNull(ltv, nullsFirst, n1, n2)
	}
	return compareValues(ltv, lt, eq)
}

// TODO(next): docs
// ARCH: this could be made entirely generic by allowing an interface `nthValue(int) T` to genericise v1/v2
//       EXCEPT for bools :-( (not comparable)
func (rc *Chunk) Compare(asc, nullsFirst bool, i, j int) int {
	var n1, n2 bool
	if rc.Nullability != nil {
		n1, n2 = rc.Nullability.Get(i), rc.Nullability.Get(j)
	}

	// OPTIM: this will be slow, so we should consider [DtypeMax]Compare closures
	switch rc.dtype {
	case DtypeInt:
		v1, v2 := rc.storage.ints[i], rc.storage.ints[j]

		return comparisonFactory(asc, nullsFirst, rc.IsLiteral, rc.Nullability != nil, v1 < v2, v1 == v2, n1, n2)
	case DtypeFloat:
		// TODO: do we have to worry about inf/nans? I thought we eliminated them from the .data slice
		v1, v2 := rc.storage.floats[i], rc.storage.floats[j]

		return comparisonFactory(asc, nullsFirst, rc.IsLiteral, rc.Nullability != nil, v1 < v2, v1 == v2, n1, n2)
	case DtypeString:
		v1, v2 := rc.nthValue(i), rc.nthValue(j)

		return comparisonFactory(asc, nullsFirst, rc.IsLiteral, rc.Nullability != nil, v1 < v2, v1 == v2, n1, n2)
	case DtypeBool:
		v1, v2 := rc.storage.bools.Get(i), rc.storage.bools.Get(j)
		lt := !v1 && v2

		return comparisonFactory(asc, nullsFirst, rc.IsLiteral, rc.Nullability != nil, lt, v1 == v2, n1, n2)
	case DtypeDate:
		v1, v2 := rc.storage.dates[i], rc.storage.dates[j]

		return comparisonFactory(asc, nullsFirst, rc.IsLiteral, rc.Nullability != nil, v1 < v2, v1 == v2, n1, n2)
	case DtypeDatetime:
		v1, v2 := rc.storage.datetimes[i], rc.storage.datetimes[j]

		return comparisonFactory(asc, nullsFirst, rc.IsLiteral, rc.Nullability != nil, v1 < v2, v1 == v2, n1, n2)
	case DtypeNull:
		return 0
	default:
		panic(fmt.Sprintf("unsupported Dtype for Compare: %v", rc.dtype))
	}
}
