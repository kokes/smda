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
type Chunk interface {
	baseChunker
	Dtype() Dtype
	AddValue(string) error
	AddValues([]string) error // consider merging AddValues and AddValue (using varargs)
	WriteTo(io.Writer) (int64, error)
	Prune(*bitmap.Bitmap) Chunk
	Append(Chunk) error
	Hash(int, []uint64)
	Clone() Chunk
	JSONLiteral(int) (string, bool) // the bool stands for 'ok' (not null)
	Compare(bool, bool, int, int) int
}

type baseChunker interface {
	Base() *baseChunk
	baseEqual(baseChunker) bool
	Len() int
	Nullify(*bitmap.Bitmap)
	// IsNullable() bool
}

type baseChunk struct {
	length      uint32
	IsLiteral   bool
	Nullability *bitmap.Bitmap
}

// ARCH: we sometimes use this, sometimes we access the struct field directly... perhaps remove this?
func (bc *baseChunk) Len() int {
	return int(bc.length)
}

// ARCH: Nullify does NOT switch the data values to be nulls/empty as well
func (bc *baseChunk) Nullify(bm *bitmap.Bitmap) {
	// OPTIM: this copies, but it covers all the cases
	bc.Nullability = bitmap.Or(bc.Nullability, bm)
}

// this might be useful for COALESCE, among other things
// though this is misleading - a column can be nullable, but have its nullability
// bitmap nil - a nearly full column will have its chunks without nulls and thus
// with nullability == nil. We should only talk about nullability in the context
// of schemas
// func (bc *baseChunk) IsNullable() bool {
// 	return bc.nullability != nil
// }

func (bc *baseChunk) Base() *baseChunk {
	return bc
}
func (bc *baseChunk) baseEqual(bcb baseChunker) bool {
	bc2 := bcb.Base()
	if bc.Len() != bc2.Len() {
		return false
	}
	if bc.IsLiteral != bc2.IsLiteral {
		return false
	}
	if !reflect.DeepEqual(bc.Nullability, bc2.Nullability) {
		return false
	}
	return true
}

// ChunksEqual compares two chunks, even if they contain []float64 data
// consider making this lenient enough to compare only the relevant bits in ChunkBools
func ChunksEqual(c1 Chunk, c2 Chunk) bool {
	// ARCH: consider doing an .EqualBase() in baseChunk - compares dtype, length, isLiteral and nullability
	if c1.Dtype() != c2.Dtype() {
		return false
	}
	if !c1.baseEqual(c2) {
		return false
	}

	switch c1t := c1.(type) {
	case *ChunkBools:
		c2t := c2.(*ChunkBools)
		if c1t.Nullability == nil && c2t.Nullability == nil {
			return reflect.DeepEqual(c1t, c2t)
		}
		// compare only the valid bits in data
		// ARCH: what about the bits beyond the cap?
		// OPTIM: we don't have to clone here - we can easily iterate and check by blocks
		// data1[j] & ~nullability1[j] == data2[j] & ~nullability2[j] or something like that
		c1d := c1t.data.Clone()
		c1d.AndNot(c1t.Nullability)
		c2d := c2t.data.Clone()
		c2d.AndNot(c2t.Nullability)
		return reflect.DeepEqual(c1d, c2d)
	case *ChunkInts:
		c2t := c2.(*ChunkInts)
		if c1t.Nullability == nil && c2t.Nullability == nil {
			return reflect.DeepEqual(c1t, c2t)
		}
		for j := 0; j < c1t.Len(); j++ {
			if c1t.Nullability.Get(j) {
				continue
			}
			if c1t.data[j] != c2t.data[j] {
				return false
			}
		}
		return true
	case *ChunkFloats:
		c2t := c2.(*ChunkFloats)
		if c1t.Nullability == nil && c2t.Nullability == nil {
			return reflect.DeepEqual(c1t, c2t)
		}
		for j := 0; j < c1t.Len(); j++ {
			if c1t.Nullability.Get(j) {
				continue
			}
			if c1t.data[j] != c2t.data[j] {
				return false
			}
		}
		return true
	case *ChunkStrings:
		c2t := c2.(*ChunkStrings)
		if c1t.Nullability == nil && c2t.Nullability == nil {
			return reflect.DeepEqual(c1t, c2t)
		}
		for j := 0; j < c1t.Len(); j++ {
			if c1t.Nullability.Get(j) {
				continue
			}
			if c1t.nthValue(j) != c2t.nthValue(j) {
				return false
			}
		}
		return true
	case *ChunkDatetimes:
		c2t := c2.(*ChunkDatetimes)
		if c1t.Nullability == nil && c2t.Nullability == nil {
			return reflect.DeepEqual(c1t, c2t)
		}
		for j := 0; j < c1t.Len(); j++ {
			if c1t.Nullability.Get(j) {
				continue
			}
			if c1t.data[j] != c2t.data[j] {
				return false
			}
		}
		return true
	case *ChunkDates:
		c2t := c2.(*ChunkDates)
		if c1t.Nullability == nil && c2t.Nullability == nil {
			return reflect.DeepEqual(c1t, c2t)
		}
		for j := 0; j < c1t.Len(); j++ {
			if c1t.Nullability.Get(j) {
				continue
			}
			if c1t.data[j] != c2t.data[j] {
				return false
			}
		}
		return true
	case *ChunkNulls:
		c2t := c2.(*ChunkNulls)
		return c1t.length == c2t.length
	default:
		panic("type not supported")
	}
}

// NewChunkFromSchema creates a new Chunk based a column schema provided
func NewChunkFromSchema(schema Schema) Chunk {
	switch schema.Dtype {
	case DtypeString:
		return newChunkStrings()
	case DtypeInt:
		return newChunkInts()
	case DtypeFloat:
		return newChunkFloats()
	case DtypeBool:
		return newChunkBools()
	case DtypeDate:
		return newChunkDates()
	case DtypeDatetime:
		return newChunkDatetimes()
	case DtypeNull:
		return newChunkNulls()
	default:
		panic(fmt.Sprintf("unknown schema type: %v", schema.Dtype))
	}
}

func NewChunkLiteralTyped(s string, dtype Dtype, length int) (Chunk, error) {
	bc := baseChunk{
		IsLiteral: true,
		length:    uint32(length),
	}
	switch dtype {
	case DtypeInt:
		val, err := parseInt(s)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid typed literal: %v", errInvalidTypedLiteral, s)
		}
		return &ChunkInts{
			baseChunk: bc,
			data:      []int64{val},
		}, nil
	case DtypeFloat:
		val, err := parseFloat(s)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid typed literal: %v", errInvalidTypedLiteral, s)
		}
		if math.IsNaN(val) || math.IsInf(val, 0) {
			return nil, fmt.Errorf("cannot set %v as a literal float value", s)
		}
		return &ChunkFloats{
			baseChunk: bc,
			data:      []float64{val},
		}, nil
	case DtypeBool:
		bm := bitmap.NewBitmap(1)
		val, err := parseBool(s)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid typed literal: %v", errInvalidTypedLiteral, s)
		}
		if val {
			bm.Set(0, true)
		}
		return &ChunkBools{
			baseChunk: bc,
			data:      bm,
		}, nil
	case DtypeString:
		return &ChunkStrings{
			baseChunk: bc,
			data:      []byte(s),
			offsets:   []uint32{0, uint32(len(s))},
		}, nil
	case DtypeDate:
		val, err := parseDate(s)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid typed literal: %v", errInvalidTypedLiteral, s)
		}
		return &ChunkDates{
			baseChunk: bc,
			data:      []date{val},
		}, nil
	case DtypeDatetime:
		val, err := parseDatetime(s)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid typed literal: %v", errInvalidTypedLiteral, s)
		}
		return &ChunkDatetimes{
			baseChunk: bc,
			data:      []datetime{val},
		}, nil
	case DtypeNull:
		return &ChunkNulls{
			baseChunk: bc,
		}, nil
	default:
		return nil, fmt.Errorf("%w: invalid typed literal: unsupported type %v", errInvalidTypedLiteral, dtype)
	}
}

// NewChunkLiteral creates a chunk that only contains a single value in the whole chunk
// it's useful in e.g. 'foo > 1', where can convert the '1' to a whole chunk
// OPTIM: we're using single-value slices, should we perhaps have a value specific for each literal
// to avoid working with slices (stack allocation etc.)
func NewChunkLiteralAuto(s string, length int) (Chunk, error) {
	dtype := guessType(s)

	return NewChunkLiteralTyped(s, dtype, length)
}

// ChunkStrings defines a backing struct for a chunk of string values
type ChunkStrings struct {
	baseChunk
	data    []byte
	offsets []uint32
}

// ChunkInts defines a backing struct for a chunk of integer values
type ChunkInts struct {
	baseChunk
	data []int64
}

// ChunkFloats defines a backing struct for a chunk of floating point values
type ChunkFloats struct {
	baseChunk
	data []float64
}

// ChunkBools defines a backing struct for a chunk of boolean values
type ChunkBools struct {
	baseChunk
	data *bitmap.Bitmap
}

// ChunkDates defines a backing struct for a chunk of date values
type ChunkDates struct {
	baseChunk
	data []date
}
type ChunkDatetimes struct {
	baseChunk
	data []datetime
}

// ChunkNulls defines a backing struct for a chunk of null values
// Since it's all nulls, we only need to know how many there are
type ChunkNulls struct {
	baseChunk
}

// preallocate column data, so that slice appends don't trigger new reallocations
const defaultChunkCap = 512

func newChunkStrings() *ChunkStrings {
	offsets := make([]uint32, 1, defaultChunkCap)
	offsets[0] = 0
	return &ChunkStrings{
		data:    make([]byte, 0, defaultChunkCap),
		offsets: offsets,
	}
}

func NewChunkLiteralStrings(value string, length int) *ChunkStrings {
	offsets := []uint32{0, uint32(len(value))}
	return &ChunkStrings{
		baseChunk: baseChunk{
			IsLiteral: true,
			length:    uint32(length),
		},
		data:    []byte(value),
		offsets: offsets,
	}
}

func newChunkInts() *ChunkInts {
	return &ChunkInts{
		data: make([]int64, 0, defaultChunkCap),
	}
}

func NewChunkLiteralInts(value int64, length int) *ChunkInts {
	return &ChunkInts{
		baseChunk: baseChunk{
			IsLiteral: true,
			length:    uint32(length),
		},
		data: []int64{value},
	}
}

func newChunkFloats() *ChunkFloats {
	return &ChunkFloats{
		data: make([]float64, 0, defaultChunkCap),
	}
}

func NewChunkLiteralFloats(value float64, length int) *ChunkFloats {
	return &ChunkFloats{
		baseChunk: baseChunk{
			IsLiteral: true,
			length:    uint32(length),
		},
		data: []float64{value},
	}
}

func newChunkBools() *ChunkBools {
	return &ChunkBools{
		data: bitmap.NewBitmap(0),
	}
}

func NewChunkLiteralBools(value bool, length int) *ChunkBools {
	bm := bitmap.NewBitmap(1)
	bm.Set(0, value)
	return &ChunkBools{
		baseChunk: baseChunk{
			IsLiteral: true,
			length:    uint32(length),
		},
		data: bm,
	}
}

func newChunkDates() *ChunkDates {
	return &ChunkDates{
		data: make([]date, 0, defaultChunkCap),
	}
}
func newChunkDatetimes() *ChunkDatetimes {
	return &ChunkDatetimes{
		data: make([]datetime, 0, defaultChunkCap),
	}
}

func NewChunkLiteralDates(value date, length int) *ChunkDates {
	return &ChunkDates{
		baseChunk: baseChunk{
			IsLiteral: true,
			length:    uint32(length),
		},
		data: []date{value},
	}
}

// TODO/ARCH: consider removing this in favour of NewChunkBoolsFromBitmap
func newChunkBoolsFromBits(data []uint64, length int) *ChunkBools {
	return &ChunkBools{
		baseChunk: baseChunk{length: uint32(length)},
		data:      bitmap.NewBitmapFromBits(data, length),
	}
}

// NewChunkBoolsFromBitmap creates a new bool chunk, but in doing so, doesn't clone the incoming bitmap,
// it uses it as is - the caller might want to clone it aims to mutate it in the future
func NewChunkBoolsFromBitmap(bm *bitmap.Bitmap) *ChunkBools {
	return &ChunkBools{
		baseChunk: baseChunk{length: uint32(bm.Cap())},
		data:      bm,
	}
}

// the next few functions could use some generics
func NewChunkIntsFromSlice(data []int64, nulls *bitmap.Bitmap) *ChunkInts {
	return &ChunkInts{
		baseChunk: baseChunk{length: uint32(len(data)), Nullability: nulls},
		data:      data,
	}
}
func NewChunkFloatsFromSlice(data []float64, nulls *bitmap.Bitmap) *ChunkFloats {
	return &ChunkFloats{
		baseChunk: baseChunk{length: uint32(len(data)), Nullability: nulls},
		data:      data,
	}
}
func newChunkDatesFromSlice(data []date, nulls *bitmap.Bitmap) *ChunkDates {
	return &ChunkDates{
		baseChunk: baseChunk{length: uint32(len(data)), Nullability: nulls},
		data:      data,
	}
}
func newChunkDatetimesFromSlice(data []datetime, nulls *bitmap.Bitmap) *ChunkDatetimes {
	return &ChunkDatetimes{
		baseChunk: baseChunk{length: uint32(len(data)), Nullability: nulls},
		data:      data,
	}
}
func newChunkStringsFromSlice(data []string, nulls *bitmap.Bitmap) *ChunkStrings {
	rc := newChunkStrings()
	if err := rc.AddValues(data); err != nil {
		panic(err)
	}
	rc.Nullability = nulls
	return rc
}

// Truths returns only true values in this boolean column's bitmap - remove those
// that are null - we use this for filtering, when we're interested in non-null
// true values (to select given rows)
func (rc *ChunkBools) Truths() *bitmap.Bitmap {
	if rc.IsLiteral {
		// ARCH: still assuming literals are not nullable
		value := rc.data.Get(0)
		bm := bitmap.NewBitmap(rc.Len())
		if value {
			bm.Invert()
		}
		return bm
	}
	bm := rc.data.Clone()
	if rc.Nullability == nil || rc.Nullability.Count() == 0 {
		return bm
	}
	// cloning was necessary as AndNot mutates (and we're cloning for good measure - we
	// don't expect to mutate this downstream, but...)
	bm.AndNot(rc.Nullability)
	return bm
}

func newChunkNulls() *ChunkNulls {
	return &ChunkNulls{}
}

// Dtype returns the type of this chunk
func (rc *ChunkBools) Dtype() Dtype {
	return DtypeBool
}

// Dtype returns the type of this chunk
func (rc *ChunkFloats) Dtype() Dtype {
	return DtypeFloat
}

// Dtype returns the type of this chunk
func (rc *ChunkInts) Dtype() Dtype {
	return DtypeInt
}

// Dtype returns the type of this chunk
func (rc *ChunkNulls) Dtype() Dtype {
	return DtypeNull
}

// Dtype returns the type of this chunk
func (rc *ChunkStrings) Dtype() Dtype {
	return DtypeString
}

// Dtype returns the type of this chunk
func (rc *ChunkDates) Dtype() Dtype {
	return DtypeDate
}

// Dtype returns the type of this chunk
func (rc *ChunkDatetimes) Dtype() Dtype {
	return DtypeDatetime
}

// TODO: does not support nullability, we should probably get rid of the whole thing anyway (only used for testing now)
// BUT, we're sort of using it for type inference - so maybe caveat it with a note that it's only to be used with
// not nullable columns?
// we could also use it for other types (especially bools)
func (rc *ChunkStrings) nthValue(n int) string {
	if rc.IsLiteral && n > 0 {
		return rc.nthValue(0)
	}
	offsetStart := rc.offsets[n]
	offsetEnd := rc.offsets[n+1]
	return string(rc.data[offsetStart:offsetEnd])
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

// Hash hashes this chunk's values into a provded container
func (rc *ChunkBools) Hash(position int, hashes []uint64) {
	mul := positionMultiplier(position)
	// TODO: we may have to revisit the idea that literal chunks cannot be nullable (review all the uses of isLiteral in this package)
	if rc.IsLiteral {
		hashVal := hashBoolFalse
		if rc.data.Get(0) {
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
		if rc.data.Get(j) {
			hashes[j] ^= hashBoolTrue * mul
		} else {
			hashes[j] ^= hashBoolFalse * mul
		}
	}
}

// TODO(generics): type Hasher[T] struct {...}, Sum[T] -> uint64
// Or maybe just `Chunk.NthHash(j int) uint64` - so it could be a part of the Chunk interface?

// Hash hashes this chunk's values into a provded container
// OPTIM/TODO(next): do we need a fnv hasher for ints/floats/dates? We can just take the uint64 representation
// of these values... or not?
func (rc *ChunkFloats) Hash(position int, hashes []uint64) {
	mul := positionMultiplier(position)
	var buf [8]byte
	hasher := fnv.New64()
	if rc.IsLiteral {
		binary.LittleEndian.PutUint64(buf[:], math.Float64bits(rc.data[0]))
		hasher.Write(buf[:])
		sum := hasher.Sum64() * mul

		for j := range hashes {
			hashes[j] ^= sum
		}
		return
	}
	for j, el := range rc.data {
		if rc.Nullability != nil && rc.Nullability.Get(j) {
			hashes[j] ^= hashNull * mul
			continue
		}
		binary.LittleEndian.PutUint64(buf[:], math.Float64bits(el))
		hasher.Write(buf[:])
		hashes[j] ^= hasher.Sum64() * mul
		hasher.Reset()
	}
}

// Hash hashes this chunk's values into a provded container
// OPTIM: maphash might be faster than fnv or maphash? test it and if it is so, implement
// everywhere, but be careful about the seed (needs to be the same for all chunks)
// careful about maphash: "The hash value of a given byte sequence is consistent within a single process, but will be different in different processes."
// oh and I rebenchmarked maphash and fnv and found maphash to be much slower (despite no allocs)
// also, check this https://github.com/segmentio/fasthash/ (via https://segment.com/blog/allocation-efficiency-in-high-performance-go-services/)
// they reimplement fnv using stack allocation only
//   - we tested it and got a 90% speedup (no allocs, shorter code) - so let's consider it, it's in the fasthash branch
func (rc *ChunkInts) Hash(position int, hashes []uint64) {
	mul := positionMultiplier(position)
	var buf [8]byte
	hasher := fnv.New64()
	// ARCH: literal chunks don't have nullability support... should we check for that?
	if rc.IsLiteral {
		binary.LittleEndian.PutUint64(buf[:], uint64(rc.data[0]))
		hasher.Write(buf[:])
		sum := hasher.Sum64() * mul

		for j := range hashes {
			hashes[j] ^= sum
		}
		return
	}
	for j, el := range rc.data {
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
}

// Hash hashes this chunk's values into a provded container
func (rc *ChunkNulls) Hash(position int, hashes []uint64) {
	mul := positionMultiplier(position)
	for j := range hashes {
		hashes[j] ^= hashNull * mul
	}
}

func (rc *ChunkDates) Hash(position int, hashes []uint64) {
	mul := positionMultiplier(position)
	var buf [8]byte
	hasher := fnv.New64()
	// ARCH: literal chunks don't have nullability support... should we check for that?
	if rc.IsLiteral {
		binary.LittleEndian.PutUint64(buf[:], uint64(rc.data[0]))
		hasher.Write(buf[:])
		sum := hasher.Sum64() * mul

		for j := range hashes {
			hashes[j] ^= sum
		}
		return
	}
	for j, el := range rc.data {
		if rc.Nullability != nil && rc.Nullability.Get(j) {
			hashes[j] ^= hashNull * mul
			continue
		}
		binary.LittleEndian.PutUint64(buf[:], uint64(el))
		hasher.Write(buf[:])
		hashes[j] ^= hasher.Sum64() * mul
		hasher.Reset()
	}
}

func (rc *ChunkDatetimes) Hash(position int, hashes []uint64) {
	mul := positionMultiplier(position)
	var buf [8]byte
	hasher := fnv.New64()
	// ARCH: literal chunks don't have nullability support... should we check for that?
	if rc.IsLiteral {
		binary.LittleEndian.PutUint64(buf[:], uint64(rc.data[0]))
		hasher.Write(buf[:])
		sum := hasher.Sum64() * mul

		for j := range hashes {
			hashes[j] ^= sum
		}
		return
	}
	for j, el := range rc.data {
		if rc.Nullability != nil && rc.Nullability.Get(j) {
			hashes[j] ^= hashNull * mul
			continue
		}
		binary.LittleEndian.PutUint64(buf[:], uint64(el))
		hasher.Write(buf[:])
		hashes[j] ^= hasher.Sum64() * mul
		hasher.Reset()
	}
}

// Hash hashes this chunk's values into a provded container
func (rc *ChunkStrings) Hash(position int, hashes []uint64) {
	mul := positionMultiplier(position)
	hasher := fnv.New64()
	if rc.IsLiteral {
		offsetStart, offsetEnd := rc.offsets[0], rc.offsets[1]
		hasher.Write(rc.data[offsetStart:offsetEnd])
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
		offsetStart := rc.offsets[j]
		offsetEnd := rc.offsets[j+1]
		hasher.Write(rc.data[offsetStart:offsetEnd])
		hashes[j] ^= hasher.Sum64() * mul
		hasher.Reset()
	}
}

// AddValue takes in a string representation of a value and converts it into
// a value suited for this chunk
func (rc *ChunkStrings) AddValue(s string) error {
	if rc.IsLiteral {
		return fmt.Errorf("cannot add values to literal chunks: %w", errNoAddToLiterals)
	}
	rc.data = append(rc.data, []byte(s)...)

	valLen := uint32(len(s))
	valLen += rc.offsets[len(rc.offsets)-1]
	rc.offsets = append(rc.offsets, valLen)

	rc.length++
	if rc.Nullability != nil {
		rc.Nullability.Ensure(int(rc.length))
	}
	return nil
}

// AddValue takes in a string representation of a value and converts it into
// a value suited for this chunk
func (rc *ChunkInts) AddValue(s string) error {
	if rc.IsLiteral {
		return fmt.Errorf("cannot add values to literal chunks: %w", errNoAddToLiterals)
	}
	if isNull(s) {
		if rc.Nullability == nil {
			rc.Nullability = bitmap.NewBitmap(rc.Len() + 1)
		}
		rc.Nullability.Set(rc.Len(), true)
		rc.data = append(rc.data, 0) // this value is not meant to be read
		rc.length++
		return nil
	}

	val, err := parseInt(s)
	if err != nil {
		return err
	}
	rc.data = append(rc.data, val)
	rc.length++
	if rc.Nullability != nil {
		rc.Nullability.Ensure(int(rc.length))
	}
	return nil
}

// AddValue takes in a string representation of a value and converts it into
// a value suited for this chunk
// let's really consider adding standard nulls here, it will probably make our lives a lot easier
func (rc *ChunkFloats) AddValue(s string) error {
	if rc.IsLiteral {
		return fmt.Errorf("cannot add values to literal chunks: %w", errNoAddToLiterals)
	}
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
		rc.data = append(rc.data, 0) // this value is not meant to be read
		rc.length++
		return nil
	}

	rc.data = append(rc.data, val)
	rc.length++
	// make sure the nullability bitmap aligns with the length of the chunk
	if rc.Nullability != nil {
		rc.Nullability.Ensure(int(rc.length))
	}
	return nil
}

// AddValue takes in a string representation of a value and converts it into
// a value suited for this chunk
func (rc *ChunkBools) AddValue(s string) error {
	if rc.IsLiteral {
		return fmt.Errorf("cannot add values to literal chunks: %w", errNoAddToLiterals)
	}
	if isNull(s) {
		if rc.Nullability == nil {
			rc.Nullability = bitmap.NewBitmap(rc.Len() + 1)
		}
		rc.Nullability.Set(rc.Len(), true)
		rc.data.Set(rc.Len(), false) // this value is not meant to be read
		rc.length++
		return nil
	}
	val, err := parseBool(s)
	if err != nil {
		return err
	}
	rc.data.Set(rc.Len(), val)
	rc.length++
	// make sure the nullability bitmap aligns with the length of the chunk
	if rc.Nullability != nil {
		rc.Nullability.Ensure(int(rc.length))
	}
	return nil
}

func (rc *ChunkDates) AddValue(s string) error {
	if rc.IsLiteral {
		return fmt.Errorf("cannot add values to literal chunks: %w", errNoAddToLiterals)
	}
	if isNull(s) {
		if rc.Nullability == nil {
			rc.Nullability = bitmap.NewBitmap(rc.Len() + 1)
		}
		rc.Nullability.Set(rc.Len(), true)
		rc.data = append(rc.data, 0) // this value is not meant to be read
		rc.length++
		return nil
	}

	val, err := parseDate(s)
	if err != nil {
		return err
	}
	rc.data = append(rc.data, val)
	rc.length++
	if rc.Nullability != nil {
		rc.Nullability.Ensure(int(rc.length))
	}
	return nil
}
func (rc *ChunkDatetimes) AddValue(s string) error {
	if rc.IsLiteral {
		return fmt.Errorf("cannot add values to literal chunks: %w", errNoAddToLiterals)
	}
	if isNull(s) {
		if rc.Nullability == nil {
			rc.Nullability = bitmap.NewBitmap(rc.Len() + 1)
		}
		rc.Nullability.Set(rc.Len(), true)
		rc.data = append(rc.data, 0) // this value is not meant to be read
		rc.length++
		return nil
	}

	val, err := parseDatetime(s)
	if err != nil {
		return err
	}
	rc.data = append(rc.data, val)
	rc.length++
	if rc.Nullability != nil {
		rc.Nullability.Ensure(int(rc.length))
	}
	return nil
}

// AddValue takes in a string representation of a value and converts it into
// a value suited for this chunk
func (rc *ChunkNulls) AddValue(s string) error {
	if !isNull(s) {
		return fmt.Errorf("a null column expects null values, got: %v", s)
	}
	rc.length++
	return nil
}

// AddValues is a helper method, it just calls AddValue repeatedly
func (rc *ChunkBools) AddValues(vals []string) error {
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

// AddValues is a helper method, it just calls AddValue repeatedly
func (rc *ChunkFloats) AddValues(vals []string) error {
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

// AddValues is a helper method, it just calls AddValue repeatedly
func (rc *ChunkInts) AddValues(vals []string) error {
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

// AddValues is a helper method, it just calls AddValue repeatedly
func (rc *ChunkDates) AddValues(vals []string) error {
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

// AddValues is a helper method, it just calls AddValue repeatedly
func (rc *ChunkDatetimes) AddValues(vals []string) error {
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

// AddValues is a helper method, it just calls AddValue repeatedly
func (rc *ChunkNulls) AddValues(vals []string) error {
	for _, el := range vals {
		if err := rc.AddValue(el); err != nil {
			return err
		}
	}
	return nil
}

// AddValues is a helper method, it just calls AddValue repeatedly
func (rc *ChunkStrings) AddValues(vals []string) error {
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

// Append adds a chunk of the same type at the end of this one (in place update)
func (rc *ChunkStrings) Append(tc Chunk) error {
	if rc.IsLiteral {
		return fmt.Errorf("cannot add values to literal chunks: %w", errNoAddToLiterals)
	}
	nrc, ok := tc.(*ChunkStrings)
	if !ok {
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

	off := uint32(0)
	if rc.length > 0 {
		off = rc.offsets[len(rc.offsets)-1]
	}

	if nrc.IsLiteral {
		vlength := nrc.offsets[1]
		for j := 0; j < nrc.Len(); j++ {
			rc.data = append(rc.data, nrc.data...)
			rc.offsets = append(rc.offsets, off+vlength*uint32(j+1))
		}
	} else {
		rc.data = append(rc.data, nrc.data...)
		for _, el := range nrc.offsets[1:] {
			rc.offsets = append(rc.offsets, el+off)
		}
	}

	return nil
}

// Append adds a chunk of the same type at the end of this one (in place update)
func (rc *ChunkInts) Append(tc Chunk) error {
	if rc.IsLiteral {
		return fmt.Errorf("cannot add values to literal chunks: %w", errNoAddToLiterals)
	}
	nrc, ok := tc.(*ChunkInts)
	if !ok {
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

	if nrc.IsLiteral {
		value := nrc.data[0] // TODO/ARCH: nthValue? (in all implementations here)
		for j := 0; j < nrc.Len(); j++ {
			rc.data = append(rc.data, value)
		}
	} else {
		rc.data = append(rc.data, nrc.data...)
	}
	rc.length += nrc.length

	return nil
}

// Append adds a chunk of the same type at the end of this one (in place update)
func (rc *ChunkFloats) Append(tc Chunk) error {
	if rc.IsLiteral {
		return fmt.Errorf("cannot add values to literal chunks: %w", errNoAddToLiterals)
	}
	nrc, ok := tc.(*ChunkFloats)
	if !ok {
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

	if nrc.IsLiteral {
		value := nrc.data[0]
		for j := 0; j < nrc.Len(); j++ {
			rc.data = append(rc.data, value)
		}
	} else {
		rc.data = append(rc.data, nrc.data...)
	}
	rc.length += nrc.length

	return nil
}

// Append adds a chunk of the same type at the end of this one (in place update)
func (rc *ChunkBools) Append(tc Chunk) error {
	if rc.IsLiteral {
		return fmt.Errorf("cannot add values to literal chunks: %w", errNoAddToLiterals)
	}
	nrc, ok := tc.(*ChunkBools)
	if !ok {
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

	if nrc.IsLiteral {
		value := nrc.data.Get(0)
		for j := 0; j < nrc.Len(); j++ {
			rc.data.Set(int(rc.length)+j, value)
		}
	} else {
		rc.data.Append(nrc.data)
	}
	rc.length += nrc.length

	return nil
}

// Append adds a chunk of the same type at the end of this one (in place update)
func (rc *ChunkDates) Append(tc Chunk) error {
	if rc.IsLiteral {
		return fmt.Errorf("cannot add values to literal chunks: %w", errNoAddToLiterals)
	}
	nrc, ok := tc.(*ChunkDates)
	if !ok {
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

	if nrc.IsLiteral {
		value := nrc.data[0]
		for j := 0; j < nrc.Len(); j++ {
			rc.data = append(rc.data, value)
		}
	} else {
		rc.data = append(rc.data, nrc.data...)
	}
	rc.length += nrc.length

	return nil
}

// Append adds a chunk of the same type at the end of this one (in place update)
func (rc *ChunkDatetimes) Append(tc Chunk) error {
	if rc.IsLiteral {
		return fmt.Errorf("cannot add values to literal chunks: %w", errNoAddToLiterals)
	}
	nrc, ok := tc.(*ChunkDatetimes)
	if !ok {
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

	if nrc.IsLiteral {
		value := nrc.data[0]
		for j := 0; j < nrc.Len(); j++ {
			rc.data = append(rc.data, value)
		}
	} else {
		rc.data = append(rc.data, nrc.data...)
	}
	rc.length += nrc.length

	return nil
}

// Append adds a chunk of the same type at the end of this one (in place update)
func (rc *ChunkNulls) Append(tc Chunk) error {
	nrc, ok := tc.(*ChunkNulls)
	if !ok {
		return errAppendTypeMismatch
	}
	rc.length += nrc.length

	return nil
}

// Prune filter this chunk and only preserves values for which the bitmap is set
func (rc *ChunkStrings) Prune(bm *bitmap.Bitmap) Chunk {
	if rc.IsLiteral {
		// TODO: pruning could be implemented by hydrating this chunk (disabling isLiteral)
		panic("pruning not supported in literal chunks")
	}
	nc := newChunkStrings()
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

	// OPTIM: nthValue is not the fastest, just iterate over offsets directly
	// OR, just iterate over positive bits in our Bitmap - this will be super fast for sparse bitmaps
	// the bitmap iteration could be implemented in all the typed chunks
	index := 0
	for j := 0; j < rc.Len(); j++ {
		if !bm.Get(j) {
			continue
		}
		// be careful here, AddValue has its own nullability logic and we don't want to mess with that
		nc.AddValue(rc.nthValue(j))
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

// Prune filter this chunk and only preserves values for which the bitmap is set
func (rc *ChunkInts) Prune(bm *bitmap.Bitmap) Chunk {
	if rc.IsLiteral {
		panic("pruning not supported in literal chunks")
	}
	nc := newChunkInts()
	if bm == nil {
		return nc
	}
	if bm.Cap() != rc.Len() {
		panic("pruning bitmap does not align with the dataset")
	}

	if bm.Count() == rc.Len() {
		return rc
	}

	index := 0
	for j := 0; j < rc.Len(); j++ {
		if !bm.Get(j) {
			continue
		}
		nc.data = append(nc.data, rc.data[j])
		if rc.Nullability != nil && rc.Nullability.Get(j) {
			if nc.Nullability == nil {
				nc.Nullability = bitmap.NewBitmap(index)
			}
			nc.Nullability.Set(index, true)
		}
		nc.length++
		index++
	}

	// make sure the nullability vector aligns with the data
	if nc.Nullability != nil {
		nc.Nullability.Ensure(nc.Len())
	}

	return nc
}

// Prune filter this chunk and only preserves values for which the bitmap is set
func (rc *ChunkFloats) Prune(bm *bitmap.Bitmap) Chunk {
	if rc.IsLiteral {
		panic("pruning not supported in literal chunks")
	}
	nc := newChunkFloats()
	if bm == nil {
		return nc
	}
	if bm.Cap() != rc.Len() {
		panic("pruning bitmap does not align with the dataset")
	}

	if bm.Count() == rc.Len() {
		return rc
	}

	index := 0
	for j := 0; j < rc.Len(); j++ {
		if !bm.Get(j) {
			continue
		}
		nc.data = append(nc.data, rc.data[j])
		if rc.Nullability != nil && rc.Nullability.Get(j) {
			if nc.Nullability == nil {
				nc.Nullability = bitmap.NewBitmap(index)
			}
			nc.Nullability.Set(index, true)
		}
		nc.length++
		index++
	}

	// make sure the nullability vector aligns with the data
	if nc.Nullability != nil {
		nc.Nullability.Ensure(nc.Len())
	}

	return nc
}

// Prune filter this chunk and only preserves values for which the bitmap is set
func (rc *ChunkBools) Prune(bm *bitmap.Bitmap) Chunk {
	if rc.IsLiteral {
		panic("pruning not supported in literal chunks")
	}
	nc := newChunkBools()
	if bm == nil {
		return nc
	}
	if bm.Cap() != rc.Len() {
		panic("pruning bitmap does not align with the dataset")
	}

	if bm.Count() == rc.Len() {
		return rc
	}

	index := 0
	for j := 0; j < rc.Len(); j++ {
		if !bm.Get(j) {
			continue
		}
		// OPTIM: not need to set false values, we already have them set as zero
		nc.data.Set(index, rc.data.Get(j))
		if rc.Nullability != nil && rc.Nullability.Get(j) {
			if nc.Nullability == nil {
				nc.Nullability = bitmap.NewBitmap(index)
			}
			nc.Nullability.Set(index, true)
		}
		nc.length++
		index++
	}

	// make sure the nullability vector aligns with the data
	if nc.Nullability != nil {
		nc.Nullability.Ensure(nc.Len())
	}

	return nc
}

// Prune filter this chunk and only preserves values for which the bitmap is set
func (rc *ChunkDates) Prune(bm *bitmap.Bitmap) Chunk {
	if rc.IsLiteral {
		panic("pruning not supported in literal chunks")
	}
	nc := newChunkDates()
	if bm == nil {
		return nc
	}
	if bm.Cap() != rc.Len() {
		panic("pruning bitmap does not align with the dataset")
	}

	if bm.Count() == rc.Len() {
		return rc
	}

	index := 0
	for j := 0; j < rc.Len(); j++ {
		if !bm.Get(j) {
			continue
		}
		nc.data = append(nc.data, rc.data[j])
		if rc.Nullability != nil && rc.Nullability.Get(j) {
			if nc.Nullability == nil {
				nc.Nullability = bitmap.NewBitmap(index)
			}
			nc.Nullability.Set(index, true)
		}
		nc.length++
		index++
	}

	// make sure the nullability vector aligns with the data
	if nc.Nullability != nil {
		nc.Nullability.Ensure(nc.Len())
	}

	return nc
}

// Prune filter this chunk and only preserves values for which the bitmap is set
func (rc *ChunkDatetimes) Prune(bm *bitmap.Bitmap) Chunk {
	if rc.IsLiteral {
		panic("pruning not supported in literal chunks")
	}
	nc := newChunkDatetimes()
	if bm == nil {
		return nc
	}
	if bm.Cap() != rc.Len() {
		panic("pruning bitmap does not align with the dataset")
	}

	if bm.Count() == rc.Len() {
		return rc
	}

	index := 0
	for j := 0; j < rc.Len(); j++ {
		if !bm.Get(j) {
			continue
		}
		nc.data = append(nc.data, rc.data[j])
		if rc.Nullability != nil && rc.Nullability.Get(j) {
			if nc.Nullability == nil {
				nc.Nullability = bitmap.NewBitmap(index)
			}
			nc.Nullability.Set(index, true)
		}
		nc.length++
		index++
	}

	// make sure the nullability vector aligns with the data
	if nc.Nullability != nil {
		nc.Nullability.Ensure(nc.Len())
	}

	return nc
}

// Prune filter this chunk and only preserves values for which the bitmap is set
func (rc *ChunkNulls) Prune(bm *bitmap.Bitmap) Chunk {
	nc := newChunkNulls()
	if bm == nil {
		return nc
	}
	if bm.Cap() != rc.Len() {
		panic("pruning bitmap does not align with the dataset")
	}

	if bm.Count() == rc.Len() {
		return rc
	}

	nc.length = uint32(bm.Count())

	return nc
}

// Deserialize reads a chunk from a reader
// this shouldn't really accept a Dtype - at this point we're requiring it, because we don't serialize Dtypes
// into the binary representation - but that's just because we always have the schema at hand... but will we always have it?
// shouldn't the files be readable as standalone files?
// OPTIM: shouldn't we deserialize based on a byte slice instead? We already have it, so we're just duplicating it using a byte buffer
// OPTIM: we may be able to safely cast these byte slice in the future - see https://github.com/golang/go/issues/19367
func Deserialize(r io.Reader, Dtype Dtype) (Chunk, error) {
	switch Dtype {
	case DtypeString:
		return deserializeChunkStrings(r)
	case DtypeInt:
		return deserializeChunkInts(r)
	case DtypeFloat:
		return deserializeChunkFloats(r)
	case DtypeBool:
		return deserializeChunkBools(r)
	case DtypeDatetime:
		return deserializeChunkDatetimes(r)
	case DtypeDate:
		return deserializeChunkDates(r)
	case DtypeNull:
		return deserializeChunkNulls(r)
	}
	panic(fmt.Sprintf("unsupported Dtype: %v", Dtype))
}

func deserializeChunkStrings(r io.Reader) (*ChunkStrings, error) {
	bm, err := bitmap.DeserializeBitmapFromReader(r)
	if err != nil {
		return nil, err
	}
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
	return &ChunkStrings{
		baseChunk: baseChunk{
			Nullability: bm,
			length:      lenOffsets - 1,
		},
		data:    data,
		offsets: offsets,
	}, nil
}

func deserializeChunkInts(r io.Reader) (*ChunkInts, error) {
	bitmap, err := bitmap.DeserializeBitmapFromReader(r)
	if err != nil {
		return nil, err
	}
	var nelements uint32
	if err := binary.Read(r, binary.LittleEndian, &nelements); err != nil {
		return nil, err
	}
	data := make([]int64, nelements)
	if err := binary.Read(r, binary.LittleEndian, &data); err != nil {
		return nil, err
	}
	return &ChunkInts{
		baseChunk: baseChunk{
			Nullability: bitmap,
			length:      nelements,
		},
		data: data,
	}, nil
}

func deserializeChunkFloats(r io.Reader) (*ChunkFloats, error) {
	bitmap, err := bitmap.DeserializeBitmapFromReader(r)
	if err != nil {
		return nil, err
	}
	var nelements uint32
	if err := binary.Read(r, binary.LittleEndian, &nelements); err != nil {
		return nil, err
	}
	data := make([]float64, nelements)
	if err := binary.Read(r, binary.LittleEndian, &data); err != nil {
		return nil, err
	}
	return &ChunkFloats{
		baseChunk: baseChunk{Nullability: bitmap, length: nelements},
		data:      data,
	}, nil
}

func deserializeChunkBools(r io.Reader) (*ChunkBools, error) {
	bm, err := bitmap.DeserializeBitmapFromReader(r)
	if err != nil {
		return nil, err
	}
	var nelements uint32
	if err := binary.Read(r, binary.LittleEndian, &nelements); err != nil {
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
	return &ChunkBools{
		baseChunk: baseChunk{Nullability: bm, length: nelements},
		data:      data,
	}, nil
}

func deserializeChunkDates(r io.Reader) (*ChunkDates, error) {
	bitmap, err := bitmap.DeserializeBitmapFromReader(r)
	if err != nil {
		return nil, err
	}
	var nelements uint32
	if err := binary.Read(r, binary.LittleEndian, &nelements); err != nil {
		return nil, err
	}
	data := make([]date, nelements)
	if err := binary.Read(r, binary.LittleEndian, &data); err != nil {
		return nil, err
	}
	return &ChunkDates{
		baseChunk: baseChunk{Nullability: bitmap, length: nelements},
		data:      data,
	}, nil
}

func deserializeChunkDatetimes(r io.Reader) (*ChunkDatetimes, error) {
	bitmap, err := bitmap.DeserializeBitmapFromReader(r)
	if err != nil {
		return nil, err
	}
	var nelements uint32
	if err := binary.Read(r, binary.LittleEndian, &nelements); err != nil {
		return nil, err
	}
	data := make([]datetime, nelements)
	if err := binary.Read(r, binary.LittleEndian, &data); err != nil {
		return nil, err
	}
	return &ChunkDatetimes{
		baseChunk: baseChunk{Nullability: bitmap, length: nelements},
		data:      data,
	}, nil
}

func deserializeChunkNulls(r io.Reader) (*ChunkNulls, error) {
	var length uint32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return nil, err
	}
	return &ChunkNulls{
		baseChunk: baseChunk{length: length},
	}, nil
}

// WriteTo converts a chunk into its binary representation
func (rc *ChunkStrings) WriteTo(w io.Writer) (int64, error) {
	if rc.IsLiteral {
		return 0, errLiteralsCannotBeSerialised
	}
	nb, err := bitmap.Serialize(w, rc.Nullability)
	if err != nil {
		return 0, err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(rc.offsets))); err != nil {
		return 0, err
	}
	// OPTIM: find the largest offset (the last one) and if it's less than 1<<16, use a smaller uint etc.
	if err := binary.Write(w, binary.LittleEndian, rc.offsets); err != nil {
		return 0, err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(rc.data))); err != nil {
		return 0, err
	}
	bdata, err := w.Write(rc.data)
	if err != nil {
		return 0, err
	}
	if bdata != len(rc.data) {
		return 0, errors.New("not enough data written")
	}
	return int64(nb + 4 + len(rc.offsets)*4 + 4 + len(rc.data)), err
}

// WriteTo converts a chunk into its binary representation
func (rc *ChunkInts) WriteTo(w io.Writer) (int64, error) {
	if rc.IsLiteral {
		return 0, errLiteralsCannotBeSerialised
	}
	nb, err := bitmap.Serialize(w, rc.Nullability)
	if err != nil {
		return 0, err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(rc.data))); err != nil {
		return 0, err
	}
	// OPTIM: find the largest int and possibly use a smaller container than int64
	err = binary.Write(w, binary.LittleEndian, rc.data)
	return int64(nb + 4 + 8*len(rc.data)), err
}

// WriteTo converts a chunk into its binary representation
func (rc *ChunkFloats) WriteTo(w io.Writer) (int64, error) {
	if rc.IsLiteral {
		return 0, errLiteralsCannotBeSerialised
	}
	nb, err := bitmap.Serialize(w, rc.Nullability)
	if err != nil {
		return 0, err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(rc.data))); err != nil {
		return 0, err
	}
	err = binary.Write(w, binary.LittleEndian, rc.data)
	return int64(nb + 4 + 8*len(rc.data)), err
}

// WriteTo converts a chunk into its binary representation
func (rc *ChunkBools) WriteTo(w io.Writer) (int64, error) {
	if rc.IsLiteral {
		return 0, errLiteralsCannotBeSerialised
	}
	nb, err := bitmap.Serialize(w, rc.Nullability)
	if err != nil {
		return 0, err
	}
	// the data bitmap doesn't have a "length", just a capacity (64 aligned), so we
	// need to explicitly write the length of this column chunk
	if err := binary.Write(w, binary.LittleEndian, rc.length); err != nil {
		return 0, err
	}
	nbd, err := bitmap.Serialize(w, rc.data)
	if err != nil {
		return 0, err
	}
	return int64(nb + 4 + nbd), nil
}

// WriteTo converts a chunk into its binary representation
func (rc *ChunkDates) WriteTo(w io.Writer) (int64, error) {
	if rc.IsLiteral {
		return 0, errLiteralsCannotBeSerialised
	}
	nb, err := bitmap.Serialize(w, rc.Nullability)
	if err != nil {
		return 0, err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(rc.data))); err != nil {
		return 0, err
	}
	err = binary.Write(w, binary.LittleEndian, rc.data)
	return int64(nb + 4 + DATE_BYTE_SIZE*len(rc.data)), err
}

// WriteTo converts a chunk into its binary representation
func (rc *ChunkDatetimes) WriteTo(w io.Writer) (int64, error) {
	if rc.IsLiteral {
		return 0, errLiteralsCannotBeSerialised
	}
	nb, err := bitmap.Serialize(w, rc.Nullability)
	if err != nil {
		return 0, err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(rc.data))); err != nil {
		return 0, err
	}
	err = binary.Write(w, binary.LittleEndian, rc.data)
	return int64(nb + 4 + DATETIME_BYTE_SIZE*len(rc.data)), err
}

// WriteTo converts a chunk into its binary representation
func (rc *ChunkNulls) WriteTo(w io.Writer) (int64, error) {
	length := rc.length
	if err := binary.Write(w, binary.LittleEndian, length); err != nil {
		return 0, err
	}
	return int64(4), nil
}

func (rc *ChunkBools) Clone() Chunk {
	var nulls, data *bitmap.Bitmap
	// ARCH: consider bitmap.Clone(bm) to handle nils
	if rc.Nullability != nil {
		nulls = rc.Nullability.Clone()
	}
	if rc.data != nil {
		data = rc.data.Clone()
	}
	return &ChunkBools{
		baseChunk: baseChunk{IsLiteral: rc.IsLiteral, Nullability: nulls, length: rc.length},
		data:      data,
	}
}

func (rc *ChunkFloats) Clone() Chunk {
	var nulls *bitmap.Bitmap
	if rc.Nullability != nil {
		nulls = rc.Nullability.Clone()
	}

	data := append(rc.data[:0:0], rc.data...)

	return &ChunkFloats{
		baseChunk: baseChunk{IsLiteral: rc.IsLiteral, Nullability: nulls, length: rc.length},
		data:      data,
	}
}

func (rc *ChunkInts) Clone() Chunk {
	var nulls *bitmap.Bitmap
	if rc.Nullability != nil {
		nulls = rc.Nullability.Clone()
	}

	data := append(rc.data[:0:0], rc.data...)

	return &ChunkInts{
		baseChunk: baseChunk{IsLiteral: rc.IsLiteral, Nullability: nulls, length: rc.length},
		data:      data,
	}
}

func (rc *ChunkDates) Clone() Chunk {
	var nulls *bitmap.Bitmap
	if rc.Nullability != nil {
		nulls = rc.Nullability.Clone()
	}

	data := append(rc.data[:0:0], rc.data...)

	return &ChunkDates{
		baseChunk: baseChunk{IsLiteral: rc.IsLiteral, Nullability: nulls, length: rc.length},
		data:      data,
	}
}

func (rc *ChunkDatetimes) Clone() Chunk {
	var nulls *bitmap.Bitmap
	if rc.Nullability != nil {
		nulls = rc.Nullability.Clone()
	}

	data := append(rc.data[:0:0], rc.data...)

	return &ChunkDatetimes{
		baseChunk: baseChunk{IsLiteral: rc.IsLiteral, Nullability: nulls, length: rc.length},
		data:      data,
	}
}

func (rc *ChunkNulls) Clone() Chunk {
	return &ChunkNulls{baseChunk: baseChunk{length: rc.length}}
}

func (rc *ChunkStrings) Clone() Chunk {
	var nulls *bitmap.Bitmap
	if rc.Nullability != nil {
		nulls = rc.Nullability.Clone()
	}
	offsets := append(rc.offsets[:0:0], rc.offsets...)
	data := append(rc.data[:0:0], rc.data...)

	return &ChunkStrings{
		baseChunk: baseChunk{IsLiteral: rc.IsLiteral, Nullability: nulls, length: rc.length},
		data:      data,
		offsets:   offsets,
	}
}

func (rc *ChunkStrings) JSONLiteral(n int) (string, bool) {
	if rc.Nullability != nil && rc.Nullability.Get(n) {
		return "", false
	}

	val := rc.nthValue(n)
	ret, err := json.Marshal(val)
	if err != nil {
		panic(err)
	}

	return string(ret), true
}

func (rc *ChunkInts) JSONLiteral(n int) (string, bool) {
	if rc.IsLiteral {
		return fmt.Sprintf("%v", rc.data[0]), true
	}
	if rc.Nullability != nil && rc.Nullability.Get(n) {
		return "", false
	}

	return fmt.Sprintf("%v", rc.data[n]), true
}

func (rc *ChunkFloats) JSONLiteral(n int) (string, bool) {
	if rc.Nullability != nil && rc.Nullability.Get(n) {
		return "", false
	}
	val := rc.data[0]
	if !rc.IsLiteral {
		val = rc.data[n]
	}
	// ARCH: this shouldn't happen? (it used to happen in division by zero... can it happen anywhere else?)
	if math.IsNaN(val) || math.IsInf(val, 0) {
		return "", false
	}

	return fmt.Sprintf("%v", val), true
}

func (rc *ChunkBools) JSONLiteral(n int) (string, bool) {
	if rc.IsLiteral {
		return fmt.Sprintf("%v", rc.data.Get(0)), true
	}
	if rc.Nullability != nil && rc.Nullability.Get(n) {
		return "", false
	}

	return fmt.Sprintf("%v", rc.data.Get(n)), true
}

func (rc *ChunkDates) JSONLiteral(n int) (string, bool) {
	if rc.Nullability != nil && rc.Nullability.Get(n) {
		return "", false
	}
	val := rc.data[0]
	if !rc.IsLiteral {
		val = rc.data[n]
	}
	ret, err := json.Marshal(val)
	if err != nil {
		panic(err)
	}
	return string(ret), true
}

func (rc *ChunkDatetimes) JSONLiteral(n int) (string, bool) {
	if rc.Nullability != nil && rc.Nullability.Get(n) {
		return "", false
	}
	val := rc.data[0]
	if !rc.IsLiteral {
		val = rc.data[n]
	}
	ret, err := json.Marshal(val)
	if err != nil {
		panic(err)
	}
	return string(ret), true
}

func (rc *ChunkNulls) JSONLiteral(n int) (string, bool) {
	return "", false
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

// ARCH: this could be made entirely generic by allowing an interface `nthValue(int) T` to genericise v1/v2
//       EXCEPT for bools :-( (not comparable)
func (rc *ChunkInts) Compare(asc, nullsFirst bool, i, j int) int {
	var n1, n2 bool
	if rc.Nullability != nil {
		n1, n2 = rc.Nullability.Get(i), rc.Nullability.Get(j)
	}
	v1, v2 := rc.data[i], rc.data[j]

	return comparisonFactory(asc, nullsFirst, rc.IsLiteral, rc.Nullability != nil, v1 < v2, v1 == v2, n1, n2)
}

func (rc *ChunkFloats) Compare(asc, nullsFirst bool, i, j int) int {
	var n1, n2 bool
	if rc.Nullability != nil {
		n1, n2 = rc.Nullability.Get(i), rc.Nullability.Get(j)
	}
	// TODO: do we have to worry about inf/nans? I thought we eliminated them from the .data slice
	v1, v2 := rc.data[i], rc.data[j]

	return comparisonFactory(asc, nullsFirst, rc.IsLiteral, rc.Nullability != nil, v1 < v2, v1 == v2, n1, n2)
}

func (rc *ChunkStrings) Compare(asc, nullsFirst bool, i, j int) int {
	var n1, n2 bool
	if rc.Nullability != nil {
		n1, n2 = rc.Nullability.Get(i), rc.Nullability.Get(j)
	}
	v1, v2 := rc.nthValue(i), rc.nthValue(j)

	return comparisonFactory(asc, nullsFirst, rc.IsLiteral, rc.Nullability != nil, v1 < v2, v1 == v2, n1, n2)

}

func (rc *ChunkBools) Compare(asc, nullsFirst bool, i, j int) int {
	var n1, n2 bool
	if rc.Nullability != nil {
		n1, n2 = rc.Nullability.Get(i), rc.Nullability.Get(j)
	}
	v1, v2 := rc.data.Get(i), rc.data.Get(j)
	lt := v1 == false && v2 == true

	return comparisonFactory(asc, nullsFirst, rc.IsLiteral, rc.Nullability != nil, lt, v1 == v2, n1, n2)
}

func (rc *ChunkDates) Compare(asc, nullsFirst bool, i, j int) int {
	var n1, n2 bool
	if rc.Nullability != nil {
		n1, n2 = rc.Nullability.Get(i), rc.Nullability.Get(j)
	}
	v1, v2 := rc.data[i], rc.data[j]

	return comparisonFactory(asc, nullsFirst, rc.IsLiteral, rc.Nullability != nil, v1 < v2, v1 == v2, n1, n2)
}

func (rc *ChunkDatetimes) Compare(asc, nullsFirst bool, i, j int) int {
	var n1, n2 bool
	if rc.Nullability != nil {
		n1, n2 = rc.Nullability.Get(i), rc.Nullability.Get(j)
	}
	v1, v2 := rc.data[i], rc.data[j]

	return comparisonFactory(asc, nullsFirst, rc.IsLiteral, rc.Nullability != nil, v1 < v2, v1 == v2, n1, n2)
}

func (rc *ChunkNulls) Compare(asc, nullsFirst bool, i, j int) int {
	return 0
}
