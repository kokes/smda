package column

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"

	"github.com/kokes/smda/src/bitmap"
)

var errAppendTypeMismatch = errors.New("cannot append chunks of differing types")
var errAppendNullabilityMismatch = errors.New("when appending, both chunks need to have the same nullability")

// Chunk defines a part of a column - constant type, stored contiguously
type Chunk interface {
	AddValue(string) error
	AddValues([]string) error // just a utility thing, mostly for tests (TODO: unexport this)
	MarshalBinary() ([]byte, error)
	MarshalJSON() ([]byte, error)
	Prune(*bitmap.Bitmap) Chunk
	Append(Chunk) error
	Hash([]uint64)
	Len() int
	Dtype() Dtype
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
	case DtypeNull:
		return newChunkNulls()
	default:
		panic(fmt.Sprintf("unknown schema type: %v", schema.Dtype))
	}
}

// ChunkStrings defines a backing struct for a chunk of string values
type ChunkStrings struct {
	data        []byte
	offsets     []uint32
	nullability *bitmap.Bitmap
	length      uint32
}

// ChunkInts defines a backing struct for a chunk of integer values
type ChunkInts struct {
	data        []int64
	nullability *bitmap.Bitmap
	length      uint32
}

// ChunkFloats defines a backing struct for a chunk of floating point values
type ChunkFloats struct {
	data        []float64
	nullability *bitmap.Bitmap
	length      uint32
}

// ChunkBools defines a backing struct for a chunk of boolean values
type ChunkBools struct {
	data        *bitmap.Bitmap
	nullability *bitmap.Bitmap
	length      uint32
}

// ChunkNulls defines a backing struct for a chunk of null values
// Since it's all nulls, we only need to know how many there are
type ChunkNulls struct {
	length uint32
}

// preallocate column data, so that slice appends don't trigger new reallocations
const defaultChunkCap = 512

func newChunkStrings() *ChunkStrings {
	offsets := make([]uint32, 1, defaultChunkCap)
	offsets[0] = 0
	return &ChunkStrings{
		data:        make([]byte, 0, defaultChunkCap),
		offsets:     offsets,
		nullability: bitmap.NewBitmap(0),
	}
}
func newChunkInts() *ChunkInts {
	return &ChunkInts{
		data:        make([]int64, 0, defaultChunkCap),
		nullability: bitmap.NewBitmap(0),
	}
}
func newChunkFloats() *ChunkFloats {
	return &ChunkFloats{
		data:        make([]float64, 0, defaultChunkCap),
		nullability: bitmap.NewBitmap(0),
	}
}
func newChunkBools() *ChunkBools {
	return &ChunkBools{
		data:        bitmap.NewBitmap(0),
		nullability: bitmap.NewBitmap(0),
	}
}

func newChunkBoolsFromBits(data []uint64, length int) *ChunkBools {
	return &ChunkBools{
		data:   bitmap.NewBitmapFromBits(data, length), // this copies
		length: uint32(length),
	}
}

// Truths returns only true values in this boolean column's bitmap - remove those
// that are null - we use this for filtering, when we're interested in non-null
// true values (to select given rows)
func (rc *ChunkBools) Truths() *bitmap.Bitmap {
	bm := rc.data.Clone()
	if rc.nullability == nil {
		return bm
	}
	// cloning was necessary as AndNot mutates (and we're cloning for good measure - we
	// don't expect to mutate this downstream, but...)
	bm.AndNot(rc.nullability)
	return bm
}

func newChunkNulls() *ChunkNulls {
	return &ChunkNulls{
		length: 0,
	}
}

// Len returns the length of this chunk
func (rc *ChunkBools) Len() int {
	return int(rc.length)
}

// Len returns the length of this chunk
func (rc *ChunkFloats) Len() int {
	return int(rc.length)
}

// Len returns the length of this chunk
func (rc *ChunkInts) Len() int {
	return int(rc.length)
}

// Len returns the length of this chunk
func (rc *ChunkNulls) Len() int {
	return int(rc.length)
}

// Len returns the length of this chunk
func (rc *ChunkStrings) Len() int {
	return int(rc.length)
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

// TODO: does not support nullability, we should probably get rid of the whole thing anyway (only used for testing now)
// BUT, we're sort of using it for type inference - so maybe caveat it with a note that it's only to be used with
// not nullable columns?
func (rc *ChunkStrings) nthValue(n int) string {
	offsetStart := rc.offsets[n]
	offsetEnd := rc.offsets[n+1]
	return string(rc.data[offsetStart:offsetEnd])
}

const nullXorHash = 0xe96766e0d6221951

// Hash hashes this chunk's values into a provded container
// TODO: none of these Hash methods accounts for nulls
// also we don't check that rc.Len() == len(hashes) - should panic otherwise
func (rc *ChunkBools) Hash(hashes []uint64) {
	for j := 0; j < rc.Len(); j++ {
		// xor it with a random big integer - we'll need something similar for bool handling
		// rand.Seed(time.Now().UnixNano())
		// for j := 0; j < 2; j++ {
		// 	val := uint64(rand.Uint32())<<32 + uint64(rand.Uint32())
		// 	fmt.Printf("%x, %v\n", val, bits.OnesCount64(val))
		// }
		if rc.nullability != nil && rc.nullability.Get(j) {
			hashes[j] ^= nullXorHash
			continue
		}
		if rc.data.Get(j) {
			hashes[j] ^= 0x5a320fa8dfcfe3a7
		} else {
			hashes[j] ^= 0x1549571b97ff2995
		}
	}
}

// Hash hashes this chunk's values into a provded container
func (rc *ChunkFloats) Hash(hashes []uint64) {
	var buf [8]byte
	hasher := fnv.New64()
	for j, el := range rc.data {
		if rc.nullability != nil && rc.nullability.Get(j) {
			hashes[j] ^= nullXorHash
			continue
		}
		binary.LittleEndian.PutUint64(buf[:], math.Float64bits(el))
		hasher.Write(buf[:])
		hashes[j] ^= hasher.Sum64()
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
func (rc *ChunkInts) Hash(hashes []uint64) {
	var buf [8]byte
	hasher := fnv.New64()
	for j, el := range rc.data {
		// OPTIM: not just here, in all of these Hash implementations - we might want to check rc.nullability
		// just once and have two separate loops - see if it helps - it may bloat the code too much (and avoid inlining,
		// but that's probably a lost cause already)
		if rc.nullability != nil && rc.nullability.Get(j) {
			hashes[j] ^= nullXorHash
			continue
		}
		binary.LittleEndian.PutUint64(buf[:], uint64(el)) // int64 always maps to a uint64 value (negatives underflow)
		hasher.Write(buf[:])
		// XOR is pretty bad here, but it'll do for now
		// since it's commutative, we'll need something to preserve order - something like
		// an odd multiplier (as a new argument)
		hashes[j] ^= hasher.Sum64()
		hasher.Reset()
	}
}

// Hash hashes this chunk's values into a provded container
func (rc *ChunkNulls) Hash(hashes []uint64) {
	for j := range hashes {
		hashes[j] ^= nullXorHash
	}
}

// Hash hashes this chunk's values into a provded container
func (rc *ChunkStrings) Hash(hashes []uint64) {
	hasher := fnv.New64()
	for j := 0; j < rc.Len(); j++ {
		if rc.nullability != nil && rc.nullability.Get(j) {
			hashes[j] ^= nullXorHash
			continue
		}
		offsetStart := rc.offsets[j]
		offsetEnd := rc.offsets[j+1]
		hasher.Write(rc.data[offsetStart:offsetEnd])
		hashes[j] ^= hasher.Sum64()
		hasher.Reset()
	}
}

// AddValue takes in a string representation of a value and converts it into
// a value suited for this chunk
func (rc *ChunkStrings) AddValue(s string) error {
	rc.data = append(rc.data, []byte(s)...)

	valLen := uint32(len(s))
	valLen += rc.offsets[len(rc.offsets)-1]
	rc.offsets = append(rc.offsets, valLen)

	rc.length++
	if rc.nullability != nil {
		rc.nullability.Ensure(int(rc.length))
	}
	return nil
}

// AddValue takes in a string representation of a value and converts it into
// a value suited for this chunk
func (rc *ChunkInts) AddValue(s string) error {
	if isNull(s) {
		if rc.nullability == nil {
			rc.nullability = bitmap.NewBitmap(rc.Len() + 1)
		}
		rc.nullability.Set(rc.Len(), true)
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
	if rc.nullability != nil {
		rc.nullability.Ensure(int(rc.length))
	}
	return nil
}

// AddValue takes in a string representation of a value and converts it into
// a value suited for this chunk
// let's really consider adding standard nulls here, it will probably make our lives a lot easier
func (rc *ChunkFloats) AddValue(s string) error {
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
	if math.IsNaN(val) {
		if rc.nullability == nil {
			rc.nullability = bitmap.NewBitmap(rc.Len() + 1)
		}
		rc.nullability.Set(rc.Len(), true)
		rc.data = append(rc.data, math.NaN()) // this value is not meant to be read
		rc.length++
		return nil
	}

	rc.data = append(rc.data, val)
	rc.length++
	// make sure the nullability bitmap aligns with the length of the chunk
	if rc.nullability != nil {
		rc.nullability.Ensure(int(rc.length))
	}
	return nil
}

// AddValue takes in a string representation of a value and converts it into
// a value suited for this chunk
func (rc *ChunkBools) AddValue(s string) error {
	if isNull(s) {
		if rc.nullability == nil {
			rc.nullability = bitmap.NewBitmap(rc.Len() + 1)
		}
		rc.nullability.Set(rc.Len(), true)
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
	if rc.nullability != nil {
		rc.nullability.Ensure(int(rc.length))
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
	for _, el := range vals {
		if err := rc.AddValue(el); err != nil {
			return err
		}
	}
	return nil
}

// AddValues is a helper method, it just calls AddValue repeatedly
func (rc *ChunkFloats) AddValues(vals []string) error {
	for _, el := range vals {
		if err := rc.AddValue(el); err != nil {
			return err
		}
	}
	return nil
}

// AddValues is a helper method, it just calls AddValue repeatedly
func (rc *ChunkInts) AddValues(vals []string) error {
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
	for _, el := range vals {
		if err := rc.AddValue(el); err != nil {
			return err
		}
	}
	return nil
}

// Append adds a chunk of the same type at the end of this one (in place update)
func (rc *ChunkStrings) Append(tc Chunk) error {
	nrc, ok := tc.(*ChunkStrings)
	if !ok {
		return errAppendTypeMismatch
	}
	if (rc.nullability == nil && nrc.nullability != nil) || (rc.nullability != nil && nrc.nullability == nil) {
		return errAppendNullabilityMismatch
	}
	if rc.nullability != nil {
		rc.nullability.Append(nrc.nullability)
	}
	rc.data = append(rc.data, nrc.data...)
	rc.length += nrc.length

	off := uint32(0)
	if rc.length > 0 {
		off = rc.offsets[len(rc.offsets)-1]
	}
	for _, el := range nrc.offsets[1:] {
		rc.offsets = append(rc.offsets, el+off)
	}

	return nil
}

// Append adds a chunk of the same type at the end of this one (in place update)
func (rc *ChunkInts) Append(tc Chunk) error {
	nrc, ok := tc.(*ChunkInts)
	if !ok {
		return errAppendTypeMismatch
	}
	if (rc.nullability == nil && nrc.nullability != nil) || (rc.nullability != nil && nrc.nullability == nil) {
		return errAppendNullabilityMismatch
	}
	if rc.nullability != nil {
		rc.nullability.Append(nrc.nullability)
	}

	rc.data = append(rc.data, nrc.data...)
	rc.length += nrc.length

	return nil
}

// Append adds a chunk of the same type at the end of this one (in place update)
func (rc *ChunkFloats) Append(tc Chunk) error {
	nrc, ok := tc.(*ChunkFloats)
	if !ok {
		return errAppendTypeMismatch
	}
	if (rc.nullability == nil && nrc.nullability != nil) || (rc.nullability != nil && nrc.nullability == nil) {
		return errAppendNullabilityMismatch
	}
	if rc.nullability != nil {
		rc.nullability.Append(nrc.nullability)
	}

	rc.data = append(rc.data, nrc.data...)
	rc.length += nrc.length

	return nil
}

// Append adds a chunk of the same type at the end of this one (in place update)
func (rc *ChunkBools) Append(tc Chunk) error {
	nrc, ok := tc.(*ChunkBools)
	if !ok {
		return errAppendTypeMismatch
	}
	if (rc.nullability == nil && nrc.nullability != nil) || (rc.nullability != nil && nrc.nullability == nil) {
		return errAppendNullabilityMismatch
	}
	if rc.nullability != nil {
		rc.nullability.Append(nrc.nullability)
	}

	rc.data.Append(nrc.data)
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
		if rc.nullability != nil && rc.nullability.Get(j) {
			nc.nullability.Set(index, true)
		}
		// nc.length++ // once we remove AddValue, we'll need this
		index++
	}

	// make sure the nullability vector aligns with the data
	if rc.nullability != nil {
		nc.nullability.Ensure(nc.Len())
	}

	return nc
}

// Prune filter this chunk and only preserves values for which the bitmap is set
func (rc *ChunkInts) Prune(bm *bitmap.Bitmap) Chunk {
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
		if rc.nullability != nil && rc.nullability.Get(j) {
			nc.nullability.Set(index, true)
		}
		nc.length++
		index++
	}

	// make sure the nullability vector aligns with the data
	if rc.nullability != nil {
		nc.nullability.Ensure(nc.Len())
	}

	return nc
}

// Prune filter this chunk and only preserves values for which the bitmap is set
func (rc *ChunkFloats) Prune(bm *bitmap.Bitmap) Chunk {
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
		if rc.nullability != nil && rc.nullability.Get(j) {
			nc.nullability.Set(index, true)
		}
		nc.length++
		index++
	}

	// make sure the nullability vector aligns with the data
	if rc.nullability != nil {
		nc.nullability.Ensure(nc.Len())
	}

	return nc
}

// Prune filter this chunk and only preserves values for which the bitmap is set
func (rc *ChunkBools) Prune(bm *bitmap.Bitmap) Chunk {
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
		if rc.nullability != nil && rc.nullability.Get(j) {
			nc.nullability.Set(index, true)
		}
		nc.length++
		index++
	}

	// make sure the nullability vector aligns with the data
	if rc.nullability != nil {
		nc.nullability.Ensure(nc.Len())
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
		data:        data,
		offsets:     offsets,
		nullability: bm,
		length:      lenOffsets - 1,
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
		data:        data,
		nullability: bitmap,
		length:      nelements,
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
		data:        data,
		nullability: bitmap,
		length:      nelements,
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
	return &ChunkBools{
		data:        data,
		nullability: bm,
		length:      nelements,
	}, nil
}

func deserializeChunkNulls(r io.Reader) (*ChunkNulls, error) {
	var length uint32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return nil, err
	}
	return &ChunkNulls{
		length: length,
	}, nil
}

// MarshalBinary converts a chunk into its binary representation
func (rc *ChunkStrings) MarshalBinary() ([]byte, error) {
	w := new(bytes.Buffer)
	_, err := rc.nullability.Serialize(w)
	if err != nil {
		return nil, err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(rc.offsets))); err != nil {
		return nil, err
	}
	// OPTIM: find the largest offset (the last one) and if it's less than 1<<16, use a smaller uint etc.
	if err := binary.Write(w, binary.LittleEndian, rc.offsets); err != nil {
		return nil, err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(rc.data))); err != nil {
		return nil, err
	}
	bdata, err := w.Write(rc.data)
	if err != nil {
		return nil, err
	}
	if bdata != len(rc.data) {
		return nil, errors.New("not enough data written")
	}
	return w.Bytes(), err
}

// MarshalBinary converts a chunk into its binary representation
func (rc *ChunkInts) MarshalBinary() ([]byte, error) {
	w := new(bytes.Buffer)
	_, err := rc.nullability.Serialize(w)
	if err != nil {
		return nil, err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(rc.data))); err != nil {
		return nil, err
	}
	// OPTIM: find the largest int and possibly use a smaller container than int64
	err = binary.Write(w, binary.LittleEndian, rc.data)
	return w.Bytes(), err
}

// MarshalBinary converts a chunk into its binary representation
func (rc *ChunkFloats) MarshalBinary() ([]byte, error) {
	w := new(bytes.Buffer)
	_, err := rc.nullability.Serialize(w)
	if err != nil {
		return nil, err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(rc.data))); err != nil {
		return nil, err
	}
	err = binary.Write(w, binary.LittleEndian, rc.data)
	return w.Bytes(), err
}

// MarshalBinary converts a chunk into its binary representation
func (rc *ChunkBools) MarshalBinary() ([]byte, error) {
	w := new(bytes.Buffer)
	_, err := rc.nullability.Serialize(w)
	if err != nil {
		return nil, err
	}
	// the data bitmap doesn't have a "length", just a capacity (64 aligned), so we
	// need to explicitly write the length of this column chunk
	if err := binary.Write(w, binary.LittleEndian, rc.length); err != nil {
		return nil, err
	}
	_, err = rc.data.Serialize(w)
	if err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

// MarshalBinary converts a chunk into its binary representation
func (rc *ChunkNulls) MarshalBinary() ([]byte, error) {
	w := new(bytes.Buffer)
	length := rc.length
	if err := binary.Write(w, binary.LittleEndian, length); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

// MarshalJSON converts a chunk into its JSON representation
func (rc *ChunkStrings) MarshalJSON() ([]byte, error) {
	if !(rc.nullability != nil && rc.nullability.Count() > 0) {
		res := make([]string, 0, int(rc.length))
		for j := 0; j < rc.Len(); j++ {
			res = append(res, rc.nthValue(j))
		}

		return json.Marshal(res)
	}

	dt := make([]*string, 0, rc.length)
	for j := 0; j < rc.Len(); j++ {
		val := rc.nthValue(j)
		dt = append(dt, &val)
	}

	for j := 0; j < int(rc.length); j++ {
		if rc.nullability.Get(j) {
			dt[j] = nil
		}
	}
	return json.Marshal(dt)
}

// MarshalJSON converts a chunk into its JSON representation
func (rc *ChunkInts) MarshalJSON() ([]byte, error) {
	if !(rc.nullability != nil && rc.nullability.Count() > 0) {
		return json.Marshal(rc.data)
	}

	dt := make([]*int64, 0, rc.length)
	for j := range rc.data {
		dt = append(dt, &rc.data[j])
	}

	for j := 0; j < int(rc.length); j++ {
		if rc.nullability.Get(j) {
			dt[j] = nil
		}
	}
	return json.Marshal(dt)
}

// MarshalJSON converts a chunk into its JSON representation
func (rc *ChunkFloats) MarshalJSON() ([]byte, error) {
	// I thought we didn't need a nullability branch here, because while we do use a bitmap for nullables,
	// we also store NaNs in the data themselves, so this should be serialised automatically
	// that's NOT the case, MarshalJSON does not allow NaNs and Infties https://github.com/golang/go/issues/3480
	if !(rc.nullability != nil && rc.nullability.Count() > 0) {
		return json.Marshal(rc.data)
	}

	dt := make([]*float64, 0, rc.length)
	for j := range rc.data {
		dt = append(dt, &rc.data[j])
	}

	for j := 0; j < int(rc.length); j++ {
		if rc.nullability.Get(j) {
			dt[j] = nil
		}
	}
	return json.Marshal(dt)
}

// MarshalJSON converts a chunk into its JSON representation
func (rc *ChunkBools) MarshalJSON() ([]byte, error) {
	if !(rc.nullability != nil && rc.nullability.Count() > 0) {
		dt := make([]bool, 0, rc.Len())
		for j := 0; j < rc.Len(); j++ {
			dt = append(dt, rc.data.Get(j))
		}
		return json.Marshal(dt)
	}

	dt := make([]*bool, 0, rc.Len())
	for j := 0; j < rc.Len(); j++ {
		if rc.nullability.Get(j) {
			dt = append(dt, nil)
			continue
		}
		val := rc.data.Get(j)
		dt = append(dt, &val)
	}

	return json.Marshal(dt)
}

// MarshalJSON converts a chunk into its JSON representation
func (rc *ChunkNulls) MarshalJSON() ([]byte, error) {
	ret := make([]*uint8, rc.length) // how else can we create a [null, null, null, ...] in JSON?
	return json.Marshal(ret)
}
