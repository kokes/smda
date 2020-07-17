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

var errNullInNonNullable = errors.New("cannot add a null value to a non-nullable column")
var errAppendTypeMismatch = errors.New("cannot append chunks of differing types")
var errAppendNullabilityMismatch = errors.New("when appending, both chunks need to have the same nullability")

// at one point I debated whether or not we should have a `data interface{}` in the storage struct or something
// along the lines of `dataInts []int64, dataFloats []float64` etc. and we'd pick one in a closure
// upon reading the schema - this would save us type assertions (and give us some perf, potentially),
// but without measuring this, I'm holding back for now
type Chunk interface {
	AddValue(string) error
	AddValues([]string) error // just a utility thing, mostly for tests
	MarshalBinary() ([]byte, error)
	MarshalJSON() ([]byte, error)
	Prune(*bitmap.Bitmap) Chunk
	Append(Chunk) error
	Hash([]uint64)
	Len() int
	Dtype() Dtype
}

func NewChunkFromSchema(schema Schema) Chunk {
	switch schema.Dtype {
	case DtypeString:
		return newChunkStrings(schema.Nullable)
	case DtypeInt:
		return newChunkInts(schema.Nullable)
	case DtypeFloat:
		return newChunkFloats(schema.Nullable)
	case DtypeBool:
		return newChunkBools(schema.Nullable)
	case DtypeNull:
		return newChunkNulls()
	default:
		panic(fmt.Sprintf("unknown schema type: %v", schema.Dtype))
	}
}

type ChunkStrings struct {
	data        []byte
	offsets     []uint32
	nullable    bool
	nullability *bitmap.Bitmap
	length      uint32
}
type ChunkInts struct {
	data        []int64
	nullable    bool
	nullability *bitmap.Bitmap
	length      uint32
}
type ChunkFloats struct {
	data        []float64
	nullable    bool
	nullability *bitmap.Bitmap
	length      uint32
}
type ChunkBools struct {
	data        *bitmap.Bitmap
	nullable    bool
	nullability *bitmap.Bitmap
	length      uint32
}

// if it's all nulls, we only need to know how many there are
type ChunkNulls struct {
	length uint32
}

// preallocate column data, so that slice appends don't trigger new reallocations
const defaultChunkCap = 512

func newChunkStrings(isNullable bool) *ChunkStrings {
	offsets := make([]uint32, 1, defaultChunkCap)
	offsets[0] = 0
	return &ChunkStrings{
		data:        make([]byte, 0, defaultChunkCap),
		offsets:     offsets,
		nullable:    isNullable,
		nullability: bitmap.NewBitmap(0),
	}
}
func newChunkInts(isNullable bool) *ChunkInts {
	return &ChunkInts{
		data:        make([]int64, 0, defaultChunkCap),
		nullable:    isNullable,
		nullability: bitmap.NewBitmap(0),
	}
}
func newChunkFloats(isNullable bool) *ChunkFloats {
	return &ChunkFloats{
		data:        make([]float64, 0, defaultChunkCap),
		nullable:    isNullable,
		nullability: bitmap.NewBitmap(0),
	}
}
func newChunkBools(isNullable bool) *ChunkBools {
	return &ChunkBools{
		data:        bitmap.NewBitmap(0),
		nullable:    isNullable,
		nullability: bitmap.NewBitmap(0),
	}
}

func newChunkNulls() *ChunkNulls {
	return &ChunkNulls{
		length: 0,
	}
}

func (rc *ChunkBools) Len() int {
	return int(rc.length)
}
func (rc *ChunkFloats) Len() int {
	return int(rc.length)
}
func (rc *ChunkInts) Len() int {
	return int(rc.length)
}
func (rc *ChunkNulls) Len() int {
	return int(rc.length)
}
func (rc *ChunkStrings) Len() int {
	return int(rc.length)
}

func (rc *ChunkBools) Dtype() Dtype {
	return DtypeBool
}
func (rc *ChunkFloats) Dtype() Dtype {
	return DtypeFloat
}
func (rc *ChunkInts) Dtype() Dtype {
	return DtypeInt
}
func (rc *ChunkNulls) Dtype() Dtype {
	return DtypeNull
}
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
		if rc.nullable && rc.nullability.Get(j) {
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
func (rc *ChunkFloats) Hash(hashes []uint64) {
	var buf [8]byte
	hasher := fnv.New64()
	for j, el := range rc.data {
		if rc.nullable && rc.nullability.Get(j) {
			hashes[j] ^= nullXorHash
			continue
		}
		binary.LittleEndian.PutUint64(buf[:], math.Float64bits(el))
		hasher.Write(buf[:])
		hashes[j] ^= hasher.Sum64()
		hasher.Reset()
	}
}

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
		// OPTIM: not just here, in all of these Hash implementations - we might want to check rc.nullable
		// just once and have two separate loops - see if it helps - it may bloat the code too much (and avoid inlining,
		// but that's probably a lost cause already)
		if rc.nullable && rc.nullability.Get(j) {
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

func (rc *ChunkNulls) Hash(hashes []uint64) {
	for j := range hashes {
		hashes[j] ^= nullXorHash
	}
}
func (rc *ChunkStrings) Hash(hashes []uint64) {
	hasher := fnv.New64()
	for j := 0; j < rc.Len(); j++ {
		if rc.nullable && rc.nullability.Get(j) {
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

func (rc *ChunkStrings) AddValue(s string) error {
	rc.data = append(rc.data, []byte(s)...)

	valLen := uint32(len(s))
	valLen += rc.offsets[len(rc.offsets)-1]
	rc.offsets = append(rc.offsets, valLen)

	rc.length++
	return nil
}

func (rc *ChunkInts) AddValue(s string) error {
	if isNull(s) {
		if !rc.nullable {
			return fmt.Errorf("adding %v, which resolves as null: %w", s, errNullInNonNullable)
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
	// make sure the nullability bitmap aligns with the length of the chunk (TODO: test this explicitly)
	if rc.nullable {
		rc.nullability.Ensure(int(rc.length))
	}
	return nil
}

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
		if !rc.nullable {
			return fmt.Errorf("adding %v, which resolves as null: %w", s, errNullInNonNullable)
		}
		rc.nullability.Set(rc.Len(), true)
		rc.data = append(rc.data, math.NaN()) // this value is not meant to be read
		rc.length++
		return nil
	}

	rc.data = append(rc.data, val)
	rc.length++
	// make sure the nullability bitmap aligns with the length of the chunk
	if rc.nullable {
		rc.nullability.Ensure(int(rc.length))
	}
	return nil
}

func (rc *ChunkBools) AddValue(s string) error {
	if isNull(s) {
		if !rc.nullable {
			return fmt.Errorf("adding %v, which resolves as null: %w", s, errNullInNonNullable)
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
	if rc.nullable {
		rc.nullability.Ensure(int(rc.length))
	}
	return nil
}

func (rc *ChunkNulls) AddValue(s string) error {
	if !isNull(s) {
		return fmt.Errorf("a null column expects null values, got: %v", s)
	}
	rc.length++
	return nil
}

func (rc *ChunkBools) AddValues(vals []string) error {
	for _, el := range vals {
		if err := rc.AddValue(el); err != nil {
			return err
		}
	}
	return nil
}
func (rc *ChunkFloats) AddValues(vals []string) error {
	for _, el := range vals {
		if err := rc.AddValue(el); err != nil {
			return err
		}
	}
	return nil
}
func (rc *ChunkInts) AddValues(vals []string) error {
	for _, el := range vals {
		if err := rc.AddValue(el); err != nil {
			return err
		}
	}
	return nil
}
func (rc *ChunkNulls) AddValues(vals []string) error {
	for _, el := range vals {
		if err := rc.AddValue(el); err != nil {
			return err
		}
	}
	return nil
}
func (rc *ChunkStrings) AddValues(vals []string) error {
	for _, el := range vals {
		if err := rc.AddValue(el); err != nil {
			return err
		}
	}
	return nil
}

func (rc *ChunkStrings) Append(tc Chunk) error {
	nrc, ok := tc.(*ChunkStrings)
	if !ok {
		return errAppendTypeMismatch
	}
	if rc.nullable != nrc.nullable {
		return errAppendNullabilityMismatch
	}
	if rc.nullable {
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
func (rc *ChunkInts) Append(tc Chunk) error {
	nrc, ok := tc.(*ChunkInts)
	if !ok {
		return errAppendTypeMismatch
	}
	if rc.nullable != nrc.nullable {
		return errAppendNullabilityMismatch
	}
	if rc.nullable {
		rc.nullability.Append(nrc.nullability)
	}

	rc.data = append(rc.data, nrc.data...)
	rc.length += nrc.length

	return nil
}
func (rc *ChunkFloats) Append(tc Chunk) error {
	nrc, ok := tc.(*ChunkFloats)
	if !ok {
		return errAppendTypeMismatch
	}
	if rc.nullable != nrc.nullable {
		return errAppendNullabilityMismatch
	}
	if rc.nullable {
		rc.nullability.Append(nrc.nullability)
	}

	rc.data = append(rc.data, nrc.data...)
	rc.length += nrc.length

	return nil
}
func (rc *ChunkBools) Append(tc Chunk) error {
	nrc, ok := tc.(*ChunkBools)
	if !ok {
		return errAppendTypeMismatch
	}
	if rc.nullable != nrc.nullable {
		return errAppendNullabilityMismatch
	}
	if rc.nullable {
		rc.nullability.Append(nrc.nullability)
	}

	rc.data.Append(nrc.data)
	rc.length += nrc.length

	return nil
}
func (rc *ChunkNulls) Append(tc Chunk) error {
	nrc, ok := tc.(*ChunkNulls)
	if !ok {
		return errAppendTypeMismatch
	}
	rc.length += nrc.length

	return nil
}

func (rc *ChunkStrings) Prune(bm *bitmap.Bitmap) Chunk {
	nc := newChunkStrings(rc.nullable)
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
		if rc.nullable && rc.nullability.Get(j) {
			nc.nullability.Set(index, true)
		}
		// nc.length++ // once we remove AddValue, we'll need this
		index++
	}

	// make sure the nullability vector aligns with the data
	if rc.nullable {
		nc.nullability.Ensure(nc.Len())
	}

	return nc
}

func (rc *ChunkInts) Prune(bm *bitmap.Bitmap) Chunk {
	nc := newChunkInts(rc.nullable)
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
		if rc.nullable && rc.nullability.Get(j) {
			nc.nullability.Set(index, true)
		}
		nc.length++
		index++
	}

	// make sure the nullability vector aligns with the data
	if rc.nullable {
		nc.nullability.Ensure(nc.Len())
	}

	return nc
}

func (rc *ChunkFloats) Prune(bm *bitmap.Bitmap) Chunk {
	nc := newChunkFloats(rc.nullable)
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
		if rc.nullable && rc.nullability.Get(j) {
			nc.nullability.Set(index, true)
		}
		nc.length++
		index++
	}

	// make sure the nullability vector aligns with the data
	if rc.nullable {
		nc.nullability.Ensure(nc.Len())
	}

	return nc
}

func (rc *ChunkBools) Prune(bm *bitmap.Bitmap) Chunk {
	nc := newChunkBools(rc.nullable)
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
		if rc.nullable && rc.nullability.Get(j) {
			nc.nullability.Set(index, true)
		}
		nc.length++
		index++
	}

	// make sure the nullability vector aligns with the data
	if rc.nullable {
		nc.nullability.Ensure(nc.Len())
	}

	return nc
}

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
	var nullable bool
	if err := binary.Read(r, binary.LittleEndian, &nullable); err != nil {
		return nil, err
	}
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
		nullable:    nullable,
		nullability: bm,
		length:      lenOffsets - 1,
	}, nil
}

// TODO: roundtrip tests (for this and floats and bools)
func deserializeChunkInts(r io.Reader) (*ChunkInts, error) {
	var nullable bool
	if err := binary.Read(r, binary.LittleEndian, &nullable); err != nil {
		return nil, err
	}
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
		nullable:    nullable,
		nullability: bitmap,
		length:      nelements,
	}, nil
}

func deserializeChunkFloats(r io.Reader) (*ChunkFloats, error) {
	var nullable bool
	if err := binary.Read(r, binary.LittleEndian, &nullable); err != nil {
		return nil, err
	}
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
		nullable:    nullable,
		nullability: bitmap,
		length:      nelements,
	}, nil
}

func deserializeChunkBools(r io.Reader) (*ChunkBools, error) {
	var nullable bool
	if err := binary.Read(r, binary.LittleEndian, &nullable); err != nil {
		return nil, err
	}
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
		nullable:    nullable,
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

func (rc *ChunkStrings) MarshalBinary() ([]byte, error) {
	w := new(bytes.Buffer)
	if err := binary.Write(w, binary.LittleEndian, rc.nullable); err != nil {
		return nil, err
	}
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

func (rc *ChunkInts) MarshalBinary() ([]byte, error) {
	w := new(bytes.Buffer)
	if err := binary.Write(w, binary.LittleEndian, rc.nullable); err != nil {
		return nil, err
	}
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

func (rc *ChunkFloats) MarshalBinary() ([]byte, error) {
	w := new(bytes.Buffer)
	if err := binary.Write(w, binary.LittleEndian, rc.nullable); err != nil {
		return nil, err
	}
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

func (rc *ChunkBools) MarshalBinary() ([]byte, error) {
	w := new(bytes.Buffer)
	if err := binary.Write(w, binary.LittleEndian, rc.nullable); err != nil {
		return nil, err
	}
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

func (rc *ChunkNulls) MarshalBinary() ([]byte, error) {
	w := new(bytes.Buffer)
	length := rc.length
	if err := binary.Write(w, binary.LittleEndian, length); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func (rc *ChunkStrings) MarshalJSON() ([]byte, error) {
	if !(rc.nullable && rc.nullability.Count() > 0) {
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

func (rc *ChunkInts) MarshalJSON() ([]byte, error) {
	if !(rc.nullable && rc.nullability.Count() > 0) {
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

func (rc *ChunkFloats) MarshalJSON() ([]byte, error) {
	// I thought we didn't need a nullability branch here, because while we do use a bitmap for nullables,
	// we also store NaNs in the data themselves, so this should be serialised automatically
	// that's NOT the case, MarshalJSON does not allow NaNs and Infties https://github.com/golang/go/issues/3480
	if !(rc.nullable && rc.nullability.Count() > 0) {
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

func (rc *ChunkBools) MarshalJSON() ([]byte, error) {
	if !(rc.nullable && rc.nullability.Count() > 0) {
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

func (rc *ChunkNulls) MarshalJSON() ([]byte, error) {
	ret := make([]*uint8, rc.length) // how else can we create a [null, null, null, ...] in JSON?
	return json.Marshal(ret)
}