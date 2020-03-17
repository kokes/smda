package smda

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
)

// Bitmap holds a series of boolean values, efficiently encoded as bits of uint64s
type Bitmap struct {
	data []uint64
}

// Cap denotes how many values can fit in this bitmap
func (bm *Bitmap) cap() int {
	return len(bm.data) * 64
}

func (bm *Bitmap) ensure(n int) {
	if bm.data != nil && n <= bm.cap() {
		return
	}
	nvals := n / 64
	if n%64 != 0 {
		nvals++
	}
	nvals -= len(bm.data)
	bm.data = append(bm.data, make([]uint64, nvals)...)
}

// NewBitmap allocates a bitmap to hold at least n values
func NewBitmap(n int) *Bitmap {
	bm := &Bitmap{}
	bm.ensure(n)
	return bm
}

// NewBitmapFromBools initialises a bitmap from a pre-existing bool slice
func NewBitmapFromBools(data []bool) *Bitmap {
	bm := NewBitmap(len(data))
	for j, el := range data {
		bm.set(j, el)
	}
	return bm
}

// NewBitmapFromBits leverages a pre-existing bitmap (usually from a file or a reader) and copies
// it into a new bitmap
func NewBitmapFromBits(data []uint64) *Bitmap {
	bm := NewBitmap(64 * len(data))
	copy(bm.data, data)
	return bm
}

func (bm *Bitmap) set(n int, val bool) {
	bm.ensure(n + 1)
	if val {
		bm.data[n/64] |= uint64(1 << (n % 64))
	} else {
		bm.data[n/64] &= ^uint64(1 << (n % 64))
	}
}

func (bm *Bitmap) get(n int) bool {
	bm.ensure(n + 1) // to avoid panics?
	return (bm.data[n/64] & uint64(1<<(n%64))) > 0
}

func (bm *Bitmap) serialize(w io.Writer) (int, error) {
	nelements := uint32(len(bm.data))
	if err := binary.Write(w, binary.LittleEndian, nelements); err != nil {
		return 0, err
	}
	return 4 + 8*int(nelements), binary.Write(w, binary.LittleEndian, bm.data)
}

func deserializeBitmapFromReader(r io.Reader) (*Bitmap, error) {
	var nelements uint32
	if err := binary.Read(r, binary.LittleEndian, &nelements); err != nil {
		return nil, err
	}
	data := make([]uint64, int(nelements))
	if err := binary.Read(r, binary.LittleEndian, &data); err != nil {
		return nil, err
	}
	bitmap := NewBitmapFromBits(data)
	return bitmap, nil
}

// at one point I debated whether or not we should have a `data interface{}` in the storage struct or something
// along the lines of `dataInts []int64, dataFloats []float64` etc. and we'd pick one in a closure
// upon reading the schema - this would save us type assertions (and give us some perf, potentially),
// but without measuring this, I'm holding back for now
// ALSO, this interface is a bit misnamed - it's not the whole column, just a given chunk within a stripe
type typedColumn interface {
	addValue(string) error
	serializeInto(io.Writer) (int, error)
	MarshalJSON() ([]byte, error)
	Len() uint32 // I'm not super convinced we should expose (internal) users to uint32, why not just int?
}

func newTypedColumnFromSchema(schema columnSchema) typedColumn {
	switch schema.Dtype {
	case dtypeString:
		return newColumnStrings(schema.Nullable)
	case dtypeInt:
		return newColumnInts(schema.Nullable)
	case dtypeFloat:
		return newColumnFloats(schema.Nullable)
	case dtypeBool:
		return newColumnBools(schema.Nullable)
	case dtypeNull:
		return newColumnNulls()
	default:
		panic(fmt.Sprintf("unknown schema type: %v", schema.Dtype))
	}
}

type columnStrings struct {
	data        []byte
	offsets     []uint32
	nullable    bool
	nullability *Bitmap
	length      uint32
}
type columnInts struct {
	data        []int64
	nullable    bool
	nullability *Bitmap
	length      uint32
}
type columnFloats struct {
	data        []float64
	nullable    bool
	nullability *Bitmap
	length      uint32
}
type columnBools struct {
	data        *Bitmap
	nullable    bool
	nullability *Bitmap
	length      uint32
}

// if it's all nulls, we only need to know how many there are
type columnNulls struct {
	length uint32
}

func newColumnStrings(isNullable bool) *columnStrings {
	return &columnStrings{
		data:        make([]byte, 0),
		offsets:     []uint32{0},
		nullable:    isNullable,
		nullability: NewBitmap(0),
	}
}
func newColumnInts(isNullable bool) *columnInts {
	return &columnInts{
		data:        make([]int64, 0),
		nullable:    isNullable,
		nullability: NewBitmap(0),
	}
}
func newColumnFloats(isNullable bool) *columnFloats {
	return &columnFloats{
		data:        make([]float64, 0),
		nullable:    isNullable,
		nullability: NewBitmap(0),
	}
}
func newColumnBools(isNullable bool) *columnBools {
	return &columnBools{
		data:        NewBitmap(0),
		nullable:    isNullable,
		nullability: NewBitmap(0),
	}
}

func newColumnNulls() *columnNulls {
	return &columnNulls{
		length: 0,
	}
}

func (rc *columnBools) Len() uint32 {
	return rc.length
}
func (rc *columnFloats) Len() uint32 {
	return rc.length
}
func (rc *columnInts) Len() uint32 {
	return rc.length
}
func (rc *columnNulls) Len() uint32 {
	return rc.length
}
func (rc *columnStrings) Len() uint32 {
	return rc.length
}

func (rc *columnStrings) addValue(s string) error {
	rc.data = append(rc.data, []byte(s)...)

	valLen := uint32(len([]byte(s)))
	valLen += rc.offsets[len(rc.offsets)-1]
	rc.offsets = append(rc.offsets, valLen)

	rc.length++
	return nil
}

// TODO: does not support nullability, we should probably get rid of the whole thing anyway (only used for testing now)
// BUT, we're sort of using it for type inference - so maybe caveat it with a note that it's only to be used with
// not nullable columns?
func (rc *columnStrings) nthValue(n uint32) string {
	offsetStart := rc.offsets[n]
	offsetEnd := rc.offsets[n+1]
	return string(rc.data[offsetStart:offsetEnd])
}

func (rc *columnInts) addValue(s string) error {
	if isNull(s) {
		if !rc.nullable {
			return fmt.Errorf("column not set as nullable, but got \"%v\", which resolved as null", s)
		}
		rc.nullability.set(int(rc.length), true)
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
	return nil
}

// let's really consider adding standard nulls here, it will probably make our lives a lot easier
func (rc *columnFloats) addValue(s string) error {
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
			return fmt.Errorf("column not set as nullable, but got \"%v\", which resolved as null", s)
		}
		rc.nullability.set(int(rc.length), true)
		rc.data = append(rc.data, math.NaN()) // this value is not meant to be read
		rc.length++
		return nil
	}

	rc.data = append(rc.data, val)
	rc.length++
	return nil
}

func (rc *columnBools) addValue(s string) error {
	if isNull(s) {
		if !rc.nullable {
			return fmt.Errorf("column not set as nullable, but got \"%v\", which resolved as null", s)
		}
		rc.nullability.set(int(rc.length), true)
		rc.data.set(int(rc.length), false) // this value is not meant to be read
		rc.length++
		return nil
	}
	val, err := parseBool(s)
	if err != nil {
		return err
	}
	rc.data.set(int(rc.length), val)
	rc.length++
	return nil
}

func (rc *columnNulls) addValue(s string) error {
	if !isNull(s) {
		return fmt.Errorf("a null column expects null values, got: %v", s)
	}
	rc.length++
	return nil
}

func deserializeColumn(r io.Reader, dtype dtype) (typedColumn, error) {
	switch dtype {
	case dtypeString:
		return deserializeColumnStrings(r)
	case dtypeInt:
		return deserializeColumnInts(r)
	case dtypeFloat:
		return deserializeColumnFloats(r)
	case dtypeBool:
		return deserializeColumnBools(r)
	case dtypeNull:
		return deserializeColumnNulls(r)
	}
	panic(fmt.Sprintf("unsupported dtype: %v", dtype))
}

func deserializeColumnStrings(r io.Reader) (*columnStrings, error) {
	var nullable bool
	if err := binary.Read(r, binary.LittleEndian, &nullable); err != nil {
		return nil, err
	}
	bitmap, err := deserializeBitmapFromReader(r)
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
	// if we're at the end of a file, reading into an empty byte slice will trigger an EOF :(
	if lenData > 0 {
		if _, err := r.Read(data); err != nil {
			return nil, err
		}
	}
	return &columnStrings{
		data:        data,
		offsets:     offsets,
		nullable:    nullable,
		nullability: bitmap,
		length:      lenOffsets - 1,
	}, nil
}

// TODO: roundtrip tests (for this and floats and bools)
func deserializeColumnInts(r io.Reader) (*columnInts, error) {
	var nullable bool
	if err := binary.Read(r, binary.LittleEndian, &nullable); err != nil {
		return nil, err
	}
	bitmap, err := deserializeBitmapFromReader(r)
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
	return &columnInts{
		data:        data,
		nullable:    nullable,
		nullability: bitmap,
		length:      nelements,
	}, nil
}

func deserializeColumnFloats(r io.Reader) (*columnFloats, error) {
	var nullable bool
	if err := binary.Read(r, binary.LittleEndian, &nullable); err != nil {
		return nil, err
	}
	bitmap, err := deserializeBitmapFromReader(r)
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
	return &columnFloats{
		data:        data,
		nullable:    nullable,
		nullability: bitmap,
		length:      nelements,
	}, nil
}

func deserializeColumnBools(r io.Reader) (*columnBools, error) {
	var nullable bool
	if err := binary.Read(r, binary.LittleEndian, &nullable); err != nil {
		return nil, err
	}
	bitmap, err := deserializeBitmapFromReader(r)
	if err != nil {
		return nil, err
	}
	var nelements uint32
	if err := binary.Read(r, binary.LittleEndian, &nelements); err != nil {
		return nil, err
	}
	data, err := deserializeBitmapFromReader(r)
	if err != nil {
		return nil, err
	}
	return &columnBools{
		data:        data,
		nullable:    nullable,
		nullability: bitmap,
		length:      nelements,
	}, nil
}

func deserializeColumnNulls(r io.Reader) (*columnNulls, error) {
	var length uint32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return nil, err
	}
	return &columnNulls{
		length: length,
	}, nil
}

func (rc *columnStrings) serializeInto(w io.Writer) (int, error) {
	if err := binary.Write(w, binary.LittleEndian, rc.nullable); err != nil {
		return 0, err
	}
	bnull, err := rc.nullability.serialize(w)
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
	bwritten := 1 + bnull + 4 + len(rc.offsets)*4 + 4 + bdata
	return bwritten, err
}

func (rc *columnInts) serializeInto(w io.Writer) (int, error) {
	if err := binary.Write(w, binary.LittleEndian, rc.nullable); err != nil {
		return 0, err
	}
	bnull, err := rc.nullability.serialize(w)
	if err != nil {
		return 0, err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(rc.data))); err != nil {
		return 0, err
	}
	// OPTIM: find the largest int and possibly use a smaller container than int64
	err = binary.Write(w, binary.LittleEndian, rc.data)
	return 1 + bnull + 4 + int(rc.length)*8, err
}

func (rc *columnFloats) serializeInto(w io.Writer) (int, error) {
	if err := binary.Write(w, binary.LittleEndian, rc.nullable); err != nil {
		return 0, err
	}
	bnull, err := rc.nullability.serialize(w)
	if err != nil {
		return 0, err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(rc.data))); err != nil {
		return 0, err
	}
	err = binary.Write(w, binary.LittleEndian, rc.data)
	return 1 + bnull + 4 + int(rc.length)*8, err
}

func (rc *columnBools) serializeInto(w io.Writer) (int, error) {
	if err := binary.Write(w, binary.LittleEndian, rc.nullable); err != nil {
		return 0, err
	}
	bnull, err := rc.nullability.serialize(w)
	if err != nil {
		return 0, err
	}
	// the data bitmap doesn't have a "length", just a capacity (64 aligned), so we
	// need to explicitly write the length of this column chunk
	if err := binary.Write(w, binary.LittleEndian, rc.length); err != nil {
		return 0, err
	}
	bdata, err := rc.data.serialize(w)
	if err != nil {
		return 0, err
	}
	return 1 + bnull + 4 + bdata, err
}

func (rc *columnNulls) serializeInto(w io.Writer) (int, error) {
	length := rc.length
	if err := binary.Write(w, binary.LittleEndian, length); err != nil {
		return 0, err
	}
	return 4, nil
}

// TODO: support nullability
func (rc *columnStrings) MarshalJSON() ([]byte, error) {
	// OPTIM: if nullable, but no nulls in this stripe, use this branch as well
	if !rc.nullable {
		res := make([]string, 0, int(rc.length))
		for j := uint32(0); j < rc.Len(); j++ {
			res = append(res, rc.nthValue(j))
		}

		return json.Marshal(res)
	}

	dt := make([]*string, 0, rc.length)
	for j := uint32(0); j < rc.Len(); j++ {
		val := rc.nthValue(j)
		dt = append(dt, &val)
	}

	for j := 0; j < int(rc.length); j++ {
		if rc.nullability.get(j) {
			dt[j] = nil
		}
	}
	return json.Marshal(dt)
}

func (rc *columnInts) MarshalJSON() ([]byte, error) {
	// OPTIM: if nullable, but no nulls in this stripe, use this branch as well
	if !rc.nullable {
		return json.Marshal(rc.data)
	}

	dt := make([]*int64, 0, rc.length)
	for j := range rc.data {
		dt = append(dt, &rc.data[j])
	}

	for j := 0; j < int(rc.length); j++ {
		if rc.nullability.get(j) {
			dt[j] = nil
		}
	}
	return json.Marshal(dt)
}

func (rc *columnFloats) MarshalJSON() ([]byte, error) {
	// I thought we didn't need a nullability branch here, because while we do use a bitmap for nullables,
	// we also store NaNs in the data themselves, so this should be serialised automatically
	// that's NOT the case, MarshalJSON does not allow NaNs and Infties https://github.com/golang/go/issues/3480
	// OPTIM: if nullable, but no nulls in this stripe, use this branch as well
	if !rc.nullable {
		return json.Marshal(rc.data)
	}

	dt := make([]*float64, 0, rc.length)
	for j := range rc.data {
		dt = append(dt, &rc.data[j])
	}

	for j := 0; j < int(rc.length); j++ {
		if rc.nullability.get(j) {
			dt[j] = nil
		}
	}
	return json.Marshal(dt)
}

func (rc *columnBools) MarshalJSON() ([]byte, error) {
	// OPTIM: if nullable, but no nulls in this stripe, use this branch as well
	if !rc.nullable {
		dt := make([]bool, 0, rc.Len())
		for j := 0; j < int(rc.Len()); j++ {
			dt = append(dt, rc.data.get(j))
		}
		return json.Marshal(dt)
	}

	dt := make([]*bool, 0, rc.Len())
	for j := 0; j < int(rc.Len()); j++ {
		if rc.nullability.get(j) {
			dt = append(dt, nil)
			continue
		}
		val := rc.data.get(j)
		dt = append(dt, &val)
	}

	return json.Marshal(dt)
}

func (rc *columnNulls) MarshalJSON() ([]byte, error) {
	ret := make([]*uint8, rc.length) // how else can we create a [null, null, null, ...] in JSON?
	return json.Marshal(ret)
}
