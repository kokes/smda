package smda

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
)

// Bitmap holds a series of boolean values, at this point it's a plain []bool, but we plan
// to migrate it to a more compact format - []byte or []uint64 with each value taking one bit
type Bitmap struct {
	data []bool // to be a []byte or []uint64
}

// NewBitmap allocates a bitmap to hold at least n values
func NewBitmap(n int) *Bitmap {
	return &Bitmap{
		data: make([]bool, 0, n),
	}
}

// NewBitmapFromBools initialises a bitmap from a pre-existing bool slice
func NewBitmapFromBools(data []bool) *Bitmap {
	return &Bitmap{
		data: data,
	}
}

func (b *Bitmap) set(n int, val bool) {
	if len(b.data) <= n {
		newvals := make([]bool, n-len(b.data)+1)
		b.data = append(b.data, newvals...)
	}
	b.data[n] = val
}

func (b *Bitmap) get(n int) bool {
	if n >= len(b.data) {
		// panic("out of range")
		// we have sparse bitmaps, so this is our way to signify this is valid, but out of range?
		// TODO: perhaps we should keep a "length" property on the bitmap, which would get potentially modified with every set?
		return false
	}
	return b.data[n]
}

func (b *Bitmap) serialize(w io.Writer) (int, error) {
	nbytes := uint32(len(b.data))
	if err := binary.Write(w, binary.LittleEndian, nbytes); err != nil {
		return 0, err
	}
	return 4 + int(nbytes), binary.Write(w, binary.LittleEndian, b.data)
}

func deserialiseBitmapFromReader(r io.Reader) (*Bitmap, error) {
	var nbytes uint32
	if err := binary.Read(r, binary.LittleEndian, &nbytes); err != nil {
		return nil, err
	}
	data := make([]bool, int(nbytes))
	if err := binary.Read(r, binary.LittleEndian, &data); err != nil {
		return nil, err
	}
	bitmap := NewBitmapFromBools(data)
	return bitmap, nil
}

// at one point I debated whether or not we should have a `data interface{}` in the storage struct or something
// along the lines of `dataInts []int64, dataFloats []float64` etc. and we'd pick one in a closure
// upon reading the schema - this would save us type assertions (and give us some perf, potentially),
// but without measuring this, I'm holding back for now
// ALSO, this interface is a bit misnamed - it's not the whole column, just a given chunk within a stripe
type typedColumn interface {
	addValue(string) error
	serializeInto(io.Writer) (int64, error)
	MarshalJSON() ([]byte, error)
	// TODO: Len? (lookup where we use the columnXyz.length and replace it)
}

func newTypedColumnFromSchema(schema columnSchema) typedColumn {
	switch schema.Dtype {
	case dtypeString:
		return newColumnStrings(schema.Nullable)
	case dtypeInt:
		return newColumnInts(schema.Nullable)
	case dtypeFloat:
		return newColumnFloats()
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
	length      int64 // can't we just limit all chunks to be uint32 in length max.? we'd also have to limit MaxRowsPerStripe
}
type columnInts struct {
	data        []int64
	nullable    bool
	nullability *Bitmap
	length      int64
}
type columnFloats struct {
	data []float64
	// no bitmap needed for nullability, can leverage nans in float64
	// buuut, it would be quite consistent, wouldn't it?
	length int64
}
type columnBools struct {
	data        []bool // TODO: bitmap
	nullable    bool
	nullability *Bitmap
	length      int64
}

// if it's all nulls, we only need to know how many there are
type columnNulls struct {
	length int64
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
func newColumnFloats() *columnFloats {
	return &columnFloats{
		data: make([]float64, 0),
	}
}
func newColumnBools(isNullable bool) *columnBools {
	return &columnBools{
		data:        make([]bool, 0),
		nullable:    isNullable,
		nullability: NewBitmap(0),
	}
}

func newColumnNulls() *columnNulls {
	return &columnNulls{
		length: 0,
	}
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
func (rc *columnStrings) nthValue(n int64) string {
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
		rc.data = append(rc.data, false) // this value is not meant to be read
		rc.length++
		return nil
	}
	val, err := parseBool(s)
	if err != nil {
		return err
	}
	rc.data = append(rc.data, val)
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
	bitmap, err := deserialiseBitmapFromReader(r)
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
		length:      int64(lenOffsets) - 1,
	}, nil
}

// TODO: roundtrip tests (for this and floats and bools)
func deserializeColumnInts(r io.Reader) (*columnInts, error) {
	var nullable bool
	if err := binary.Read(r, binary.LittleEndian, &nullable); err != nil {
		return nil, err
	}
	bitmap, err := deserialiseBitmapFromReader(r)
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
		length:      int64(nelements),
	}, nil
}

func deserializeColumnFloats(r io.Reader) (*columnFloats, error) {
	var nelements uint32
	if err := binary.Read(r, binary.LittleEndian, &nelements); err != nil {
		return nil, err
	}
	data := make([]float64, nelements)
	if err := binary.Read(r, binary.LittleEndian, &data); err != nil {
		return nil, err
	}
	return &columnFloats{
		data:   data,
		length: int64(nelements),
	}, nil
}

func deserializeColumnBools(r io.Reader) (*columnBools, error) {
	var nullable bool
	if err := binary.Read(r, binary.LittleEndian, &nullable); err != nil {
		return nil, err
	}
	bitmap, err := deserialiseBitmapFromReader(r)
	if err != nil {
		return nil, err
	}
	var nelements uint32
	if err := binary.Read(r, binary.LittleEndian, &nelements); err != nil {
		return nil, err
	}
	data := make([]bool, nelements)
	if err := binary.Read(r, binary.LittleEndian, &data); err != nil {
		return nil, err
	}
	return &columnBools{
		data:        data,
		nullable:    nullable,
		nullability: bitmap,
		length:      int64(nelements),
	}, nil
}

func deserializeColumnNulls(r io.Reader) (*columnNulls, error) {
	var length uint32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return nil, err
	}
	return &columnNulls{
		length: int64(length),
	}, nil
}

func (rc *columnStrings) serializeInto(w io.Writer) (int64, error) {
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
	bwritten := int64(1 + bnull + 4 + len(rc.offsets)*4 + 4 + bdata)
	return bwritten, err
}

func (rc *columnInts) serializeInto(w io.Writer) (int64, error) {
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
	return 1 + int64(bnull) + 4 + rc.length*8, err
}

func (rc *columnFloats) serializeInto(w io.Writer) (int64, error) {
	if err := binary.Write(w, binary.LittleEndian, uint32(len(rc.data))); err != nil {
		return 0, err
	}
	err := binary.Write(w, binary.LittleEndian, rc.data)
	return 4 + rc.length*8, err
}

func (rc *columnBools) serializeInto(w io.Writer) (int64, error) {
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
	// writing one byte per entry, will change with bitmaps (1-2 bits)
	return 1 + int64(bnull) + 4 + rc.length, err
}

func (rc *columnNulls) serializeInto(w io.Writer) (int64, error) {
	length := uint32(rc.length)
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
		for j := int64(0); j < rc.length; j++ {
			res = append(res, rc.nthValue(j))
		}

		return json.Marshal(res)
	}

	dt := make([]*string, 0, rc.length)
	for j := int64(0); j < rc.length; j++ {
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
	return json.Marshal(rc.data)
}

func (rc *columnBools) MarshalJSON() ([]byte, error) {
	// OPTIM: if nullable, but no nulls in this stripe, use this branch as well
	if !rc.nullable {
		return json.Marshal(rc.data)
	}

	dt := make([]*bool, 0, rc.length)
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

func (rc *columnNulls) MarshalJSON() ([]byte, error) {
	ret := make([]*uint8, rc.length) // how else can we create a [null, null, null, ...] in JSON?
	return json.Marshal(ret)
}
