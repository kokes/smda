package smda

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
)

// at one point I debated whether or not we should have a `data interface{}` in the storage struct or something
// along the lines of `dataInts []int64, dataFloats []float64` etc. and we'd pick one in a closure
// upon reading the schema - this would save us type assertions (and give us some perf, potentially),
// but without measuring this, I'm holding back for now
// ALSO, this interface is a bit misnamed - it's not the whole column, just a given chunk within a stripe
type typedColumn interface {
	addValue(string) error
	serializeInto(io.Writer) (int, error)
	MarshalJSON() ([]byte, error)
	Prune(*Bitmap) typedColumn
	Append(typedColumn) error
	Len() int
	// for now it takes a string expression, it will be parsed beforehand in the future
	// also, we should consider if this should return a new typedColumn with the filtered values already?
	Filter(operator, string) *Bitmap
}

type operator uint8

// make sure to update the stringer and marshaler
const (
	opNone operator = iota
	opIsNull
	opIsNotNull
	opEqual
	opNotEqual
	opGt
	opGte
	opLt
	opLte
	// contains (icontains? for case insensitive?)
	// opIn // make sure we disallow nulls in this list
)

// OPTIM: ...
// support '<>' as opNotEqual?
// and 'is not null' as opIsNotNull?
func (op operator) String() string {
	return []string{"none", "is null", "not null", "=", "!=", ">", ">=", "<", "<="}[op]
}

func (op operator) MarshalJSON() ([]byte, error) {
	return append(append([]byte("\""), []byte(op.String())...), '"'), nil
}

func (op *operator) UnmarshalJSON(data []byte) error {
	if !(len(data) >= 2 && data[0] == '"' && data[len(data)-1] == '"') {
		return errors.New("unexpected string to be unmarshaled into a dtype")
	}
	sdata := string(data[1 : len(data)-1])
	switch sdata {
	case "none":
		*op = opNone
	case "=":
		*op = opEqual
	case "!=":
		*op = opNotEqual
	case "is null":
		*op = opIsNull
	case "not null":
		*op = opIsNotNull
	case ">":
		*op = opGt
	case ">=":
		*op = opGte
	case "<":
		*op = opLt
	case "<=":
		*op = opLte
	default:
		return fmt.Errorf("operator does not exist: %v", sdata)
	}
	return nil
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

func (rc *columnBools) Len() int {
	return int(rc.length)
}
func (rc *columnFloats) Len() int {
	return int(rc.length)
}
func (rc *columnInts) Len() int {
	return int(rc.length)
}
func (rc *columnNulls) Len() int {
	return int(rc.length)
}
func (rc *columnStrings) Len() int {
	return int(rc.length)
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
func (rc *columnStrings) nthValue(n int) string {
	offsetStart := rc.offsets[n]
	offsetEnd := rc.offsets[n+1]
	return string(rc.data[offsetStart:offsetEnd])
}

func (rc *columnInts) addValue(s string) error {
	if isNull(s) {
		if !rc.nullable {
			return fmt.Errorf("column not set as nullable, but got \"%v\", which resolved as null", s)
		}
		rc.nullability.set(rc.Len(), true)
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
		rc.nullability.ensure(int(rc.length))
	}
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
		rc.nullability.set(rc.Len(), true)
		rc.data = append(rc.data, math.NaN()) // this value is not meant to be read
		rc.length++
		return nil
	}

	rc.data = append(rc.data, val)
	rc.length++
	// make sure the nullability bitmap aligns with the length of the chunk
	if rc.nullable {
		rc.nullability.ensure(int(rc.length))
	}
	return nil
}

func (rc *columnBools) addValue(s string) error {
	if isNull(s) {
		if !rc.nullable {
			return fmt.Errorf("column not set as nullable, but got \"%v\", which resolved as null", s)
		}
		rc.nullability.set(rc.Len(), true)
		rc.data.set(rc.Len(), false) // this value is not meant to be read
		rc.length++
		return nil
	}
	val, err := parseBool(s)
	if err != nil {
		return err
	}
	rc.data.set(rc.Len(), val)
	rc.length++
	// make sure the nullability bitmap aligns with the length of the chunk
	if rc.nullable {
		rc.nullability.ensure(int(rc.length))
	}
	return nil
}

func (rc *columnNulls) addValue(s string) error {
	if !isNull(s) {
		return fmt.Errorf("a null column expects null values, got: %v", s)
	}
	rc.length++
	return nil
}

func (rc *columnStrings) Append(tc typedColumn) error {
	nrc, ok := tc.(*columnStrings)
	if !ok {
		return errors.New("cannot append chunks of differing types")
	}
	if rc.nullable != nrc.nullable {
		return errors.New("when appending, both chunks need to have the same nullability") // TODO: fix in all of these appends? Makes little sense
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
func (rc *columnInts) Append(tc typedColumn) error {
	nrc, ok := tc.(*columnInts)
	if !ok {
		return errors.New("cannot append chunks of differing types")
	}
	if rc.nullable != nrc.nullable {
		return errors.New("when appending, both chunks need to have the same nullability") // TODO: fix in all of these appends? Makes little sense
	}
	if rc.nullable {
		rc.nullability.Append(nrc.nullability)
	}

	rc.data = append(rc.data, nrc.data...)
	rc.length += nrc.length

	return nil
}
func (rc *columnFloats) Append(tc typedColumn) error {
	nrc, ok := tc.(*columnFloats)
	if !ok {
		return errors.New("cannot append chunks of differing types")
	}
	if rc.nullable != nrc.nullable {
		return errors.New("when appending, both chunks need to have the same nullability") // TODO: fix in all of these appends? Makes little sense
	}
	if rc.nullable {
		rc.nullability.Append(nrc.nullability)
	}

	rc.data = append(rc.data, nrc.data...)
	rc.length += nrc.length

	return nil
}
func (rc *columnBools) Append(tc typedColumn) error {
	nrc, ok := tc.(*columnBools)
	if !ok {
		return errors.New("cannot append chunks of differing types")
	}
	if rc.nullable != nrc.nullable {
		return errors.New("when appending, both chunks need to have the same nullability") // TODO: fix in all of these appends? Makes little sense
	}
	if rc.nullable {
		rc.nullability.Append(nrc.nullability)
	}

	rc.data.Append(nrc.data)
	rc.length += nrc.length

	return nil
}
func (rc *columnNulls) Append(tc typedColumn) error {
	nrc, ok := tc.(*columnBools)
	if !ok {
		return errors.New("cannot append chunks of differing types")
	}
	rc.length += nrc.length

	return nil
}

func (rc *columnStrings) Prune(bm *Bitmap) typedColumn {
	nc := newColumnStrings(rc.nullable)
	if bm == nil {
		return nc
	}
	if bm.cap != rc.Len() {
		panic("pruning bitmap does not align with the dataset")
	}

	// if we're not pruning anything, we might just return ourselves
	// we don't need to clone anything, since the typedColumn itself is immutable, right?
	if bm.Count() == rc.Len() {
		return rc
	}

	// OPTIM: nthValue is not the fastest, just iterate over offsets directly
	// OR, just iterate over positive bits in our Bitmap - this will be super fast for sparse bitmaps
	// the bitmap iteration could be implemented in all the typed columns
	for j := 0; j < rc.Len(); j++ {
		if !bm.get(j) {
			continue
		}
		// be careful here, addValue has its own nullability logic and we don't want to mess with that
		nc.addValue(rc.nthValue(j))
		if rc.nullable && rc.nullability.get(j) {
			nc.nullability.set(j, true)
		}
		// nc.length++ // once we remove addValue, we'll need this
	}

	return nc
}

func (rc *columnInts) Prune(bm *Bitmap) typedColumn {
	nc := newColumnInts(rc.nullable)
	if bm == nil {
		return nc
	}
	if bm.cap != rc.Len() {
		panic("pruning bitmap does not align with the dataset")
	}

	if bm.Count() == rc.Len() {
		return rc
	}

	for j := 0; j < rc.Len(); j++ {
		if !bm.get(j) {
			continue
		}
		nc.data = append(nc.data, rc.data[j])
		if rc.nullable && rc.nullability.get(j) {
			nc.nullability.set(j, true)
		}
		nc.length++
	}

	return nc
}

func (rc *columnFloats) Prune(bm *Bitmap) typedColumn {
	nc := newColumnFloats(rc.nullable)
	if bm == nil {
		return nc
	}
	if bm.cap != rc.Len() {
		panic("pruning bitmap does not align with the dataset")
	}

	if bm.Count() == rc.Len() {
		return rc
	}

	for j := 0; j < rc.Len(); j++ {
		if !bm.get(j) {
			continue
		}
		nc.data = append(nc.data, rc.data[j])
		if rc.nullable && rc.nullability.get(j) {
			nc.nullability.set(j, true)
		}
		nc.length++
	}

	return nc
}

func (rc *columnBools) Prune(bm *Bitmap) typedColumn {
	nc := newColumnBools(rc.nullable)
	if bm == nil {
		return nc
	}
	if bm.cap != rc.Len() {
		panic("pruning bitmap does not align with the dataset")
	}

	if bm.Count() == rc.Len() {
		return rc
	}

	for j := 0; j < rc.Len(); j++ {
		if !bm.get(j) {
			continue
		}
		// OPTIM: not need to set false values, we already have them set as zero
		nc.data.set(j, rc.data.get(j))
		if rc.nullable && rc.nullability.get(j) {
			nc.nullability.set(j, true)
		}
		nc.length++
	}

	return nc
}

func (rc *columnNulls) Prune(bm *Bitmap) typedColumn {
	nc := newColumnNulls()
	if bm == nil {
		return nc
	}
	if bm.cap != rc.Len() {
		panic("pruning bitmap does not align with the dataset")
	}

	if bm.Count() == rc.Len() {
		return rc
	}

	nc.length = uint32(bm.Count())

	return nc
}

// this shouldn't really accept a dtype - at this point we're requiring it, because we don't serialize dtypes
// into the binary representation - but that's just because we always have the schema at hand... but will we always have it?
// shouldn't the files be readable as standalone files?
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

func (rc *columnStrings) MarshalJSON() ([]byte, error) {
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
		if rc.nullability.get(j) {
			dt[j] = nil
		}
	}
	return json.Marshal(dt)
}

func (rc *columnInts) MarshalJSON() ([]byte, error) {
	if !(rc.nullable && rc.nullability.Count() > 0) {
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
	if !(rc.nullable && rc.nullability.Count() > 0) {
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
	if !(rc.nullable && rc.nullability.Count() > 0) {
		dt := make([]bool, 0, rc.Len())
		for j := 0; j < rc.Len(); j++ {
			dt = append(dt, rc.data.get(j))
		}
		return json.Marshal(dt)
	}

	dt := make([]*bool, 0, rc.Len())
	for j := 0; j < rc.Len(); j++ {
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

func (rc *columnStrings) Filter(op operator, expr string) *Bitmap {
	switch op {
	case opEqual:
		// if we can't match the expression even across value boundaries, there's no way we'll match it properly
		if !bytes.Contains(rc.data, []byte(expr)) {
			return nil
		}
		var bm *Bitmap
		// OPTIM: we don't have to go value by value, we can do the whole bytes.Contains and go from there - find out
		// if the boundaries match an entry etc.
		for j := 0; j < rc.Len(); j++ {
			if rc.nthValue(j) == expr {
				if bm == nil {
					bm = NewBitmap(rc.Len())
				}
				bm.set(int(j), true)
			}
		}

		return bm
	case opNotEqual:
		var bm *Bitmap
		for j := 0; j < rc.Len(); j++ {
			if rc.nthValue(j) != expr {
				if bm == nil {
					bm = NewBitmap(rc.Len())
				}
				bm.set(int(j), true)
			}
		}

		return bm
	default:
		panic(fmt.Sprintf("op not supported: %v", op))
	}
}

func (rc *columnInts) Filter(op operator, expr string) *Bitmap {
	val, err := parseInt(expr)
	if err != nil {
		panic(err)
	}
	switch op {
	case opEqual, opNotEqual, opLt, opLte, opGt, opGte:
		var bm *Bitmap
		var match bool
		for j := 0; j < rc.Len(); j++ {
			match = false
			diff := rc.data[j] - val
			// OPTIM: collapse into one big if?
			if diff == 0 && (op == opEqual || op == opLte || op == opGte) {
				match = true
			} else if diff < 0 && (op == opNotEqual || op == opLt || op == opLte) {
				match = true
			} else if diff > 0 && (op == opNotEqual || op == opGt || op == opGte) {
				match = true
			}

			if match {
				if bm == nil {
					bm = NewBitmap(rc.Len())
				}
				bm.set(j, true)
			}
		}

		return bm
	default:
		panic(fmt.Sprintf("op not supported: %v", op))
	}
}

func (rc *columnFloats) Filter(op operator, expr string) *Bitmap {
	val, err := parseFloat(expr)
	if err != nil {
		panic(err)
	}
	switch op {
	case opEqual, opNotEqual, opLt, opLte, opGt, opGte:
		var bm *Bitmap
		var match bool
		for j := 0; j < rc.Len(); j++ {
			match = false
			diff := rc.data[j] - val
			// OPTIM: collapse into one big if?
			if diff == 0 && (op == opEqual || op == opLte || op == opGte) {
				match = true
			} else if diff < 0 && (op == opNotEqual || op == opLt || op == opLte) {
				match = true
			} else if diff > 0 && (op == opNotEqual || op == opGt || op == opGte) {
				match = true
			}

			if match {
				if bm == nil {
					bm = NewBitmap(rc.Len())
				}
				bm.set(j, true)
			}
		}

		return bm
	default:
		panic(fmt.Sprintf("op not supported: %v", op))
	}
}

func (rc *columnBools) Filter(op operator, expr string) *Bitmap {
	val, err := parseBool(expr)
	if err != nil {
		panic(err)
	}
	switch op {
	case opEqual:
		// OPTIM: if we get zero matches, let's return nil (.Count is fast)
		// if we're looking for true values, we already have them in our bitmap
		if val {
			return rc.data
		}
		// otherwise we just flip all the relevant bits
		bm := rc.data.Clone()
		bm.invert()

		return bm
	case opNotEqual:
		// OPTIM: if we get zero matches, let's return nil (.Count is fast)
		// if we're looking for true values, we already have them in our bitmap
		if !val {
			return rc.data
		}
		// otherwise we just flip all the relevant bits
		bm := rc.data.Clone()
		bm.invert()

		return bm
	default:
		panic(fmt.Sprintf("op not supported %v", op))
	}
}

// OPTIM: we shouldn't need to return a full bitmap - it will always be all ones or all zeroes
// let's not support any ops for now, we don't know what we'll do with this column
func (rc *columnNulls) Filter(op operator, expr string) *Bitmap {
	// switch op {
	// case opEqual:
	// 	bm := NewBitmap(rc.Len())
	// 	if isNull(expr) {
	// 		bm.invert()
	// 	}

	// 	return bm
	// default:
	// 	panic(fmt.Sprintf("op not supported %v", op))
	// }
	panic(fmt.Sprintf("op not supported %v", op))
}
