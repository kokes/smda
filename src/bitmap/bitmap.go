package bitmap

import (
	"encoding/binary"
	"io"
	"math/bits"
)

// Bitmap holds a series of boolean values, efficiently encoded as bits of uint64s
type Bitmap struct {
	data []uint64
	cap  int
}

// Data returns a slice of the underlying bitmap data
func (bm *Bitmap) Data() []uint64 {
	return bm.data
}

// Cap returns the length of this bitmap (because the storage uints can have a higher capacity)
func (bm *Bitmap) Cap() int {
	return bm.cap
}

// Count returns the number of true values in a bitmap
func (bm *Bitmap) Count() int {
	ret := 0
	for _, val := range bm.data {
		ret += bits.OnesCount64(val)
	}
	return ret
}

// KeepFirstN leaves only the first n bits set, resets the rest to zeroes
// does not truncate the underlying storage - the cap is still the same - perhaps we should do this?
// once we hit the n == count condition, we can discard the rest and lower the cap? will require a fair bit
// of testing, but should be doable
func (bm *Bitmap) KeepFirstN(n int) {
	if n < 0 {
		panic("disallowed value")
	}
	if n >= bm.Count() {
		return
	}
	for j, el := range bm.data {
		if n <= 0 {
			bm.data[j] = 0
			continue
		}
		count := bits.OnesCount64(el)
		if count > n {
			for b := 63; b > 0; b-- {
				bm.data[j] &= (1 << b) - 1
				if bits.OnesCount64(bm.data[j]) == n {
					break
				}
			}
		}
		n -= count
	}
}

// Append adds data from an incoming bitmap to this bitmap (in place modification)
func (bm *Bitmap) Append(obm *Bitmap) {
	cap := bm.cap
	for j := 0; j < obm.cap; j++ {
		bm.Set(cap+j, obm.Get(j))
	}
}

// Clone returns a new bitmap with identical content (but a different backing data structure)
func (bm *Bitmap) Clone() *Bitmap {
	data := make([]uint64, len(bm.data))
	copy(data, bm.data)
	return &Bitmap{
		data: data,
		cap:  bm.cap,
	}
}

func Clone(bm *Bitmap) *Bitmap {
	if bm == nil {
		return bm
	}
	return bm.Clone()
}

// AndNot modified this bitmap in place by executing &^ on each element
func (bm *Bitmap) AndNot(obm *Bitmap) {
	if bm.cap != obm.cap {
		panic("cannot &^ two not aligned bitmaps")
	}

	for j, el := range obm.data {
		bm.data[j] &= ^el
	}
}

// Or ors this bitmap with another one (a | b)
func (bm *Bitmap) Or(obm *Bitmap) {
	if obm == nil {
		return
	}
	if bm.cap != obm.cap {
		panic("cannot OR two not aligned bitmaps")
	}

	for j, el := range obm.data {
		bm.data[j] |= el
	}
}

// Or returns a copy (unlike the method)
func Or(bm1 *Bitmap, bm2 *Bitmap) *Bitmap {
	if bm1 == nil && bm2 == nil {
		return nil
	}
	if bm1 == nil {
		return bm2.Clone()
	}
	if bm2 == nil {
		return bm1.Clone()
	}
	bm := bm1.Clone()
	bm.Or(bm2)
	return bm
}

// Ensure makes sure we have at least n capacity in this bitmap (e.g. so that .Get works)
func (bm *Bitmap) Ensure(n int) {
	if bm.data != nil && n <= bm.cap {
		return
	}
	if n > bm.cap {
		bm.cap = n
	}
	nvals := (n + 63) / 64
	nvals -= len(bm.data)
	bm.data = append(bm.data, make([]uint64, nvals)...)
}

// NewBitmap allocates a bitmap to hold at least n values
func NewBitmap(n int) *Bitmap {
	bm := &Bitmap{}
	bm.Ensure(n)
	return bm
}

// NewBitmapFromBools initialises a bitmap from a pre-existing bool slice
func NewBitmapFromBools(data []bool) *Bitmap {
	bm := NewBitmap(len(data))
	for j, el := range data {
		bm.Set(j, el)
	}
	return bm
}

// NewBitmapFromBits leverages a pre-existing bitmap (usually from a file or a reader) and moves
// it into a new bitmap (does NOT copy)
func NewBitmapFromBits(data []uint64, length int) *Bitmap {
	bm := &Bitmap{}
	bm.cap = length
	bm.data = data
	return bm
}

// Set sets nth bit to `val` - true (1) or false (0)
func (bm *Bitmap) Set(n int, val bool) {
	bm.Ensure(n + 1)
	if val {
		bm.data[n/64] |= uint64(1 << (n % 64))
	} else {
		bm.data[n/64] &^= uint64(1 << (n % 64))
	}
}

// Get returns nth bit as a boolean (true for 1, false for 0)
func (bm *Bitmap) Get(n int) bool {
	// OPTIM: this will always escape to the heap, so it's an inlining blocker
	//        maybe make sure the bitmap is long enough some other place and leave this to be just a return
	bm.Ensure(n + 1) // to avoid panics?
	return (bm.data[n/64] & uint64(1<<(n%64))) > 0
}

// Invert flips all the bits in this bitmap
func (bm *Bitmap) Invert() {
	for j, el := range bm.data {
		bm.data[j] = ^el
	}
	// not sure about the bm.cap > 0 - what if we invert an empty bitmap?
	if bm.cap > 0 && bm.cap%64 != 0 {
		bm.data[len(bm.data)-1] &= (1 << (bm.cap % 64)) - 1
	}
}

// Serialize writes this bitmap into a writer, so that it can be deserialised later
func Serialize(w io.Writer, bm *Bitmap) (int, error) {
	if bm == nil {
		if err := binary.Write(w, binary.LittleEndian, uint32(0)); err != nil {
			return 0, err
		}
		return 4, nil
	}
	cap := uint32(bm.cap)
	if err := binary.Write(w, binary.LittleEndian, cap); err != nil {
		return 0, err
	}
	// OPTIM: we could calculate nelements from the cap (integer division and modulo)
	nelements := uint32(len(bm.data))
	if err := binary.Write(w, binary.LittleEndian, nelements); err != nil {
		return 0, err
	}
	return 4 + 4 + 8*int(nelements), binary.Write(w, binary.LittleEndian, bm.data)
}

// DeserializeBitmapFromReader is the inverse of Serialize
func DeserializeBitmapFromReader(r io.Reader) (*Bitmap, error) {
	var cap uint32
	if err := binary.Read(r, binary.LittleEndian, &cap); err != nil {
		return nil, err
	}
	if cap == 0 {
		return nil, nil
	}
	var nelements uint32
	if err := binary.Read(r, binary.LittleEndian, &nelements); err != nil {
		return nil, err
	}
	// OPTIM: we may be able to safely cast these byte slice in the future - see https://github.com/golang/go/issues/19367
	data := make([]uint64, int(nelements))
	if err := binary.Read(r, binary.LittleEndian, &data); err != nil {
		return nil, err
	}
	bitmap := NewBitmapFromBits(data, int(cap))
	return bitmap, nil
}
