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

func (bm *Bitmap) Append(obm *Bitmap) {
	cap := bm.cap
	for j := 0; j < obm.cap; j++ {
		bm.Set(cap+j, obm.Get(j))
	}
}

func (bm *Bitmap) Clone() *Bitmap {
	data := make([]uint64, len(bm.data))
	copy(data, bm.data)
	return &Bitmap{
		data: data,
		cap:  bm.cap,
	}
}

func (bm *Bitmap) AndNot(obm *Bitmap) {
	if bm.cap != obm.cap {
		panic("cannot &^ two not aligned bitmaps")
	}

	for j, el := range obm.data {
		bm.data[j] &= ^el
	}
}

func (bm *Bitmap) Ensure(n int) {
	if bm.data != nil && n <= bm.cap {
		return
	}
	if n > bm.cap {
		bm.cap = n
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

// NewBitmapFromBits leverages a pre-existing bitmap (usually from a file or a reader) and copies
// it into a new bitmap
func NewBitmapFromBits(data []uint64) *Bitmap {
	bm := NewBitmap(64 * len(data))
	copy(bm.data, data)
	return bm
}

// OPTIM: cost is 84, can be almost inlined
func (bm *Bitmap) Set(n int, val bool) {
	bm.Ensure(n + 1)
	if val {
		bm.data[n/64] |= uint64(1 << (n % 64))
	} else {
		bm.data[n/64] &= ^uint64(1 << (n % 64))
	}
}

func (bm *Bitmap) Get(n int) bool {
	// OPTIM: this will always escape to the heap, so it's an inlining blocker
	//        maybe make sure the bitmap is long enough some other place and leave this to be just a return
	bm.Ensure(n + 1) // to avoid panics?
	return (bm.data[n/64] & uint64(1<<(n%64))) > 0
}

func (bm *Bitmap) Invert() {
	for j, el := range bm.data {
		bm.data[j] = ^el
	}
	// not sure about the bm.cap > 0 - what if we invert an empty bitmap?
	if bm.cap > 0 && bm.cap%64 != 0 {
		bm.data[len(bm.data)-1] &= (1 << (bm.cap % 64)) - 1
	}
}

func (bm *Bitmap) Serialize(w io.Writer) (int, error) {
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

func DeserializeBitmapFromReader(r io.Reader) (*Bitmap, error) {
	var cap uint32
	if err := binary.Read(r, binary.LittleEndian, &cap); err != nil {
		return nil, err
	}
	var nelements uint32
	if err := binary.Read(r, binary.LittleEndian, &nelements); err != nil {
		return nil, err
	}
	data := make([]uint64, int(nelements))
	if err := binary.Read(r, binary.LittleEndian, &data); err != nil {
		return nil, err
	}
	bitmap := NewBitmapFromBits(data)
	bitmap.cap = int(cap)
	return bitmap, nil
}
