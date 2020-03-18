package smda

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

// Count returns the number of true values in a bitmap
func (bm *Bitmap) Count() int {
	ret := 0
	for _, val := range bm.data {
		ret += bits.OnesCount64(val)
	}
	return ret
}

func (bm *Bitmap) Clone() *Bitmap {
	data := make([]uint64, len(bm.data))
	copy(data, bm.data)
	return &Bitmap{
		data: data,
		cap:  bm.cap,
	}
}

func (bm *Bitmap) ensure(n int) {
	if n > bm.cap {
		bm.cap = n
	}
	if bm.data != nil && n <= bm.cap {
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

func (bm *Bitmap) invert() {
	for j, el := range bm.data {
		bm.data[j] = ^el
	}
	// not sure about the bm.cap > 0 - what if we invert an empty bitmap?
	if bm.cap > 0 && bm.cap%64 != 0 {
		bm.data[len(bm.data)-1] &= (1 << (bm.cap % 64)) - 1
	}
}

func (bm *Bitmap) serialize(w io.Writer) (int, error) {
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

func deserializeBitmapFromReader(r io.Reader) (*Bitmap, error) {
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
