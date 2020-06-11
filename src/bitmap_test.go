package smda

import (
	"bytes"
	"reflect"
	"testing"
)

func TestBitmapSetsGets(t *testing.T) {
	vals := []bool{true, false, false, false, true, true, false}
	bm := NewBitmap(0)
	for j, v := range vals {
		bm.set(j, v)
	}
	for j, v := range vals {
		if bm.get(j) != v {
			t.Fatalf("position %v: expected %v, got %v", j, v, bm.get(j))
		}
	}
}

func TestBitmapRoundtrip(t *testing.T) {
	bitmaps := []*Bitmap{
		NewBitmapFromBools([]bool{true, false, true, false}),
		NewBitmap(0),
		NewBitmap(1),
		NewBitmap(9),
		NewBitmap(64),
		NewBitmap(128),
		NewBitmap(129),
		NewBitmap(1000),
		NewBitmap(1000_000),
	}
	for _, b := range bitmaps {
		bf := new(bytes.Buffer)
		_, err := b.serialize(bf)
		if err != nil {
			t.Error(err)
			return
		}
		br := bytes.NewReader(bf.Bytes())

		b2, err := deserializeBitmapFromReader(br)
		if err != nil {
			t.Error(err)
			return
		}
		if !reflect.DeepEqual(b, b2) {
			t.Errorf("expecting %v, got %v", b, b2)
			return
		}
	}
}

// fuzz it perhaps? or at least increase the size of the raw set
func TestKeepingFirstN(t *testing.T) {
	raw := []bool{true, true, false, true, false, true}
	for j := 0; j < NewBitmapFromBools(raw).Count(); j++ {
		bm := NewBitmapFromBools(raw)
		bm.KeepFirstN(j)
		if bm.Count() != j {
			t.Errorf("expecting truncating to %v to keep that many values, got %v", j, bm.Count())
		}
		if len(raw) != bm.cap {
			t.Errorf("not expecting the length of the bitmap to change after KeepFirstN, got %v from %v", bm.cap, len(raw))
		}
	}
	// if we tell it to keep more values then there are, it will just keep them all
	for j := NewBitmapFromBools(raw).Count(); j < NewBitmapFromBools(raw).Count()*2; j++ {
		bm := NewBitmapFromBools(raw)
		bm.KeepFirstN(j)
		if bm.Count() > j {
			t.Errorf("expecting truncating to %v to keep that many values, got %v", j, bm.Count())
		}
		if len(raw) != bm.cap {
			t.Errorf("not expecting the length of the bitmap to change after KeepFirstN, got %v from %v", bm.cap, len(raw))
		}
	}
}

func TestKeepingFirstNBurning(t *testing.T) {
	defer func() {
		if err := recover(); err != "disallowed value" {
			t.Fatal(err)
		}
	}()
	raw := []bool{true, true, false, true, false, true}
	for _, j := range []int{-1, -10, -100} {
		bm := NewBitmapFromBools(raw)
		bm.KeepFirstN(j)
	}
}

func TestBitmapAppending(t *testing.T) {
	tests := []struct {
		a, b, res []bool
	}{
		{[]bool{true, false, true}, []bool{false, true}, []bool{true, false, true, false, true}},
		{[]bool{}, []bool{}, []bool{}},
		{[]bool{true}, []bool{}, []bool{true}},
		{[]bool{}, []bool{true}, []bool{true}},
	}

	for _, test := range tests {
		bm1 := NewBitmapFromBools(test.a)
		bm2 := NewBitmapFromBools(test.b)
		bm3 := NewBitmapFromBools(test.res)

		bm1.Append(bm2)
		if !reflect.DeepEqual(bm1, bm3) {
			t.Errorf("could not concat %v and %v to get %v", test.a, test.b, test.res)
		}
	}
}

// func NewBitmap(n int) *bitmap {
// func NewBitmapFromBools(data []bool) *bitmap {
// func (bm *Bitmap) Count() int {
// func (b *bitmap) set(n int, val bool) {
// func (b *bitmap) get(n int) bool {
// func (b *bitmap) serialize(w io.Writer) (int, error) {
// func deserialiseBitmapFromReader(r io.Reader) (*bitmap, error) {

// invert - testing inverting an empty bitmap
// AndNot
