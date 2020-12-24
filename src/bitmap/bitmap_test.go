package bitmap

import (
	"bytes"
	"math/bits"
	"math/rand"
	"reflect"
	"testing"
)

func TestBitmapSetsGets(t *testing.T) {
	vals := []bool{true, false, false, false, true, true, false}
	bm := NewBitmap(0)
	for j, v := range vals {
		bm.Set(j, v)
	}
	for j, v := range vals {
		if bm.Get(j) != v {
			t.Fatalf("position %v: expected %v, got %v", j, v, bm.Get(j))
		}
	}
}

func BenchmarkBitmapSets(b *testing.B) {
	n := 1000
	bm := NewBitmap(n)
	b.ResetTimer()
	for j := 0; j < b.N; j++ {
		bm.Set(n/2, true)
	}
}

func TestBitmapData(t *testing.T) {
	tests := []struct {
		length int
		set    []int
	}{
		{0, nil},
		{1, nil},
		{1, []int{0}},
		{32, []int{12, 14, 16}},
		{64, []int{12, 14, 16}},
		{65, []int{12, 14, 64}},
		{300, []int{12, 14, 200, 245, 244, 299}},
	}
	ones := func(data []uint64) int {
		sum := 0
		for _, el := range data {
			sum += bits.OnesCount64(el)
		}
		return sum
	}
	for _, test := range tests {
		bm := NewBitmap(test.length)
		for _, pos := range test.set {
			bm.Set(pos, true)
		}
		bmc := len(test.set)
		onc := ones(bm.Data())
		if bmc != onc {
			t.Errorf("expecting a bitmap of %v to result in %d ones, got %d", test.set, onc, bmc)
		}
	}
}

func TestBitmapCap(t *testing.T) {
	tests := []struct {
		bm     *Bitmap
		expCap int
	}{
		{NewBitmap(0), 0},
		{NewBitmap(10), 10},
		{NewBitmap(1000), 1000},
	}

	for j, test := range tests {
		if test.bm.Cap() != test.expCap {
			t.Errorf("expecting bitmap %d to have capacity of %d, got %d instead", j, test.expCap, test.bm.Cap())
		}
	}
}

func TestBitmapCapSet(t *testing.T) {
	bm := NewBitmap(0)

	for _, newpos := range []int{10, 64, 65, 100, 128, 1000, 10000} {
		bm.Set(newpos, true)
		if bm.Cap() != newpos+1 {
			t.Errorf("after setting position %d, we'd expect the cap to be the same, but got %d instead", newpos, bm.Cap())
		}
	}
}

func TestBitmapAndOrAlignment(t *testing.T) {
	tests := []struct{ a, b int }{
		{1, 0},
		{0, 1},
		{1000, 0},
		{1000, 1},
		{1, 1000},
		{64, 63},
	}
	for _, test := range tests {
		bm1, bm2 := NewBitmap(test.a), NewBitmap(test.b)
		for _, fnc := range []struct {
			fnc  func(*Bitmap)
			errt string
		}{
			{bm1.AndNot, "cannot &^ two not aligned bitmaps"},
			{bm1.Or, "cannot OR two not aligned bitmaps"},
		} {
			func(bm2 *Bitmap) {
				defer func() {
					if err := recover(); err != fnc.errt {
						t.Fatal(err)
					}
				}()
				fnc.fnc(bm2)
			}(bm2)
		}
	}
}

func TestBitmapAndNot(t *testing.T) {
	bm1, bm2 := NewBitmap(100), NewBitmap(100)
	bm1.Set(12, true)
	bm1.AndNot(bm2) // noop
	if !bm1.Get(12) || bm1.Count() != 1 {
		t.Error("AndNot of a single-bit bitmap with an empty bitmap should do nothing")
	}

	bm2.Set(12, true)
	bm1.AndNot(bm2)
	if bm1.Get(12) || bm1.Count() != 0 {
		t.Error("AndNot of two equivalent bitmaps should reset the first one")
	}
}

func TestBitmapCloning(t *testing.T) {
	var bm1, bm2 *Bitmap
	bm1 = NewBitmap(1000)
	rand.Seed(0)

	for j := 0; j < 100; j++ {
		bm1.Set(rand.Intn(bm1.Cap()), true)
	}
	bm2 = bm1.Clone()
	c2 := bm2.Count()
	for j := 0; j < 100; j++ {
		bm1.Set(rand.Intn(bm1.Cap()), true)
	}
	if bm2.Count() != c2 {
		t.Errorf("expecting a cloned bitmap not to be affected by changes to the original bitmap")
	}
}

func TestBitmapRoundtrip(t *testing.T) {
	bitmaps := []*Bitmap{
		NewBitmapFromBools([]bool{true, false, true, false}),
		// NewBitmap(0), // this now deserialises as nil, so we can't test it like this
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
		_, err := Serialize(bf, b)
		if err != nil {
			t.Error(err)
			return
		}
		br := bytes.NewReader(bf.Bytes())

		b2, err := DeserializeBitmapFromReader(br)
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

func TestOr(t *testing.T) {
	tests := []struct {
		a, b, exp []bool
	}{
		{nil, []bool{true}, []bool{true}},
		{[]bool{true}, nil, []bool{true}},
		{nil, nil, nil},
		{[]bool{true}, []bool{true}, []bool{true}},
		{[]bool{true}, []bool{false}, []bool{true}},
		{[]bool{false}, []bool{true}, []bool{true}},
		{[]bool{false}, []bool{false}, []bool{false}},
		{[]bool{true, false}, []bool{true, false}, []bool{true, false}},
		{[]bool{true, true}, []bool{true, false}, []bool{true, true}},
		{[]bool{false, false}, []bool{false, false}, []bool{false, false}},
		{[]bool{false, false}, []bool{false, true}, []bool{false, true}},
	}

	for _, test := range tests {
		var ba, bb, exp *Bitmap
		if test.a != nil {
			ba = NewBitmapFromBools(test.a)
		}
		if test.b != nil {
			bb = NewBitmapFromBools(test.b)
		}
		if test.exp != nil {
			exp = NewBitmapFromBools(test.exp)
		}

		ored := Or(ba, bb)
		if !reflect.DeepEqual(ored, exp) {
			t.Errorf("expecting %v | %v to result in %v, got %v instead", test.a, test.b, test.exp, ored)
		}
		if ba != nil {
			ba.Or(bb)
			if !reflect.DeepEqual(ba, exp) {
				t.Errorf("expecting %v |= %v to result in %v, got %v instead", test.a, test.b, test.exp, ba)
			}
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
// Or - both receiver and function
