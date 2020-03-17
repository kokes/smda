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

// func NewBitmap(n int) *bitmap {
// func NewBitmapFromBools(data []bool) *bitmap {
// func (bm *Bitmap) Count() int {
// func (b *bitmap) set(n int, val bool) {
// func (b *bitmap) get(n int) bool {
// func (b *bitmap) serialize(w io.Writer) (int, error) {
// func deserialiseBitmapFromReader(r io.Reader) (*bitmap, error) {
