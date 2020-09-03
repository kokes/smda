package column

// at some point test sum(nullif([1,2,3], 2)) to check we're not interpreting
// "dead" values
// treat this differently, if cs[0] is a literal column
func EvalNullIf(cs ...Chunk) (Chunk, error) {
	eq, err := EvalEq(cs[0], cs[1])
	if err != nil {
		return nil, err
	}
	_ = eq
	panic("not implemented yet (don't have chunk.Clone())") // TODO
	// bm := eq.(*ChunkBools).data
	// if bm.Count() == 0 {
	// 	return cs[0], nil
	// }
	// cb := cs[0].Clone()
	// cb.nullability.Or(bm)
	return nil, nil
}
