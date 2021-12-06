package main

import "github.com/dgraph-io/sroar"

type bitmapPostings []*sroar.Bitmap

// For now, test uint32. After success, test uint64.
func newBitmapPostings(seriesRef ...uint32) *sroar.Bitmap {
	bitmap := sroar.NewBitmap()
	for i := range seriesRef {
		bitmap.Set(uint64(seriesRef[i]))
	}
	return bitmap
}

func roaringIntersect(p ...*sroar.Bitmap) *sroar.Bitmap {
	prev := sroar.FastAnd(p[0], p[1])
	for i := 2; i < len(p); i++ {
		prev = sroar.FastAnd(prev, p[i])
	}
	return prev
}
