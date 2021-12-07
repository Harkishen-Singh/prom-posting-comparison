package main

import (
	"github.com/dgraph-io/sroar"
)

type bitmapPostings struct {
	slice []byte
	b     *sroar.Bitmap
}

func newBitmapPostingsFromBSlice(l []byte) *bitmapPostings {
	return &bitmapPostings{
		slice: l,
		b:     sroar.FromBuffer(l),
	}
}

// For now, test uint32. After success, test uint64.
func newRoarBitmap(seriesRef ...uint64) *sroar.Bitmap {
	return sroar.FromSortedList(seriesRef)
}

func roaringIntersect(p ...*sroar.Bitmap) *sroar.Bitmap {
	prev := sroar.FastAnd(p[0], p[1])
	for i := 2; i < len(p); i++ {
		prev = sroar.FastAnd(prev, p[i])
	}
	return prev
}

func roaringUnion(p ...*sroar.Bitmap) *sroar.Bitmap {
	prev := sroar.FastOr(p[0], p[1])
	for i := 2; i < len(p); i++ {
		prev = sroar.FastOr(prev, p[i])
	}
	return prev
}
