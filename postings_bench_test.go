package main

import (
	"encoding/binary"
	"testing"

	"github.com/dgraph-io/sroar"
	"github.com/stretchr/testify/require"
)

func generateSeriesIds(start, end, incr int) []uint32 {
	if incr < 1 {
		panic("incr must be >= 1")
	}
	series := make([]uint32, 0, int(end-start/incr)+1)
	for i := start; i <= end; i += incr {
		series = append(series, uint32(i))
	}
	return series
}

func getBigEndianPostings(ids []uint32) *bigEndianPostings {
	bSlice1 := make([]byte, len(ids)*4)
	for i, n := range ids {
		b := bSlice1[i*4 : i*4+4]
		binary.BigEndian.PutUint32(b, n)
	}
	return newBigEndianPostings(bSlice1)
}

func BenchmarkIntersection(b *testing.B) {
	increments := []int{2, 3, 4, 6, 8, 10, 12, 14, 16, 18, 20}
	seriesIds := make([][]uint32, len(increments))
	for i, incr := range increments {
		seriesIds[i] = generateSeriesIds(1, 1000, incr)
	}

	// BigEndian postings.
	bigEndianPs := make([]Postings, 0)
	for i := range seriesIds {
		bigEndianPs = append(bigEndianPs, getBigEndianPostings(seriesIds[i]))
	}
	numBigEndian := 0
	b.Run("Intersect_BigEndianPostings", func(b *testing.B) {
		p, err := ExpandPostings(Intersect(bigEndianPs...))
		require.NoError(b, err)
		if numBigEndian == 0 {
			numBigEndian = len(p)
		}
	})

	// Roaring bitmap postings.
	roaringBitmapPs := make([]*sroar.Bitmap, 0)
	for i := range seriesIds {
		roaringBitmapPs = append(roaringBitmapPs, newBitmapPostings(seriesIds[i]...))
	}

	numRoaring := 0
	b.Run("Intersect_RoaringBitmapsPostings", func(b *testing.B) {
		p := roaringIntersect(roaringBitmapPs...)
		numRoaring = p.GetCardinality()
	})

	require.Equal(b, numBigEndian, numRoaring)
}

func BenchmarkUnion(b *testing.B) {
	increments := []int{2, 3, 4, 6, 8, 10}
	seriesIds := make([][]uint32, len(increments))
	for i, incr := range increments {
		seriesIds[i] = generateSeriesIds(1, 100, incr)
	}

	// BigEndian postings.
	bigEndianPs := make([]Postings, 0)
	for i := range seriesIds {
		bigEndianPs = append(bigEndianPs, getBigEndianPostings(seriesIds[i]))
	}
	numBigEndian := 0
	b.Run("Union_BigEndianPostings", func(b *testing.B) {
		p, err := ExpandPostings(Merge(bigEndianPs...))
		require.NoError(b, err)
		if numBigEndian == 0 {
			numBigEndian = len(p)
		}
	})

	// Roaring bitmap postings.
	roaringBitmapPs := make([]*sroar.Bitmap, 0)
	for i := range seriesIds {
		roaringBitmapPs = append(roaringBitmapPs, newBitmapPostings(seriesIds[i]...))
	}

	numRoaring := 0
	b.Run("Union_RoaringBitmapsPostings", func(b *testing.B) {
		p := roaringUnion(roaringBitmapPs...)
		numRoaring = p.GetCardinality()
	})

	require.Equal(b, numBigEndian, numRoaring)
}
