package main

import (
	"encoding/binary"
	"fmt"
	"github.com/dgraph-io/sroar"
	"testing"
)

func generateSeriesRefs(n, offset int, shouldAppend func(num int) bool) []uint32 {
	series := make([]uint32, 0)
	for i := 1; i <= n; i++ {
		if shouldAppend(i) {
			series = append(series, uint32(i+offset))
		}
	}
	return series
}

func BenchmarkIntersection(b *testing.B) {
	count := 100000

	bigEndianPs := make([]Postings, 100000)
	roaringBitmapPs := make([]*sroar.Bitmap, 100000)

	for i := 0; i < 1000; i++ {
		seriesIds := generateSeriesRefs(count, i, func(num int) bool {
			return num % 2 == 0
		})
		n := len(seriesIds)
		bSlice := make([]byte, n*4)
		for i, n := range seriesIds {
			b := bSlice[i*4 : i*4+4]
			binary.BigEndian.PutUint32(b, n)
		}

		bigEndianPs[i] = newBigEndianPostings(bSlice)
		roaringBitmapPs[i] = newBitmapPostings(seriesIds...)
	}

	countBigEndian := 0
	b.Run("intersect_big_endian", func(b *testing.B) {
		Intersect(bigEndianPs...)
		//for p.Next() {
		//	countBigEndian++
		//}
	})

	countRoaring := 0
	b.Run("intersect_roaring", func(b *testing.B) {
		roaringIntersect(roaringBitmapPs...)
		//countRoaring = p.GetCardinality()
	})

	fmt.Println("big endian", countBigEndian)
	fmt.Println("roaring", countRoaring)
}
