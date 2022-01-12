package main

import (
	"encoding/binary"
	"testing"

	rb_index "github.com/Harkishen-Singh/prometheus-index-roaringbitmaps/tsdb/index"
	"github.com/dgraph-io/sroar"
	be_index "github.com/prometheus/prometheus/tsdb/index"
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
	seriesIdsUint64 := make([][]uint64, len(seriesIds))
	for i := range seriesIds {
		tmp := make([]uint64, 0, len(seriesIds[i]))
		for j := range seriesIds[i] {
			tmp = append(tmp, uint64(seriesIds[i][j]))
		}
		seriesIdsUint64[i] = tmp
	}
	roaringBitmapPs := make([]*sroar.Bitmap, 0)
	for i := range seriesIds {
		roaringBitmapPs = append(roaringBitmapPs, newRoarBitmap(seriesIdsUint64[i]...))
	}

	numRoaring := 0
	b.Run("Intersect_RoaringBitmapsPostings", func(b *testing.B) {
		p := roaringIntersect(roaringBitmapPs...)
		numRoaring = p.GetCardinality()
	})

	require.Equal(b, numBigEndian, numRoaring)
}

const be_index_path = "data/index_big_endian"
const rb_index_path = "data/index_roaring_bitmap"

func BenchmarkIntersectionRealIndex(b *testing.B) {
	b.Run("Intersect_big_endian_real_index", func(b *testing.B) {
		irBE, err := be_index.NewFileReader(be_index_path)
		require.NoError(b, err)
		defer irBE.Close()

		bePostings_1, err := irBE.Postings("job", "prometheus")
		require.NoError(b, err)

		bePostings_2, err := irBE.Postings("job", "promscale")
		require.NoError(b, err)

		bePostings_3, err := irBE.Postings("job", "demo")
		require.NoError(b, err)

		bePostings_4, err := irBE.Postings("job", "robust")
		require.NoError(b, err)

		be_index.Intersect(bePostings_1, bePostings_2, bePostings_3, bePostings_4)
	})

	b.Run("Intersect_roaring_bitmap_real_index", func(b *testing.B) {
		irRB, err := rb_index.NewFileReader(rb_index_path)
		require.NoError(b, err)
		defer irRB.Close()

		rbPostings_1, err := irRB.Postings("job", "prometheus")
		require.NoError(b, err)

		rbPostings_2, err := irRB.Postings("job", "promscale")
		require.NoError(b, err)

		rbPostings_3, err := irRB.Postings("job", "demo")
		require.NoError(b, err)

		rbPostings_4, err := irRB.Postings("job", "robust")
		require.NoError(b, err)

		rb_index.Intersect(rbPostings_1, rbPostings_2, rbPostings_3, rbPostings_4)
	})
}

func BenchmarkUnionRealIndex(b *testing.B) {
	b.Run("Union_big_endian_real_index", func(b *testing.B) {
		irBE, err := be_index.NewFileReader(be_index_path)
		require.NoError(b, err)
		defer irBE.Close()

		bePostings_1, err := irBE.Postings("job", "prometheus")
		require.NoError(b, err)

		bePostings_2, err := irBE.Postings("job", "promscale")
		require.NoError(b, err)

		bePostings_3, err := irBE.Postings("job", "demo")
		require.NoError(b, err)

		bePostings_4, err := irBE.Postings("job", "robust")
		require.NoError(b, err)

		be_index.Merge(bePostings_1, bePostings_2, bePostings_3, bePostings_4)
	})

	b.Run("Union_roaring_bitmap_real_index", func(b *testing.B) {
		irRB, err := rb_index.NewFileReader(rb_index_path)
		require.NoError(b, err)
		defer irRB.Close()

		rbPostings_1, err := irRB.Postings("job", "prometheus")
		require.NoError(b, err)

		rbPostings_2, err := irRB.Postings("job", "promscale")
		require.NoError(b, err)

		rbPostings_3, err := irRB.Postings("job", "demo")
		require.NoError(b, err)

		rbPostings_4, err := irRB.Postings("job", "robust")
		require.NoError(b, err)

		rb_index.Merge(rbPostings_1, rbPostings_2, rbPostings_3, rbPostings_4)
	})
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
	seriesIdsUint64 := make([][]uint64, len(seriesIds))
	for i := range seriesIds {
		tmp := make([]uint64, 0, len(seriesIds[i]))
		for j := range seriesIds[i] {
			tmp = append(tmp, uint64(seriesIds[i][j]))
		}
		seriesIdsUint64[i] = tmp
	}
	roaringBitmapPs := make([]*sroar.Bitmap, 0)
	for i := range seriesIds {
		roaringBitmapPs = append(roaringBitmapPs, newRoarBitmap(seriesIdsUint64[i]...))
	}

	numRoaring := 0
	b.Run("Union_RoaringBitmapsPostings", func(b *testing.B) {
		p := roaringUnion(roaringBitmapPs...)
		numRoaring = p.GetCardinality()
	})

	require.Equal(b, numBigEndian, numRoaring)
}
