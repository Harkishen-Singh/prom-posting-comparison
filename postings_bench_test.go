package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	h_labels "github.com/Harkishen-Singh/prometheus/model/labels"
	p_labels "github.com/prometheus/prometheus/model/labels"

	h_storage "github.com/Harkishen-Singh/prometheus/storage"
	p_storage "github.com/prometheus/prometheus/storage"

	rb_tsdb "github.com/Harkishen-Singh/prometheus/tsdb"
	rb_chunkenc "github.com/Harkishen-Singh/prometheus/tsdb/chunkenc"
	be_tsdb "github.com/prometheus/prometheus/tsdb"
	be_chunkenc "github.com/prometheus/prometheus/tsdb/chunkenc"
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

//func BenchmarkIntersection(b *testing.B) {
//	increments := []int{2, 3, 4, 6, 8, 10, 12, 14, 16, 18, 20}
//	seriesIds := make([][]uint32, len(increments))
//	for i, incr := range increments {
//		seriesIds[i] = generateSeriesIds(1, 1000, incr)
//	}
//
//	// BigEndian postings.
//	bigEndianPs := make([]Postings, 0)
//	for i := range seriesIds {
//		bigEndianPs = append(bigEndianPs, getBigEndianPostings(seriesIds[i]))
//	}
//	numBigEndian := 0
//	b.Run("Intersect_BigEndianPostings", func(b *testing.B) {
//		p, err := ExpandPostings(Intersect(bigEndianPs...))
//		require.NoError(b, err)
//		if numBigEndian == 0 {
//			numBigEndian = len(p)
//		}
//	})
//
//	// Roaring bitmap postings.
//	seriesIdsUint64 := make([][]uint64, len(seriesIds))
//	for i := range seriesIds {
//		tmp := make([]uint64, 0, len(seriesIds[i]))
//		for j := range seriesIds[i] {
//			tmp = append(tmp, uint64(seriesIds[i][j]))
//		}
//		seriesIdsUint64[i] = tmp
//	}
//	roaringBitmapPs := make([]*sroar.Bitmap, 0)
//	for i := range seriesIds {
//		roaringBitmapPs = append(roaringBitmapPs, newRoarBitmap(seriesIdsUint64[i]...))
//	}
//
//	numRoaring := 0
//	b.Run("Intersect_RoaringBitmapsPostings", func(b *testing.B) {
//		p := roaringIntersect(roaringBitmapPs...)
//		numRoaring = p.GetCardinality()
//	})
//
//	require.Equal(b, numBigEndian, numRoaring)
//}
//
//const be_index_path = "data/be_block_copy/index"
//const rb_index_path = "data/rb_block_copy/01FTR26BGTB9SDS8W56N8HZGGG/index"
//
//func BenchmarkIntersectionRealIndex(b *testing.B) {
//	c_be := 0
//	b.Run("Intersect_big_endian_real_index", func(b *testing.B) {
//		irBE, err := be_index.NewFileReader(be_index_path)
//		require.NoError(b, err)
//		defer irBE.Close()
//
//		bePostings_1, err := irBE.Postings("job", "prometheus")
//		require.NoError(b, err)
//
//		bePostings_2, err := irBE.Postings("job", "promscale")
//		require.NoError(b, err)
//
//		bePostings_3, err := irBE.Postings("job", "demo")
//		require.NoError(b, err)
//
//		bePostings_4, err := irBE.Postings("job", "robust")
//		require.NoError(b, err)
//
//		p := be_index.Intersect(bePostings_1, bePostings_2, bePostings_3, bePostings_4)
//		for p.Next() {
//			c_be++
//		}
//	})
//
//	c_rb := 0
//	b.Run("Intersect_roaring_bitmap_real_index", func(b *testing.B) {
//		irRB, err := rb_index.NewFileReader(rb_index_path)
//		require.NoError(b, err)
//		defer irRB.Close()
//
//		rbPostings_1, err := irRB.Postings("job", "prometheus")
//		require.NoError(b, err)
//
//		rbPostings_2, err := irRB.Postings("job", "promscale")
//		require.NoError(b, err)
//
//		rbPostings_3, err := irRB.Postings("job", "demo")
//		require.NoError(b, err)
//
//		rbPostings_4, err := irRB.Postings("job", "robust")
//		require.NoError(b, err)
//
//		p := rb_index.Intersect(rbPostings_1, rbPostings_2, rbPostings_3, rbPostings_4)
//		for p.Next() {
//			c_rb++
//		}
//	})
//
//	require.Equal(b, c_be, c_rb)
//}
//
//func BenchmarkUnionRealIndex(b *testing.B) {
//	c_be := 0
//	b.Run("Union_big_endian_real_index", func(b *testing.B) {
//		irBE, err := be_index.NewFileReader(be_index_path)
//		require.NoError(b, err)
//		defer irBE.Close()
//
//		bePostings_1, err := irBE.Postings("job", "prometheus")
//		require.NoError(b, err)
//
//		bePostings_2, err := irBE.Postings("job", "promscale")
//		require.NoError(b, err)
//
//		bePostings_3, err := irBE.Postings("job", "demo")
//		require.NoError(b, err)
//
//		bePostings_4, err := irBE.Postings("job", "robust")
//		require.NoError(b, err)
//
//		p := be_index.Merge(bePostings_1, bePostings_2, bePostings_3, bePostings_4)
//		for p.Next() {
//			c_be++
//		}
//	})
//
//	c_rb := 0
//	b.Run("Union_roaring_bitmap_real_index", func(b *testing.B) {
//		irRB, err := rb_index.NewFileReader(rb_index_path)
//		require.NoError(b, err)
//		defer irRB.Close()
//
//		rbPostings_1, err := irRB.Postings("job", "prometheus")
//		require.NoError(b, err)
//
//		rbPostings_2, err := irRB.Postings("job", "promscale")
//		require.NoError(b, err)
//
//		rbPostings_3, err := irRB.Postings("job", "demo")
//		require.NoError(b, err)
//
//		rbPostings_4, err := irRB.Postings("job", "robust")
//		require.NoError(b, err)
//
//		p := rb_index.Merge(rbPostings_1, rbPostings_2, rbPostings_3, rbPostings_4)
//		for p.Next() {
//			c_rb++
//		}
//	})
//
//	require.Equal(b, c_be, c_rb)
//}
//
//func BenchmarkUnion(b *testing.B) {
//	increments := []int{2, 3, 4, 6, 8, 10}
//	seriesIds := make([][]uint32, len(increments))
//	for i, incr := range increments {
//		seriesIds[i] = generateSeriesIds(1, 100, incr)
//	}
//
//	// BigEndian postings.
//	bigEndianPs := make([]Postings, 0)
//	for i := range seriesIds {
//		bigEndianPs = append(bigEndianPs, getBigEndianPostings(seriesIds[i]))
//	}
//	numBigEndian := 0
//	b.Run("Union_BigEndianPostings", func(b *testing.B) {
//		p, err := ExpandPostings(Merge(bigEndianPs...))
//		require.NoError(b, err)
//		if numBigEndian == 0 {
//			numBigEndian = len(p)
//		}
//	})
//
//	// Roaring bitmap postings.
//	seriesIdsUint64 := make([][]uint64, len(seriesIds))
//	for i := range seriesIds {
//		tmp := make([]uint64, 0, len(seriesIds[i]))
//		for j := range seriesIds[i] {
//			tmp = append(tmp, uint64(seriesIds[i][j]))
//		}
//		seriesIdsUint64[i] = tmp
//	}
//	roaringBitmapPs := make([]*sroar.Bitmap, 0)
//	for i := range seriesIds {
//		roaringBitmapPs = append(roaringBitmapPs, newRoarBitmap(seriesIdsUint64[i]...))
//	}
//
//	numRoaring := 0
//	b.Run("Union_RoaringBitmapsPostings", func(b *testing.B) {
//		p := roaringUnion(roaringBitmapPs...)
//		numRoaring = p.GetCardinality()
//	})
//
//	require.Equal(b, numBigEndian, numRoaring)
//}

const be_blockpath = "data/be_block"
const rb_blockpath = "data/rb_block"

func ConvertBigEndianBlockToRoaringBitmapBLock(t *testing.T) {
	block, err := be_tsdb.OpenBlock(log.NewLogfmtLogger(os.Stdout), be_blockpath, be_chunkenc.NewPool())
	querier, err := be_tsdb.NewBlockQuerier(block, 0, 1641945600000)
	require.NoError(t, err)

	matcher := p_labels.MustNewMatcher(p_labels.MatchRegexp, "__name__", ".+")
	seriesSet := querier.Select(false, &p_storage.SelectHints{
		Start: 0,
		End:   1641945600000,
		Step:  1,
	}, matcher)

	blockWriter, err := rb_tsdb.NewBlockWriter(log.NewLogfmtLogger(os.Stdout), rb_blockpath, 100*1024*1024*1024)
	require.NoError(t, err)
	defer blockWriter.Close()

	for seriesSet.Next() {
		serie := seriesSet.At()
		lbls := serie.Labels()
		itr := serie.Iterator()
		seriesAppender := blockWriter.Appender(context.Background())
		cl := convertLabels(lbls)
		for itr.Next() {
			ts, v := itr.At()
			_, err = seriesAppender.Append(0, cl, ts, v)
			require.NoError(t, err)
		}
		require.NoError(t, seriesAppender.Commit())
	}
	ulid, err := blockWriter.Flush(context.Background())
	require.NoError(t, err)
	fmt.Println("roaring bitmap index block ulid", ulid)
}

func convertLabels(p p_labels.Labels) h_labels.Labels {
	hLabels := []h_labels.Label{}
	for i := range p {
		l := h_labels.Label{
			Name: p[i].Name,
			Value: p[i].Value,
		}
		hLabels = append(hLabels, l)
	}
	return hLabels
}

type benchQueries struct {
	id int
	pmatchers []*p_labels.Matcher
	hmatchers []*h_labels.Matcher
}

var queries = []benchQueries{
	{
		id: 1,
		pmatchers: []*p_labels.Matcher{
			p_labels.MustNewMatcher(p_labels.MatchRegexp, "__name__", ".+"),
		},
		hmatchers: []*h_labels.Matcher{
			h_labels.MustNewMatcher(h_labels.MatchRegexp, "__name__", ".+"),
		},
	},
	{
		id: 2,
		pmatchers: []*p_labels.Matcher{
			p_labels.MustNewMatcher(p_labels.MatchEqual, "job", "prometheus"),
			p_labels.MustNewMatcher(p_labels.MatchEqual, "__name__", "go_goroutines"),
		},
		hmatchers: []*h_labels.Matcher{
			h_labels.MustNewMatcher(h_labels.MatchEqual, "job", "prometheus"),
			h_labels.MustNewMatcher(h_labels.MatchEqual, "__name__", "go_goroutines"),
		},
	},
	{
		id: 3,
		pmatchers: []*p_labels.Matcher{
			p_labels.MustNewMatcher(p_labels.MatchEqual, "job", "prometheus"),
			p_labels.MustNewMatcher(p_labels.MatchNotEqual, "__name__", "go_goroutines"),
		},
		hmatchers: []*h_labels.Matcher{
			h_labels.MustNewMatcher(h_labels.MatchEqual, "job", "prometheus"),
			h_labels.MustNewMatcher(h_labels.MatchNotEqual, "__name__", "go_goroutines"),
		},
	},
}

func BenchmarkPromQLQueries(b *testing.B) {
	// BigEndian block.
	be_block_path := "data/be_block_copy"
	be_block, err := be_tsdb.OpenBlock(log.NewLogfmtLogger(os.Stdout), be_block_path, be_chunkenc.NewPool())
	require.NoError(b, err)

	be_querier, err := be_tsdb.NewBlockQuerier(be_block, 0, 1641945600000)
	require.NoError(b, err)

	// Bitmap block.
	rb_block_path := "data/rb_block_copy/01FTR26BGTB9SDS8W56N8HZGGG"
	rb_block, err := rb_tsdb.OpenBlock(log.NewLogfmtLogger(os.Stdout), rb_block_path, rb_chunkenc.NewPool())
	require.NoError(b, err)

	rb_querier, err := rb_tsdb.NewBlockQuerier(rb_block, 0, 1641945600000)
	require.NoError(b, err)

	var be_series_set p_storage.SeriesSet
	var rb_series_set h_storage.SeriesSet

	for i := range queries {
		b.Run("big_endian", func(b *testing.B) {
			be_series_set = be_querier.Select(false, nil, queries[i].pmatchers...)
		})
		b.Run("roaring_bitmap", func(b *testing.B) {
			rb_series_set = rb_querier.Select(false, nil, queries[i].hmatchers...)
		})
	}

	// Verify similar outputs.
	for {
		containsBE := be_series_set.Next()
		containsRB := rb_series_set.Next()
		require.Equal(b, containsBE, containsRB)
		if !containsRB || !containsBE {
			break
		}

		require.NoError(b, be_series_set.Err())
		require.NoError(b, rb_series_set.Err())

		seriesSetBE := be_series_set.At()
		seriesSetRB := rb_series_set.At()

		labelsBE := seriesSetBE.Labels()
		labelsRB := seriesSetRB.Labels()
		require.Equal(b, convertLabels(labelsBE), labelsRB)

		//itrBE := seriesSetBE.Iterator()
		//itrRB := seriesSetRB.Iterator()
		//for {
		//	hasMoreBE := itrBE.Next()
		//	hasMoreRB := itrRB.Next()
		//	require.Equal(b, hasMoreBE, hasMoreRB)
		//
		//	if !hasMoreRB || !hasMoreBE {
		//		break
		//	}
		//
		//	tsBE, vBE := itrBE.At()
		//	tsRB, vRB := itrRB.At()
		//	require.Equal(b, tsBE, tsRB)
		//	if math.IsNaN(vBE) {
		//		require.True(b, math.IsNaN(vRB))
		//		continue
		//	}
		//	require.EqualValues(b, vBE, vRB)
		//}
	}
}
