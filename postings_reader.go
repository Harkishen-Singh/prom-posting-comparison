package main

import (
	"fmt"
	"github.com/prometheus/prometheus/tsdb/index"
	"io"
	"os"
)

const sampleIndexFile = "data/prometheus_block_index"

func read() error {
	f, err := os.Open(sampleIndexFile)
	if err != nil {
		return fmt.Errorf("open sample index file: %w", err)
	}

	buf, err := io.ReadAll(f)
	if err != nil {
		return fmt.Errorf("io read: %w", err)
	}

	bSlice := realByteSlice(buf)

	toc, err := index.NewTOCFromByteSlice(bSlice)
	if err != nil {
		return fmt.Errorf("make toc: %w", err)
	}

	postings := make(map[string][]postingOffset)
	lastKey := []string{}
	lastOff := 0
	valueCount := 0
	if err = index.ReadOffsetTable(bSlice, toc.PostingsTable, func(key []string, _ uint64, off int) error {
		if len(key) != 2 {
			return fmt.Errorf("unexpected key length for posting table %d", len(key))
		}
		if _, ok := postings[key[0]]; !ok {
			// Next label name.
			postings[key[0]] = []postingOffset{}
			if lastKey != nil {
				// Always include last value for each label name.
				postings[lastKey[0]] = append(postings[lastKey[0]], postingOffset{value: lastKey[1], off: lastOff})
			}
			lastKey = nil
			valueCount = 0
		}
		if valueCount%symbolFactor == 0 {
			postings[key[0]] = append(postings[key[0]], postingOffset{value: key[1], off: off})
			lastKey = nil
		} else {
			lastKey = key
			lastOff = off
		}
		valueCount++
		return nil
	}); err != nil {
		return fmt.Errorf("reading postings table: %w", err)
	}
	if lastKey != nil {
		postings[lastKey[0]] = append(postings[lastKey[0]], postingOffset{value: lastKey[1], off: lastOff})
	}
	// Trim any extra space in the slices.
	for k, v := range postings {
		l := make([]postingOffset, len(v))
		copy(l, v)
		postings[k] = l
	}

	postingSlice := make([]int, 0)
	for _, v := range postings {
		for i := range v {
			postingSlice = append(postingSlice, v[i].off)
		}
	}

	fmt.Println(len(postingSlice))
	fmt.Println(postingSlice)

	return nil
}
