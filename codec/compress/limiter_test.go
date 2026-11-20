// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package compress

import (
	"compress/gzip"
	"errors"
	"runtime"
	"testing"
	"testing/synctest"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// boundedCases are the compressors that keep state between calls, and so bound their calls
var boundedCases = []struct {
	name string
	new  func(concurrency int) Compressor
}{
	{"gzip", func(concurrency int) Compressor {
		return NewGzip(gzip.DefaultCompression, concurrency)
	}},
	{"lz4", func(concurrency int) Compressor {
		return NewLZ4(concurrency)
	}},
	{"zstd", func(concurrency int) Compressor {
		return NewZstd(zstd.SpeedDefault, concurrency)
	}},
}

// limiterOf is the compressor's limiter, for a test to read or replace
func limiterOf(t *testing.T, compressor Compressor) *limiter {
	t.Helper()
	switch bounded := compressor.(type) {
	case *gzipCompressor:
		return &bounded.limit
	case *lz4Compressor:
		return &bounded.limit
	case *zstdCompressor:
		return &bounded.limit
	}
	t.Fatalf("%T has no limiter", compressor)
	return nil
}

// TestConcurrencyIsOneTo256And16ByDefault: each bounded compressor runs as many calls at once
// as it is built with, from 1 to 256, and a number outside that stops the program where the
// compressor is built
func TestConcurrencyIsOneTo256And16ByDefault(t *testing.T) {
	if MinimumConcurrency != 1 || MaximumConcurrency != 256 || DefaultConcurrency != 16 {
		t.Fatalf("concurrency is %d to %d, %d by default; want 1 to 256, 16 by default",
			MinimumConcurrency, MaximumConcurrency, DefaultConcurrency)
	}
	plain := compressibleBytes(1024)
	for _, testCase := range boundedCases {
		for _, concurrency := range []int{MinimumConcurrency, DefaultConcurrency,
			MaximumConcurrency} {
			compressor := testCase.new(concurrency)
			if slots := cap(*limiterOf(t, compressor)); slots != concurrency {
				t.Errorf("%s built with concurrency %d has %d slots", testCase.name,
					concurrency, slots)
			}
			roundTrip(t, compressor, plain)
		}
		for _, concurrency := range []int{-1, 0, MaximumConcurrency + 1} {
			recovered := constructionPanic(func() {
				testCase.new(concurrency)
			})
			err, isError := recovered.(error)
			if !isError || !errors.Is(err, ErrConcurrency) {
				t.Errorf("%s with concurrency %d panicked with %v, want ErrConcurrency",
					testCase.name, concurrency, recovered)
			}
		}
	}
}

// TestACallBeyondTheConcurrencyWaitsForASlot: with every slot taken, Compress and Decompress
// each wait, and each runs as soon as a slot is freed
func TestACallBeyondTheConcurrencyWaitsForASlot(t *testing.T) {
	plain := compressibleBytes(1024)
	for _, testCase := range boundedCases {
		compressor := testCase.new(DefaultConcurrency)
		compressed := roundTrip(t, compressor, plain)
		calls := map[string]func() error{
			"Compress": func() error {
				_, err := compressor.Compress(plain)
				return err
			},
			"Decompress": func() error {
				_, err := compressor.Decompress(compressed)
				return err
			},
		}
		for method, call := range calls {
			t.Run(testCase.name+"/"+method, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					// a limiter of the bubble's own, so a call waiting on it is durably blocked
					held := newLimiter(2)
					*limiterOf(t, compressor) = held
					held.acquire()
					held.acquire()

					done := make(chan error, 1)
					go func() {
						done <- call()
					}()
					synctest.Wait()
					select {
					case <-done:
						t.Fatalf("%s ran with every slot taken", method)
					default:
					}

					held.release()
					synctest.Wait()
					select {
					case err := <-done:
						if err != nil {
							t.Fatalf("%s once a slot was freed: %v", method, err)
						}
					default:
						t.Fatalf("%s still waits with a slot free", method)
					}
				})
			})
		}
	}
}

// TestZstdHoldsNoMoreEncodersThanItsBoundOrTheCPUs: zstd builds its encoders on first use, one
// per call it may run at once, so its memory follows the smaller of the bound and GOMAXPROCS
func TestZstdHoldsNoMoreEncodersThanItsBoundOrTheCPUs(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(4))
	firstUse := func(concurrency int) uint64 {
		compressor := NewZstd(zstd.SpeedDefault, concurrency)
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		_, err := compressor.Compress(compressibleBytes(64))
		runtime.ReadMemStats(&after)
		if err != nil {
			t.Fatalf("compress at concurrency %d: %v", concurrency, err)
		}
		return after.TotalAlloc - before.TotalAlloc
	}
	one, four, maximum := firstUse(1), firstUse(4), firstUse(MaximumConcurrency)
	if one*2 > four {
		t.Errorf("one encoder allocated %d bytes and four %d, want four to take over twice as "+
			"much", one, four)
	}
	if maximum > four*3/2 {
		t.Errorf("concurrency %d on 4 CPUs allocated %d bytes, want about the %d of 4 encoders",
			MaximumConcurrency, maximum, four)
	}
}

// TestLZ4BlocksAre64KB: lz4 frames carry 64 KB blocks, so a writer or reader holds buffers of
// that size rather than lz4's default 4 MB, and a block size the caller passes still wins
func TestLZ4BlocksAre64KB(t *testing.T) {
	// the frame descriptor's BD byte follows the magic number and the FLG byte, and its bits
	// 6 to 4 are the block size: 4 for 64 KB, 7 for 4 MB
	blockSizeOf := func(frame []byte) byte {
		return frame[5] >> 4 & 0x7
	}
	plain := compressibleBytes(1024)
	if got := blockSizeOf(roundTrip(t, NewLZ4(DefaultConcurrency), plain)); got != 4 {
		t.Errorf("NewLZ4 wrote block size %d, want 4 (64 KB)", got)
	}
	chosen := NewLZ4(DefaultConcurrency, lz4.BlockSizeOption(lz4.Block4Mb))
	if got := blockSizeOf(roundTrip(t, chosen, plain)); got != 7 {
		t.Errorf("NewLZ4 with Block4Mb wrote block size %d, want 7 (4 MB)", got)
	}
}
