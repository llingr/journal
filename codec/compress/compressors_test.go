// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package compress

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

const (
	payloadSize = 25_000

	// four callers for each slot of the default bound, so every call contends for one and the
	// pooled writers and readers pass between goroutines; under -race each goroutine adds to
	// the detector's own memory, which a thousand took past 600 MB
	goroutines = 4 * DefaultConcurrency
)

type compressorCase struct {
	name        string
	new         func() Compressor
	compression DataCompression
}

var compressorCases = []compressorCase{
	{"gzip", func() Compressor {
		return NewGzip(gzip.DefaultCompression, DefaultConcurrency)
	}, Gzip},
	{"lz4", func() Compressor {
		return NewLZ4(DefaultConcurrency)
	}, LZ4},
	{"snappy", NewSnappy, Snappy},
	{"zstd", func() Compressor {
		return NewZstd(zstd.SpeedDefault, DefaultConcurrency)
	}, Zstd},
}

// randomBytes is deterministic per seed, so a failure names a reproducible payload.
func randomBytes(seed uint64, size int) []byte {
	var key [32]byte
	for i := range 8 {
		key[i] = byte(seed >> (8 * i))
	}
	payload := make([]byte, size)
	_, _ = rand.NewChaCha8(key).Read(payload)
	return payload
}

func compressibleBytes(size int) []byte {
	return bytes.Repeat([]byte("journal events "), size/15+1)[:size]
}

func roundTrip(t *testing.T, compressor Compressor, plain []byte) []byte {
	t.Helper()
	compressed, err := compressor.Compress(plain)
	if err != nil {
		t.Fatalf("Compress(%d bytes): %v", len(plain), err)
	}
	restored, err := compressor.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress(%d bytes): %v", len(compressed), err)
	}
	if !bytes.Equal(restored, plain) {
		t.Fatalf("round trip of %d bytes restored %d bytes that differ", len(plain), len(restored))
	}
	return compressed
}

func TestCompression(t *testing.T) {
	for _, testCase := range compressorCases {
		if got := testCase.new().Compression(); got != testCase.compression {
			t.Errorf("%s: Compression() = %s, want %s", testCase.name, got, testCase.compression)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	payloads := map[string][]byte{
		"nil":                  nil,
		"empty":                {},
		"one byte":             {0x42},
		"25k random":           randomBytes(1, payloadSize),
		"25k compressible":     compressibleBytes(payloadSize),
		"1 MiB random":         randomBytes(2, 1<<20),
		"256 KiB compressible": compressibleBytes(256 << 10),
	}
	for _, testCase := range compressorCases {
		compressor := testCase.new()
		for name, plain := range payloads {
			t.Run(testCase.name+"/"+name, func(t *testing.T) {
				roundTrip(t, compressor, plain)
			})
		}
	}
}

// TestCompressibleInputShrinks: the configured level is in effect, not a zero-value writer.
func TestCompressibleInputShrinks(t *testing.T) {
	plain := compressibleBytes(payloadSize)
	for _, testCase := range compressorCases {
		t.Run(testCase.name, func(t *testing.T) {
			compressed := roundTrip(t, testCase.new(), plain)
			if len(compressed) > len(plain)/4 {
				t.Errorf("%d compressible bytes compressed to %d, want under %d", len(plain),
					len(compressed), len(plain)/4)
			}
		})
	}
}

// TestRandomInputDoesNotBloat: incompressible input costs a header, not a multiple.
func TestRandomInputDoesNotBloat(t *testing.T) {
	plain := randomBytes(3, payloadSize)
	for _, testCase := range compressorCases {
		t.Run(testCase.name, func(t *testing.T) {
			compressed := roundTrip(t, testCase.new(), plain)
			if len(compressed) > len(plain)+256 {
				t.Errorf("%d random bytes compressed to %d", len(plain), len(compressed))
			}
		})
	}
}

// TestDecompressRejectsCorruptInput: an error, never a panic, never a silent partial.
func TestDecompressRejectsCorruptInput(t *testing.T) {
	plain := randomBytes(4, payloadSize)
	for _, testCase := range compressorCases {
		compressor := testCase.new()
		valid := roundTrip(t, compressor, plain)
		corrupt := map[string][]byte{
			"garbage":        randomBytes(5, 64),
			"truncated":      valid[:len(valid)/2],
			"one byte short": valid[:len(valid)-1],
			"one byte over":  append(bytes.Clone(valid), 0),
			"single byte":    {0xff},
		}
		for name, input := range corrupt {
			t.Run(testCase.name+"/"+name, func(t *testing.T) {
				if _, err := compressor.Decompress(input); err == nil {
					t.Error("Decompress returned nil error")
				}
			})
		}
	}
}

// TestDecompressEmptyInput: a blob Compress never produces is ErrEmptyInput on every codec.
func TestDecompressEmptyInput(t *testing.T) {
	for _, testCase := range compressorCases {
		t.Run(testCase.name, func(t *testing.T) {
			for _, input := range [][]byte{nil, {}} {
				out, err := testCase.new().Decompress(input)
				if !errors.Is(err, ErrEmptyInput) {
					t.Errorf("Decompress(empty) error = %v, want ErrEmptyInput", err)
				}
				if len(out) != 0 {
					t.Errorf("Decompress(empty) returned %d bytes", len(out))
				}
			}
		})
	}
}

// TestResultsDoNotAlias: outputs are owned by the caller, inputs are not retained.
func TestResultsDoNotAlias(t *testing.T) {
	first := randomBytes(6, payloadSize)
	second := randomBytes(7, payloadSize)
	for _, testCase := range compressorCases {
		t.Run(testCase.name, func(t *testing.T) {
			compressor := testCase.new()

			input := bytes.Clone(first)
			compressedFirst, err := compressor.Compress(input)
			if err != nil {
				t.Fatal(err)
			}
			snapshot := bytes.Clone(compressedFirst)
			for i := range input {
				input[i] = ^input[i]
			}
			if _, err = compressor.Compress(second); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(compressedFirst, snapshot) {
				t.Error("compressed output changed after mutating the input and compressing again")
			}

			restoredFirst, err := compressor.Decompress(compressedFirst)
			if err != nil {
				t.Fatal(err)
			}
			for i := range compressedFirst {
				compressedFirst[i] = ^compressedFirst[i]
			}
			restoredAgain, err := compressor.Decompress(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			for i := range restoredAgain {
				restoredAgain[i] = ^restoredAgain[i]
			}
			if !bytes.Equal(restoredFirst, first) {
				t.Error("decompressed output changed after mutating its input and a later result")
			}
		})
	}
}

// TestPoolRecoversAfterErrors: a reader dropped on a bad input never poisons the next call.
func TestPoolRecoversAfterErrors(t *testing.T) {
	plain := randomBytes(8, payloadSize)
	garbage := randomBytes(9, 64)
	for _, testCase := range compressorCases {
		t.Run(testCase.name, func(t *testing.T) {
			compressor := testCase.new()
			valid := roundTrip(t, compressor, plain)
			for range 200 {
				if _, err := compressor.Decompress(garbage); err == nil {
					t.Fatal("garbage decompressed without error")
				}
				restored, err := compressor.Decompress(valid)
				if err != nil || !bytes.Equal(restored, plain) {
					t.Fatalf("valid input failed after a bad one: %v", err)
				}
			}
		})
	}
}

func TestSequentialReuseAcrossSizes(t *testing.T) {
	for _, testCase := range compressorCases {
		t.Run(testCase.name, func(t *testing.T) {
			compressor := testCase.new()
			for size := 0; size < 4096; size += 37 {
				roundTrip(t, compressor, randomBytes(uint64(size), size))
				roundTrip(t, compressor, compressibleBytes(size))
			}
		})
	}
}

// TestDistinctInstancesKeepTheirOwnConfiguration: level lives in the instance, not a global.
func TestDistinctInstancesKeepTheirOwnConfiguration(t *testing.T) {
	plain := compressibleBytes(payloadSize)
	fast := NewGzip(gzip.BestSpeed, DefaultConcurrency)
	best := NewGzip(gzip.BestCompression, DefaultConcurrency)
	fastOut := roundTrip(t, fast, plain)
	bestOut := roundTrip(t, best, plain)
	roundTrip(t, fast, plain)
	if bytes.Equal(fastOut, bestOut) {
		t.Error("BestSpeed and BestCompression produced identical output")
	}
	if restored, err := best.Decompress(fastOut); err != nil || !bytes.Equal(restored, plain) {
		t.Errorf("one instance could not read another's output: %v", err)
	}
}

// TestGzipLevels: every level gzip supports works, and any other stops the program where the
// compressor is built rather than at its first write.
func TestGzipLevels(t *testing.T) {
	plain := randomBytes(10, 1024)
	levels := []int{
		gzip.HuffmanOnly,
		gzip.DefaultCompression,
		gzip.NoCompression,
		gzip.BestSpeed,
		gzip.BestCompression,
	}
	for _, level := range levels {
		roundTrip(t, NewGzip(level, DefaultConcurrency), plain)
	}
	for _, level := range []int{-3, 10, 42} {
		recovered := constructionPanic(func() {
			NewGzip(level, DefaultConcurrency)
		})
		err, isError := recovered.(error)
		if !isError || !errors.Is(err, ErrGzipLevel) {
			t.Errorf("level %d: NewGzip panicked with %v, want ErrGzipLevel", level, recovered)
		}
	}
}

// TestLZ4Options: an option lz4 rejects stops the program where the compressor is built.
func TestLZ4Options(t *testing.T) {
	plain := randomBytes(11, 1024)
	roundTrip(t, NewLZ4(DefaultConcurrency), plain)
	roundTrip(t, NewLZ4(DefaultConcurrency, lz4.CompressionLevelOption(lz4.Level9)), plain)

	recovered := constructionPanic(func() {
		NewLZ4(DefaultConcurrency, lz4.CompressionLevelOption(lz4.CompressionLevel(99)))
	})
	if recovered == nil {
		t.Error("an option lz4 rejects did not stop NewLZ4")
	}
}

// TestZstdLevels: an unsupported level stops the program where the compressor is built.
func TestZstdLevels(t *testing.T) {
	plain := randomBytes(12, 1024)
	levels := []zstd.EncoderLevel{
		zstd.SpeedFastest,
		zstd.SpeedDefault,
		zstd.SpeedBetterCompression,
		zstd.SpeedBestCompression,
	}
	for _, level := range levels {
		// one encoder each, as at best compression an encoder holds about 35 MB
		roundTrip(t, NewZstd(level, MinimumConcurrency), plain)
	}
	recovered := constructionPanic(func() {
		NewZstd(zstd.EncoderLevel(99), DefaultConcurrency)
	})
	if recovered == nil {
		t.Error("an unsupported level did not stop NewZstd")
	}
}

// constructionPanic runs build and returns what it panicked with, nil when it did not
func constructionPanic(build func()) (recovered any) {
	defer func() {
		recovered = recover()
	}()
	build()
	return nil
}

// TestOutputIsStandardFormat: the stored bytes are readable by the codec's own library, with
// no framing of ours.
func TestOutputIsStandardFormat(t *testing.T) {
	plain := randomBytes(13, payloadSize)
	decoders := map[string]func([]byte) ([]byte, error){
		"gzip": func(b []byte) ([]byte, error) {
			reader, err := gzip.NewReader(bytes.NewReader(b))
			if err != nil {
				return nil, err
			}
			return io.ReadAll(reader)
		},
		"lz4": func(b []byte) ([]byte, error) {
			return io.ReadAll(lz4.NewReader(bytes.NewReader(b)))
		},
		"snappy": func(b []byte) ([]byte, error) {
			return snappy.Decode(nil, b)
		},
		"zstd": func(b []byte) ([]byte, error) {
			decoder, err := zstd.NewReader(nil)
			if err != nil {
				return nil, err
			}
			defer decoder.Close()
			return decoder.DecodeAll(b, nil)
		},
	}
	for _, testCase := range compressorCases {
		t.Run(testCase.name, func(t *testing.T) {
			compressed := roundTrip(t, testCase.new(), plain)
			restored, err := decoders[testCase.name](compressed)
			if err != nil || !bytes.Equal(restored, plain) {
				t.Errorf("library decoder failed on our output: %v", err)
			}
		})
	}
}

// TestConcurrentRoundTrips: one shared instance, more goroutines than slots, distinct
// payloads, run under -race.
func TestConcurrentRoundTrips(t *testing.T) {
	for _, testCase := range compressorCases {
		t.Run(testCase.name, func(t *testing.T) {
			compressor := testCase.new()
			var wg sync.WaitGroup
			for g := range goroutines {
				wg.Add(1)
				go func() {
					defer wg.Done()
					plain := randomBytes(uint64(g), payloadSize)
					if g%2 == 1 {
						plain = compressibleBytes(payloadSize - g)
					}
					for range 3 {
						compressed, err := compressor.Compress(plain)
						if err != nil {
							t.Errorf("goroutine %d: Compress: %v", g, err)
							return
						}
						restored, err := compressor.Decompress(compressed)
						if err != nil {
							t.Errorf("goroutine %d: Decompress: %v", g, err)
							return
						}
						if !bytes.Equal(restored, plain) {
							t.Errorf("goroutine %d: restored bytes differ", g)
							return
						}
					}
				}()
			}
			wg.Wait()
		})
	}
}

// TestConcurrentRoundTripsWithFailures: bad inputs on some goroutines never corrupt the rest.
func TestConcurrentRoundTripsWithFailures(t *testing.T) {
	garbage := randomBytes(14, 64)
	for _, testCase := range compressorCases {
		t.Run(testCase.name, func(t *testing.T) {
			compressor := testCase.new()
			valid := roundTrip(t, compressor, randomBytes(15, payloadSize))
			var wg sync.WaitGroup
			for g := range goroutines {
				wg.Add(1)
				go func() {
					defer wg.Done()
					switch g % 4 {
					case 0:
						if _, err := compressor.Decompress(garbage); err == nil {
							t.Errorf("goroutine %d: garbage decompressed", g)
						}
					case 1:
						if _, err := compressor.Decompress(valid[:len(valid)/2]); err == nil {
							t.Errorf("goroutine %d: truncated input decompressed", g)
						}
					default:
						plain := randomBytes(uint64(g), payloadSize)
						compressed, err := compressor.Compress(plain)
						if err != nil {
							t.Errorf("goroutine %d: Compress: %v", g, err)
							return
						}
						restored, err := compressor.Decompress(compressed)
						if err != nil || !bytes.Equal(restored, plain) {
							t.Errorf("goroutine %d: round trip failed beside failing neighbours: %v", g, err)
						}
					}
				}()
			}
			wg.Wait()
		})
	}
}
