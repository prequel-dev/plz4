package test

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/pierrec/lz4/v4"
	"github.com/prequel-dev/plz4"
)

// Basic round-trip tests for CompressBlock/DecompressBlock.
func TestCompressDecompressBlockBasic(t *testing.T) {
	defer testBorrowed(t)

	randBuf := make([]byte, 4<<10)
	if _, err := rand.Read(randBuf); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	tests := map[string][]byte{
		"nil":       nil,
		"empty":     {},
		"small":     []byte("hello world"),
		"random_4k": randBuf,
	}

	for name, src := range tests {
		name, src := name, src
		t.Run(name, func(t *testing.T) {
			cmp, err := plz4.CompressBlock(src)
			if err != nil {
				t.Fatalf("CompressBlock failed: %v", err)
			}

			// Ensure compressed size never exceeds bound.
			if len(cmp) > plz4.CompressBlockBound(len(src)) {
				t.Fatalf("compressed size %d exceeds bound %d", len(cmp), plz4.CompressBlockBound(len(src)))
			}

			dec, err := plz4.DecompressBlock(cmp)
			if err != nil {
				t.Fatalf("DecompressBlock failed: %v", err)
			}

			if !bytes.Equal(src, dec) {
				t.Fatalf("round-trip mismatch: got %d bytes, want %d", len(dec), len(src))
			}
		})
	}
}

// Verify that WithBlockShrinkToFit shrinks the returned compressed buffer
// while preserving the compressed contents.
func TestCompressBlockWithShrinkToFit(t *testing.T) {
	defer testBorrowed(t)

	src := make([]byte, 4<<10)
	if _, err := rand.Read(src); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	// Use an oversized destination buffer so that the compressed size is
	// guaranteed to be smaller than len(dst), regardless of whether the
	// data is compressible.
	dst := make([]byte, plz4.CompressBlockBound(len(src))*2)

	cmpNoShrink, err := plz4.CompressBlock(src, plz4.WithBlockDst(dst))
	if err != nil {
		t.Fatalf("CompressBlock (no shrink) failed: %v", err)
	}

	cmpShrink, err := plz4.CompressBlock(src, plz4.WithBlockDst(dst), plz4.WithBlockShrinkToFit(true))
	if err != nil {
		t.Fatalf("CompressBlock (shrink) failed: %v", err)
	}

	if !bytes.Equal(cmpNoShrink, cmpShrink) {
		t.Fatalf("compressed data mismatch between shrink and no-shrink paths")
	}
	if len(cmpNoShrink) == 0 || len(cmpShrink) == 0 {
		t.Fatalf("expected non-empty compressed buffers")
	}

	// Without shrink, the result should reuse the provided destination buffer
	// and retain its capacity.
	if &cmpNoShrink[0] != &dst[0] {
		t.Fatalf("expected no-shrink compressed buffer to share underlying array with dst")
	}
	if cap(cmpNoShrink) != cap(dst) {
		t.Fatalf("expected no-shrink compressed buffer to retain dst capacity: got %d, want %d", cap(cmpNoShrink), cap(dst))
	}

	// With shrink, the result should use a new underlying buffer sized to
	// the actual compressed length.
	if &cmpShrink[0] == &dst[0] {
		t.Fatalf("expected shrink compressed buffer to use a new underlying array")
	}
	if cap(cmpShrink) != len(cmpShrink) {
		t.Fatalf("expected shrink compressed buffer to have cap == len, got len=%d cap=%d", len(cmpShrink), cap(cmpShrink))
	}
}

// Verify that compression level option does not break round-trip.
func TestCompressDecompressBlockWithLevel(t *testing.T) {
	defer testBorrowed(t)

	src := make([]byte, 4<<10)
	if _, err := rand.Read(src); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}
	levels := []plz4.LevelT{plz4.Level1, plz4.Level3, plz4.Level6, plz4.Level9}

	for _, lvl := range levels {
		lvl := lvl
		t.Run(fmt.Sprintf("level_%d", lvl), func(t *testing.T) {
			cmp, err := plz4.CompressBlock(src, plz4.WithBlockCompressionLevel(lvl))
			if err != nil {
				t.Fatalf("CompressBlock(level=%d) failed: %v", lvl, err)
			}

			dec, err := plz4.DecompressBlock(cmp)
			if err != nil {
				t.Fatalf("DecompressBlock(level=%d) failed: %v", lvl, err)
			}

			if !bytes.Equal(src, dec) {
				t.Fatalf("round-trip mismatch at level %d", lvl)
			}
		})
	}
}

// Verify that WithBlockShrinkToFit shrinks the decompressed buffer when a
// destination slice is provided, while still round-tripping the data.
func TestDecompressBlockWithShrinkToFit(t *testing.T) {
	defer testBorrowed(t)

	src := make([]byte, 4<<10)
	if _, err := rand.Read(src); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	cmp, err := plz4.CompressBlock(src)
	if err != nil {
		t.Fatalf("CompressBlock failed: %v", err)
	}

	// Use an oversized destination buffer to make shrink/no-shrink behavior visible.
	dst := make([]byte, len(src)*2)

	decNoShrink, err := plz4.DecompressBlock(cmp, plz4.WithBlockDst(dst))
	if err != nil {
		t.Fatalf("DecompressBlock (no shrink) failed: %v", err)
	}
	if !bytes.Equal(src, decNoShrink) {
		t.Fatalf("no-shrink round-trip mismatch: got %d bytes, want %d", len(decNoShrink), len(src))
	}
	if len(decNoShrink) == 0 {
		t.Fatalf("expected non-empty decompressed buffer")
	}
	if &decNoShrink[0] != &dst[0] {
		t.Fatalf("expected no-shrink result to share underlying array with provided dst")
	}
	if cap(decNoShrink) != cap(dst) {
		t.Fatalf("expected no-shrink result to retain dst capacity: got %d, want %d", cap(decNoShrink), cap(dst))
	}

	decShrink, err := plz4.DecompressBlock(cmp, plz4.WithBlockDst(dst), plz4.WithBlockShrinkToFit(true))
	if err != nil {
		t.Fatalf("DecompressBlock (shrink) failed: %v", err)
	}
	if !bytes.Equal(src, decShrink) {
		t.Fatalf("shrink round-trip mismatch: got %d bytes, want %d", len(decShrink), len(src))
	}
	if len(decShrink) == 0 {
		t.Fatalf("expected non-empty decompressed buffer with shrink")
	}
	if &decShrink[0] == &dst[0] {
		t.Fatalf("expected shrink result to use a new underlying array")
	}
	if cap(decShrink) != len(decShrink) {
		t.Fatalf("expected shrink decompressed buffer to have cap == len, got len=%d cap=%d", len(decShrink), cap(decShrink))
	}
}

// Verify that WithBlockShrinkToFit shrinks the decompressed buffer even when
// no destination buffer is provided (dst == nil).
func TestDecompressBlockWithShrinkToFitNoDst(t *testing.T) {
	defer testBorrowed(t)

	src := make([]byte, 4<<10)
	if _, err := rand.Read(src); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	cmp, err := plz4.CompressBlock(src)
	if err != nil {
		t.Fatalf("CompressBlock failed: %v", err)
	}

	decShrink, err := plz4.DecompressBlock(cmp, plz4.WithBlockShrinkToFit(true))
	if err != nil {
		t.Fatalf("DecompressBlock (shrink, no dst) failed: %v", err)
	}

	if !bytes.Equal(src, decShrink) {
		t.Fatalf("shrink round-trip mismatch (no dst): got %d bytes, want %d", len(decShrink), len(src))
	}
	if len(decShrink) == 0 {
		t.Fatalf("expected non-empty decompressed buffer with shrink (no dst)")
	}
	// When no dst is provided, DecompressBlock allocates an internal buffer.
	// WithBlockShrinkToFit(true) should return a slice that is tightly sized
	// to the decompressed length.
	if cap(decShrink) != len(decShrink) {
		t.Fatalf("expected shrink decompressed buffer (no dst) to have cap == len, got len=%d cap=%d", len(decShrink), cap(decShrink))
	}
}

// Verify that providing a dictionary option is accepted and preserves round-trip.
func TestCompressDecompressBlockWithDictionary(t *testing.T) {
	maybeSkip(t)

	defer testBorrowed(t)

	src := make([]byte, 4<<10)
	if _, err := rand.Read(src); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}
	// Use a simple dictionary; only last 64KiB is used internally.
	dict := make([]byte, 8<<10)
	if _, err := rand.Read(dict); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	cmp, err := plz4.CompressBlock(src, plz4.WithBlockDictionary(dict))
	if err != nil {
		t.Fatalf("CompressBlock with dict failed: %v", err)
	}

	dec, err := plz4.DecompressBlock(cmp, plz4.WithBlockDictionary(dict))
	if err != nil {
		t.Fatalf("DecompressBlock with dict failed: %v", err)
	}

	if !bytes.Equal(src, dec) {
		t.Fatalf("round-trip mismatch with dict: got %d bytes, want %d", len(dec), len(src))
	}
}

// Verify that providing an incorrect dictionary on decompress either
// fails with a corrupted error or produces mismatched data.
func TestDecompressBlockWithBadDictionary(t *testing.T) {
	maybeSkip(t)

	defer testBorrowed(t)

	// Create source and two different dictionaries derived from it so that
	// the dictionary actually has useful overlap with the data.
	src := make([]byte, 4<<10)
	if _, err := rand.Read(src); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	// Use a prefix of src as the "good" dictionary.
	dictSz := 2 << 10 // 2 KiB prefix
	goodDict := append([]byte(nil), src[:dictSz]...)

	// Start from the same prefix but perturb bytes to create a "bad" dict
	// that still has overlap but will not match exactly.
	badDict := append([]byte(nil), src[:dictSz]...)
	for i := 0; i < len(badDict); i += 16 {
		badDict[i] ^= 0xFF
	}

	// Compress with good dictionary.
	cmp, err := plz4.CompressBlock(src, plz4.WithBlockDictionary(goodDict))
	if err != nil {
		t.Fatalf("CompressBlock with good dict failed: %v", err)
	}

	// Decompress with a different (bad) dictionary.
	dec, err := plz4.DecompressBlock(cmp, plz4.WithBlockDictionary(badDict))
	if err != nil {
		// When an error is returned, it should be marked corrupted.
		if !plz4.Lz4Corrupted(err) {
			t.Fatalf("expected corrupted error with bad dictionary, got: %v", err)
		}
		return
	}

	if bytes.Equal(src, dec) {
		t.Fatalf("decompression with bad dictionary unexpectedly matched original data")
	}

	// Validate that a good dictionary still works.
	decGood, err := plz4.DecompressBlock(cmp, plz4.WithBlockDictionary(goodDict))
	if err != nil {
		t.Fatalf("DecompressBlock with good dict failed: %v", err)
	}

	if !bytes.Equal(src, decGood) {
		t.Fatalf("decompression with good dictionary produced mismatched data")
	}
}

// Ensure DecompressBlock reports corruption on clearly invalid input.
func TestDecompressBlockCorruptedInput(t *testing.T) {
	maybeSkip(t)

	defer testBorrowed(t)

	badInputs := [][]byte{
		{},
		[]byte("not-a-valid-lz4-block"),
		bytes.Repeat([]byte{0xFF}, 64),
	}

	for i, src := range badInputs {
		src := src
		t.Run(fmt.Sprintf("case_%d_len_%d", i, len(src)), func(t *testing.T) {
			_, err := plz4.DecompressBlock(src)
			if err == nil {
				t.Fatalf("expected error for corrupted input, got nil")
			}

			// Corrupted block errors should be tagged as such.
			if !plz4.Lz4Corrupted(err) {
				t.Fatalf("expected corrupted error, got: %v", err)
			}
		})
	}
}

// Sanity check for CompressBlockBound growth behavior.
func TestCompressBlockBoundMonotonic(t *testing.T) {
	// This does not touch block pools but keep behavior consistent.
	defer testBorrowed(t)

	prev := 0
	for sz := 0; sz <= 1<<20; sz += 4096 {
		b := plz4.CompressBlockBound(sz)
		if b < prev {
			t.Fatalf("CompressBlockBound not monotonic: size %d -> %d, previous %d", sz, b, prev)
		}
		if b < sz {
			t.Fatalf("CompressBlockBound(%d) < size (%d)", sz, b)
		}
		prev = b
	}
}

// Prove interoperability between plz4 block compression and
// the Go lz4 block decompressor.
func TestBlockInteropPlz4ToGoLz4(t *testing.T) {
	defer testBorrowed(t)

	sizes := []int{0, 1, 1024, 4 << 10, 64 << 10}

	for _, sz := range sizes {
		sz := sz
		t.Run(fmt.Sprintf("size_%d", sz), func(t *testing.T) {
			// Generate source buffer.
			src := make([]byte, sz)
			if sz > 0 {
				if _, err := rand.Read(src); err != nil {
					t.Fatalf("rand.Read failed: %v", err)
				}
			}

			// Compress with plz4 block API.
			cmp, err := plz4.CompressBlock(src)
			if err != nil {
				t.Fatalf("plz4.CompressBlock failed: %v", err)
			}

			// Decompress with Go lz4 block API.
			dst := make([]byte, len(src))
			n, err := lz4.UncompressBlock(cmp, dst)
			if err != nil {
				t.Fatalf("lz4.UncompressBlock failed: %v", err)
			}

			if n != len(src) {
				t.Fatalf("unexpected decompressed size: got %d, want %d", n, len(src))
			}

			if !bytes.Equal(src, dst[:n]) {
				t.Fatalf("data mismatch after plz4->lz4 block round-trip")
			}
		})
	}
}

// Prove interoperability between Go lz4 block compression and
// the plz4 block decompressor.
func TestBlockInteropGoLz4ToPlz4(t *testing.T) {
	defer testBorrowed(t)

	sizes := []int{0, 1, 1024, 4 << 10, 64 << 10}

	for _, sz := range sizes {
		sz := sz
		t.Run(fmt.Sprintf("size_%d", sz), func(t *testing.T) {
			// Generate source buffer.
			src := make([]byte, sz)
			if sz > 0 {
				if _, err := rand.Read(src); err != nil {
					t.Fatalf("rand.Read failed: %v", err)
				}
			}

			// Compress with Go lz4 block API.
			var c lz4.Compressor
			cmpBuf := make([]byte, lz4.CompressBlockBound(len(src)))
			nCmp, err := c.CompressBlock(src, cmpBuf)
			if err != nil {
				t.Fatalf("lz4.Compressor.CompressBlock failed: %v", err)
			}
			cmp := cmpBuf[:nCmp]

			// Decompress with plz4 block API.
			dec, err := plz4.DecompressBlock(cmp)
			if err != nil {
				t.Fatalf("plz4.DecompressBlock failed: %v", err)
			}

			if len(dec) != len(src) {
				t.Fatalf("unexpected decompressed size: got %d, want %d", len(dec), len(src))
			}

			if !bytes.Equal(src, dec) {
				t.Fatalf("data mismatch after lz4->plz4 block round-trip")
			}
		})
	}
}

//---
// Benchmarks

// BenchmarkCompressBlock measures raw block compression throughput
// for different input sizes.
func BenchmarkCompressBlock(b *testing.B) {
	sizes := []int{1 << 10, 4 << 10, 64 << 10, 1 << 20}

	for _, sz := range sizes {
		b.Run(fmt.Sprintf("size_%d", sz), func(b *testing.B) {
			// Prepare random (mostly uncompressible) data of the given size.
			src := make([]byte, sz)
			if _, err := rand.Read(src); err != nil {
				b.Fatalf("rand.Read failed: %v", err)
			}

			dst := make([]byte, plz4.CompressBlockBound(len(src)))

			b.SetBytes(int64(len(src)))
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if _, err := plz4.CompressBlock(src, plz4.WithBlockDst(dst)); err != nil {
					b.Fatalf("CompressBlock failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkDecompressBlock measures raw block decompression throughput
// matching the data produced by BenchmarkCompressBlock-sized inputs.
func BenchmarkDecompressBlock(b *testing.B) {
	sizes := []int{1 << 10, 4 << 10, 64 << 10, 1 << 20}

	for _, sz := range sizes {
		sz := sz
		b.Run(fmt.Sprintf("size_%d", sz), func(b *testing.B) {
			// Prepare sample compressed data once.
			src := make([]byte, sz)
			if _, err := rand.Read(src); err != nil {
				b.Fatalf("rand.Read failed: %v", err)
			}

			cmp, err := plz4.CompressBlock(src)
			if err != nil {
				b.Fatalf("CompressBlock failed while preparing sample: %v", err)
			}

			dst := make([]byte, sz)

			b.SetBytes(int64(len(src)))
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				dec, err := plz4.DecompressBlock(cmp, plz4.WithBlockDst(dst))
				if err != nil {
					b.Fatalf("DecompressBlock failed: %v", err)
				}
				if len(dec) != len(src) {
					b.Fatalf("unexpected decompressed size: got %d, want %d", len(dec), len(src))
				}
			}
		})
	}
}

// BenchmarkCompressBlockPlz4WithLevel measures how compression level
// affects performance across different input sizes.
func BenchmarkCompressBlockPlz4WithLevel(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping slow benchmark in short mode")
	}

	src, _ := LoadSample(b, LargeUncompressed)
	slices := [][]byte{
		src[:64<<10],  // 64 KiB slice for benchmarking
		src[:256<<10], // 256 KiB slice for benchmarking
		src[:1<<20],   // 1 MiB slice for benchmarking
		src[:8<<20],   // 8 MiB slice for benchmarking
	}

	var (
		dst    = make([]byte, plz4.CompressBlockBound(len(src)))
		levels = []plz4.LevelT{plz4.Level1, plz4.Level3, plz4.Level6, plz4.Level9}
	)
	for _, src := range slices {
		for _, lvl := range levels {
			b.Run(fmt.Sprintf("level_%d/size_%d", lvl, len(src)), func(b *testing.B) {
				b.SetBytes(int64(len(src)))
				b.ReportAllocs()
				b.ResetTimer()

				for range b.N {
					v, err := plz4.CompressBlock(src, plz4.WithBlockCompressionLevel(lvl), plz4.WithBlockDst(dst))
					if err != nil {
						b.Fatalf("CompressBlock(level=%d) failed: %v", lvl, err)
					}
					b.ReportMetric(float64(len(v))/float64(len(src))*100.0, "ratio")
				}
			})
		}
	}
}

// BenchmarkCompressBlockLz4WithLevel measures how compression level
// affects performance across different input sizes using the lz4 library directly.
func BenchmarkCompressBlockLz4WithLevel(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping slow benchmark in short mode")
	}

	src, _ := LoadSample(b, LargeUncompressed)
	slices := [][]byte{
		src[:64<<10],  // 64 KiB slice for benchmarking
		src[:256<<10], // 256 KiB slice for benchmarking
		src[:1<<20],   // 1 MiB slice for benchmarking
		src[:8<<20],   // 8 MiB slice for benchmarking
	}

	var (
		dst    = make([]byte, plz4.CompressBlockBound(len(src)))
		levels = []lz4.CompressionLevel{lz4.Fast, lz4.Level3, lz4.Level6, lz4.Level9}
	)
	for _, src := range slices {
		for _, lvl := range levels {
			b.Run(fmt.Sprintf("level_%d/size_%d", lvl, len(src)), func(b *testing.B) {
				b.SetBytes(int64(len(src)))
				b.ReportAllocs()
				b.ResetTimer()

				for range b.N {
					var err error
					var n int
					if lvl == lz4.Fast {
						n, err = lz4.CompressBlock(src, dst, nil)
					} else {
						n, err = lz4.CompressBlockHC(src, dst, lvl, nil, nil)
					}

					if err != nil {
						b.Fatalf("CompressBlock(level=%d) failed: %v", lvl, err)
					}
					b.ReportMetric(float64(n)/float64(len(src))*100.0, "ratio")
				}
			})
		}
	}
}
