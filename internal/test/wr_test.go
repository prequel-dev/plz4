package test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/prequel-dev/plz4"
	"github.com/prequel-dev/plz4/internal/pkg/blk"
	"github.com/prequel-dev/plz4/internal/pkg/wpool"
)

var cgoEnabled = true

func maybeSkip(t *testing.T) {
	if !cgoEnabled {
		t.Skip("Skipping test that requires CGO")
	}
}

type Option = plz4.OptT

func testBorrowed(t testing.TB) {
	if v := blk.CntBorrowed(); v != 0 {
		t.Errorf("Fail block cnt test: %v", v)
	}
}

type writeBasicT struct {
	src   []byte
	dict  []byte
	hash  string
	wopts []plz4.OptT
}

func (wb writeBasicT) rOpts() []plz4.OptT {
	opts := []Option{plz4.WithParallel(-1)}
	if wb.dict != nil {
		opts = append(opts, plz4.WithDictionary(wb.dict))
	}
	return opts
}

func writeBasics(t *testing.T) map[string]writeBasicT {
	var (
		lsrc, lhash = LoadSample(t, LargeUncompressed)
		usrc, uhash = LoadSample(t, Uncompressable)
	)

	defOpts := func(opt ...Option) writeBasicT {
		return writeBasicT{
			src:   lsrc,
			hash:  lhash,
			wopts: opt,
		}
	}

	dictOpts := func(opt ...Option) writeBasicT {
		opt = append(opt, plz4.WithDictionary(lsrc))
		w := defOpts(opt...)
		w.dict = lsrc
		return w
	}

	basics := map[string]writeBasicT{
		"empty":             {hash: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		"level1":            defOpts(plz4.WithLevel(1)),
		"level3":            defOpts(plz4.WithLevel(3)),
		"content_sum_on":    defOpts(plz4.WithContentChecksum(true)),
		"content_sum_off":   defOpts(plz4.WithContentChecksum(false)),
		"block_sum_on":      defOpts(plz4.WithBlockChecksum(true)),
		"block_sum_off":     defOpts(plz4.WithBlockChecksum(false)),
		"content_block_on":  defOpts(plz4.WithContentChecksum(true), plz4.WithBlockChecksum(true)),
		"content_block_off": defOpts(plz4.WithContentChecksum(false), plz4.WithBlockChecksum(false)),
		"block_size_64K":    defOpts(plz4.WithBlockSize(plz4.BlockIdx64KB)),
		"block_size_256":    defOpts(plz4.WithBlockSize(plz4.BlockIdx256KB)),
		"block_size_1MB":    defOpts(plz4.WithBlockSize(plz4.BlockIdx1MB)),
		"block_size_4MB":    defOpts(plz4.WithBlockSize(plz4.BlockIdx4MB)),
		"content_size_on":   defOpts(plz4.WithContentSize(uint64(len(lsrc)))),
		"uncompressable":    {src: usrc, hash: uhash},
	}

	if cgoEnabled {
		basics["dict_enabled"] = dictOpts()
		basics["dict_HC"] = dictOpts(plz4.WithLevel(3))
		basics["linked"] = defOpts(plz4.WithBlockLinked(true))
		basics["linked_HC"] = defOpts(plz4.WithBlockLinked(true), plz4.WithLevel(3))
		basics["linked_with_dict"] = dictOpts(plz4.WithBlockLinked(true))
		if !testing.Short() {
			basics["dict_12"] = dictOpts(plz4.WithLevel(12))
		}
	} else if !testing.Short() {
		basics["level12"] = defOpts(plz4.WithLevel(12))
	}

	return basics
}

// Run the basic tests on ReadFrom interface.
// Run with variable parallel level.
func TestLz4FrameReadFrom(t *testing.T) {
	defer testBorrowed(t)

	for name, tc := range writeBasics(t) {
		// Run each case at a different parallel level
		parallels := []int{0, 1, -1}

		if testing.Short() {
			parallels = []int{0, -1}
		}

		for _, nParallel := range parallels {
			tc.wopts = append(tc.wopts, plz4.WithParallel(nParallel))

			t.Run(fmt.Sprintf("%s_%d", name, nParallel), func(t *testing.T) {

				var (
					compressedData   = compressReadFrom(t, tc.src, tc.wopts)
					uncompressedData = decompressWriteTo(t, compressedData.Bytes(), tc.rOpts())
				)

				if sum := Sha2sum(uncompressedData.Bytes()); sum != tc.hash {
					t.Errorf("Expected sha2: %s got %s", tc.hash, sum)
				}
			})
		}
	}
}

// Run the basic tests on Write interface.
// Run with variable parallel level.
func TestLz4FrameWrite(t *testing.T) {
	defer testBorrowed(t)

	parallels := []int{0, 1, -1}

	if testing.Short() {
		parallels = []int{0, -1}
	}

	for name, tc := range writeBasics(t) {
		// Run each case at a different parallel level

		for _, nParallel := range parallels {
			tc.wopts = append(tc.wopts, plz4.WithParallel(nParallel))

			t.Run(fmt.Sprintf("%s_%d", name, nParallel), func(t *testing.T) {

				var (
					chunkSz          = 24 << 10
					compressedData   = compressWrite(t, tc.src, tc.wopts, chunkSz)
					uncompressedData = decompressWriteTo(t, compressedData.Bytes(), tc.rOpts())
				)

				if sum := Sha2sum(uncompressedData.Bytes()); sum != tc.hash {
					t.Errorf("Expected sha2: %s got %s", tc.hash, sum)
				}
			})
		}
	}
}

// Use Random large block sizes to exercise buffer handling;
// particularly in the sync mode where the writer will write
// the large buffer directly if it is at least block size.
func TestLz4FrameWriteDirect(t *testing.T) {
	defer testBorrowed(t)

	parallels := []int{0, 1, -1}

	if testing.Short() {
		parallels = []int{0, -1}
	}

	for name, tc := range writeBasics(t) {
		// Run each case at a different parallel level
		for _, nParallel := range parallels {
			tc.wopts = append(tc.wopts, plz4.WithParallel(nParallel))

			t.Run(fmt.Sprintf("%s_%d", name, nParallel), func(t *testing.T) {

				var (
					chunkSz          = rand.IntN(4<<20) + 4<<20
					compressedData   = compressWrite(t, tc.src, tc.wopts, chunkSz)
					uncompressedData = decompressWriteTo(t, compressedData.Bytes(), tc.rOpts())
				)

				if sum := Sha2sum(uncompressedData.Bytes()); sum != tc.hash {
					t.Errorf("Expected sha2: %s got %s", tc.hash, sum)
				}
			})
		}
	}
}

func _testLz4WriteProgress(t *testing.T, opts ...Option) ([]byte, []int64, bytes.Buffer) {
	lsrc, _ := LoadSample(t, LargeUncompressed)

	var pos []int64
	handler := func(srcPos, dstPos int64) {
		pos = append(pos, srcPos, dstPos)
	}

	opts = append(opts, plz4.WithProgress(handler))

	buf := compressReadFrom(t, lsrc, opts)
	return lsrc, pos, buf
}

// Expect the write handler to emit to on block boundaries
func testLz4WriteHandler(t *testing.T, opts ...Option) {
	lsrc, pos, buf := _testLz4WriteProgress(t, opts...)
	validatePos(t, lsrc, buf.Bytes(), pos)
}

// Test write handler in sync mode
func TestLz4WriteHandlerSync(t *testing.T) {
	defer testBorrowed(t)
	testLz4WriteHandler(t, plz4.WithParallel(0))
}

// Test write handler in async mode
func TestLz4WriteHandlerAsync(t *testing.T) {
	defer testBorrowed(t)
	testLz4WriteHandler(t, plz4.WithParallel(4))
}

// Write to Lz4 one byte at a time and flush on each write.
// This will cause a new block to be created on each flush and will
// cause output to expand rather than compress.
// The test validates that the flush mechanisms work correctly.
func testLz4FlushOneByteAtATime(t *testing.T, src []byte, opts ...Option) []byte {
	return testLz4FlushWrite(t, src, singleStep, opts...)
}

func testLz4FlushWrite(t *testing.T, src []byte, s stepT, opts ...Option) []byte {

	var buf bytes.Buffer
	wr := plz4.NewWriter(&buf, opts...)

	for i := 0; i < len(src); {
		cnt := s(len(src) - i)
		n, err := wr.Write(src[i : i+cnt])
		if err != nil || n != cnt {
			t.Fatalf("Fail write single byte %d:%v", n, err)
		}
		i += n

		if err := wr.Flush(); err != nil {
			t.Fatalf("Fail flush: %v", err)
		}
	}

	if err := wr.Close(); err != nil {
		t.Fatalf("Fail close: %v", err)
	}

	// Validate buffer
	var uncompressed = decompressWriteTo(t, buf.Bytes(), nil)
	if !bytes.Equal(src, uncompressed.Bytes()) {
		t.Errorf("Bytes not equal")
	}

	return buf.Bytes()
}

// Test single step write and flush in sync mode.
func TestLz4FlushOneByteAtATimeSync(t *testing.T) {
	defer testBorrowed(t)
	lsrc, _ := LoadSample(t, LargeUncompressed)
	shortSz := (1 << 20) + 11

	testLz4FlushOneByteAtATime(t, lsrc[:shortSz], plz4.WithParallel(0))
}

// Test single step write and flush in async mode.
func TestLz4FlushOneByteAtATimeParallel(t *testing.T) {
	defer testBorrowed(t)
	lsrc, _ := LoadSample(t, LargeUncompressed)
	shortSz := (1 << 20) + 11
	testLz4FlushOneByteAtATime(t, lsrc[:shortSz], plz4.WithParallel(4))
}

func TestLz4FlushRandomParallel(t *testing.T) {
	defer testBorrowed(t)
	lsrc, _ := LoadSample(t, LargeUncompressed)
	testLz4FlushWrite(t, lsrc, randStep, plz4.WithParallel(4))
}

// Test single step write and flush with linked mode.
func TestLz4FlushOneByteAtATimeLinked(t *testing.T) {
	maybeSkip(t)
	defer testBorrowed(t)
	lsrc, _ := LoadSample(t, LargeUncompressed)
	shortSz := (1 << 20) + 11
	testLz4FlushOneByteAtATime(t, lsrc[:shortSz], plz4.WithParallel(4), plz4.WithBlockLinked(true))
}

func TestLz4FlushRandomLinked(t *testing.T) {
	maybeSkip(t)
	defer testBorrowed(t)
	lsrc, _ := LoadSample(t, LargeUncompressed)
	testLz4FlushWrite(t, lsrc, randStep, plz4.WithParallel(4), plz4.WithBlockLinked(true))
}

// Single step with handler enabled
func testLz4FlushOneByteAtATimeWithHandler(t *testing.T, opts ...Option) {
	lsrc, _ := LoadSample(t, LargeUncompressed)
	shortSz := (256 << 10) + 11
	lsrc = lsrc[:shortSz]

	var pos []int64
	handler := func(srcPos, dstPos int64) {
		pos = append(pos, srcPos, dstPos)
	}

	opts = append(opts, plz4.WithProgress(handler))

	dst := testLz4FlushOneByteAtATime(t, lsrc, opts...)

	// One for each boundary plus final trailer block
	tgt := 2 * (shortSz + 1)
	if len(pos) != tgt {
		t.Errorf("Did not flush on each spin %v != %v", len(pos), tgt)
	}

	validatePos(t, lsrc, dst, pos)
}

// Test single step with flush synchronously with handler enabled.
func TestLz4FlushOneByteAtATimeWithHandlerSync(t *testing.T) {
	defer testBorrowed(t)
	testLz4FlushOneByteAtATimeWithHandler(t, plz4.WithParallel(0))
}

// Test single step with flush in parallel with handler enabled.
func TestLz4FlushOneByteAtATimeWithHandlerAsync(t *testing.T) {
	defer testBorrowed(t)
	testLz4FlushOneByteAtATimeWithHandler(t, plz4.WithParallel(4))
}

type stepT func(max int) int

func singleStep(max int) int { return 1 }
func randStep(max int) int   { return rand.IntN(max) + 1 }

func testSingleFlushReadFromInterface(t *testing.T, src []byte, opts ...Option) []byte {
	return testFlushReadFromInterface(t, src, singleStep, opts...)
}

func testFlushReadFromInterface(t *testing.T, src []byte, sfunc stepT, opts ...Option) []byte {
	var (
		buf bytes.Buffer
		rdr = bytes.NewReader(nil)
	)
	wr := plz4.NewWriter(&buf, opts...)

	// Early flush should be fine.
	if err := wr.Flush(); err != nil {
		t.Fatalf("Fail flush: %v", err)
	}

	for i := range src {
		cnt := sfunc(len(src) - i)
		rdr.Reset(src[i : i+cnt])
		n, err := wr.ReadFrom(rdr)
		if err != nil || n != 1 {
			t.Fatalf("Fail write single byte %d:%v", n, err)
		}

		if err := wr.Flush(); err != nil {
			t.Fatalf("Fail flush: %v", err)
		}
	}

	// Extra flushes should not be a problem
	if err := wr.Flush(); err != nil {
		t.Errorf("Expected nil flush, got: %v", err)
	}
	if err := wr.Flush(); err != nil {
		t.Errorf("Expected nil flush, got: %v", err)
	}

	if err := wr.Close(); err != nil {
		t.Fatalf("Fail close: %v", err)
	}

	var uncompressed = decompressWriteTo(t, buf.Bytes(), nil)
	if !bytes.Equal(src, uncompressed.Bytes()) {
		t.Errorf("Bytes not equal")
	}

	return buf.Bytes()
}

func TestSingleFlushReadFromInterfaceSync(t *testing.T) {
	defer testBorrowed(t)
	lsrc, _ := LoadSample(t, LargeUncompressed)
	shortSz := 1<<20 + 999 // Full sample takes too long
	testSingleFlushReadFromInterface(t, lsrc[:shortSz], plz4.WithParallel(0))
}

func TestSingleFlushReadFromInterfaceParallel(t *testing.T) {
	defer testBorrowed(t)
	lsrc, _ := LoadSample(t, LargeUncompressed)
	shortSz := 100<<10 + 999 // Full sample takes too long
	testSingleFlushReadFromInterface(t, lsrc[:shortSz], plz4.WithParallel(4))
}

func testDictId(t *testing.T, id uint32, data []byte, opts ...Option) {
	var buf bytes.Buffer
	wr := plz4.NewWriter(&buf, opts...)

	if n, err := wr.Write(data); err != nil && n != len(data) {
		t.Errorf("Fail write: %v", err)
	}
	if err := wr.Close(); err != nil {
		t.Errorf("Fail close: %v", err)
	}
	var dictId uint32
	cb := func(id uint32) ([]byte, error) {
		dictId = id
		return nil, nil
	}
	rd := plz4.NewReader(&buf, plz4.WithDictCallback(cb))
	var nbuf bytes.Buffer
	if n, err := rd.WriteTo(&nbuf); err != nil || n != int64(len(data)) {
		t.Errorf("Fail writeTo: %v", err)
	}
	if err := rd.Close(); err != nil {
		t.Errorf("Fail close: %v", err)
	}

	if dictId != id {
		t.Errorf("Expected dictId:%v got %v", id, dictId)
	}
}

// Test that dict Id is written properly to the headers.
// Leverage the callback on the read routine to validate.
func TestLz4WriterDictID(t *testing.T) {
	defer testBorrowed(t)
	data := []byte("testy")

	// Test DictId boundaries
	testDictId(t, math.MaxUint32, data, plz4.WithDictionaryId(math.MaxUint32))
	testDictId(t, 0, data, plz4.WithDictionaryId(0))

	// Test DictId with content size enabled
	testDictId(
		t,
		math.MaxUint32,
		data,
		plz4.WithDictionaryId(math.MaxUint32),
		plz4.WithContentSize(uint64(len(data))),
	)
}

// Ensure that a user-provided dictionary is applied when compressing in
// linked-block mode, for both synchronous and asynchronous writers.
// We verify this by compressing with a known dictionary and then
// confirming that decompression with a bad dictionary either fails with
// a corrupted error or produces mismatched data, while decompression
// with the correct dictionary round-trips.
func TestLz4WriterLinkedDictionaryApplied(t *testing.T) {
	maybeSkip(t)
	defer testBorrowed(t)

	// Source payload large enough to span multiple 64KiB blocks so that
	// linked-block behavior and inter-block dictionaries are exercised.
	src := make([]byte, 256<<10)
	for i := range src {
		// Deterministic, pseudo-random pattern to avoid trivially
		// compressible data while still overlapping with the dictionary.
		src[i] = byte(i % 251)
	}

	// Use a prefix of src as the "good" dictionary so that there is
	// meaningful overlap between the dictionary and the payload.
	dictSz := 2 << 10 // 2 KiB prefix
	if dictSz > len(src) {
		t.Fatalf("dictionary size %d larger than src %d", dictSz, len(src))
	}
	goodDict := append([]byte(nil), src[:dictSz]...)

	parallels := []int{0, 4} // 0 = sync writer, 4 = async/parallel writer

	for _, nParallel := range parallels {
		t.Run(fmt.Sprintf("parallel_%d", nParallel), func(t *testing.T) {

			var buf bytes.Buffer
			wr := plz4.NewWriter(&buf,
				plz4.WithDictionary(goodDict),
				plz4.WithBlockLinked(true),
				plz4.WithBlockSize(plz4.BlockIdx64KB),
				plz4.WithParallel(nParallel),
			)

			// Stream the payload in smaller chunks to exercise true
			// streaming behavior across multiple linked blocks.
			const chunkSz = 8 << 10
			if sum, err := spinWrite(wr, src, chunkSz, false); err != nil || sum != int64(len(src)) {
				t.Fatalf("Fail write: sum=%d len=%d err=%v", sum, len(src), err)
			}

			if err := wr.Close(); err != nil {
				t.Fatalf("Fail close: %v", err)
			}

			cmp := buf.Bytes()

			// Verify that decompression with the correct dictionary preserves
			// the original payload.
			decGood := decompressWriteTo(t, cmp, []Option{plz4.WithDictionary(goodDict)})
			if !bytes.Equal(src, decGood.Bytes()) {
				t.Fatalf("round-trip with good dictionary mismatch: got %d bytes, want %d", decGood.Len(), len(src))
			}

			// Now perturb the dictionary and ensure that decompression with
			// this bad dictionary does not successfully round-trip. This
			// demonstrates that the compressed stream actually depends on the
			// user-provided dictionary in linked mode.
			badDict := append([]byte(nil), goodDict...)
			for i := 0; i < len(badDict); i += 16 {
				badDict[i] ^= 0xFF
			}

			rd := plz4.NewReader(bytes.NewReader(cmp), plz4.WithDictionary(badDict))
			var out bytes.Buffer
			n64, err := rd.WriteTo(&out)
			if err == nil && bytes.Equal(src, out.Bytes()) {
				t.Fatalf("decompression with bad dictionary unexpectedly matched original data (n=%d)", n64)
			}
			if err != nil && !plz4.Lz4Corrupted(err) {
				t.Fatalf("expected corrupted error with bad dictionary, got: %v", err)
			}
			if cerr := rd.Close(); cerr != nil && !plz4.Lz4Corrupted(cerr) {
				t.Fatalf("expected corrupted error or nil on close with bad dictionary, got: %v", cerr)
			}
		})
	}
}

// Ensure that a user-provided dictionary is actually applied when
// compressing data smaller than a single block. We verify this by
// compressing with a known dictionary and then confirming that
// decompression with a bad dictionary either fails with a corrupted
// error or produces mismatched data, while decompression with the
// correct dictionary round-trips.
func TestLz4WriterDictionaryAppliedForSmallPayload(t *testing.T) {
	maybeSkip(t)
	defer testBorrowed(t)

	// Source payload smaller than one 64KiB block.
	src := make([]byte, 4<<10)
	for i := range src {
		// Fill with a deterministic, pseudo-random pattern to avoid
		// trivially compressible data while still having overlap with
		// the dictionary below.
		src[i] = byte(i % 251)
	}

	// Use a prefix of src as the "good" dictionary so that there is
	// meaningful overlap between the dictionary and the payload.
	dictSz := 2 << 10 // 2 KiB prefix
	if dictSz > len(src) {
		t.Fatalf("dictionary size %d larger than src %d", dictSz, len(src))
	}
	goodDict := append([]byte(nil), src[:dictSz]...)

	// Compress using a writer configured with the user dictionary and a
	// block size larger than the payload to ensure we stay within a
	// single block.
	var buf bytes.Buffer
	wr := plz4.NewWriter(&buf,
		plz4.WithDictionary(goodDict),
		plz4.WithBlockSize(plz4.BlockIdx64KB),
		plz4.WithParallel(4),
	)

	if n, err := wr.Write(src); err != nil || n != len(src) {
		t.Fatalf("Fail write: n=%d err=%v", n, err)
	}

	if err := wr.Close(); err != nil {
		t.Fatalf("Fail close: %v", err)
	}

	cmp := buf.Bytes()

	// Verify that decompression with the correct dictionary preserves
	// the original payload.
	decGood := decompressWriteTo(t, cmp, []Option{plz4.WithDictionary(goodDict)})
	if !bytes.Equal(src, decGood.Bytes()) {
		t.Fatalf("round-trip with good dictionary mismatch: got %d bytes, want %d", decGood.Len(), len(src))
	}

	// Now perturb the dictionary and ensure that decompression with this
	// bad dictionary does not successfully round-trip. This demonstrates
	// that the compressed stream actually depends on the user-provided
	// dictionary, even though the payload is smaller than a single block.
	badDict := append([]byte(nil), goodDict...)
	for i := 0; i < len(badDict); i += 16 {
		badDict[i] ^= 0xFF
	}

	rd := plz4.NewReader(bytes.NewReader(cmp), plz4.WithDictionary(badDict))
	var out bytes.Buffer
	n64, err := rd.WriteTo(&out)
	if err == nil && bytes.Equal(src, out.Bytes()) {
		t.Fatalf("decompression with bad dictionary unexpectedly matched original data (n=%d)", n64)
	}
	if err != nil && !plz4.Lz4Corrupted(err) {
		t.Fatalf("expected corrupted error with bad dictionary, got: %v", err)
	}
	if cerr := rd.Close(); cerr != nil && !plz4.Lz4Corrupted(cerr) {
		t.Fatalf("expected corrupted error or nil on close with bad dictionary, got: %v", cerr)
	}
}

func _testLz4WriterWorkerPool(t *testing.T, N, poolSz int) {

	// Create N requests attached to worker pool, they should all finish.
	var (
		wg      sync.WaitGroup
		wp      = wpool.NewWorkerPool(wpool.WithMaxWorkers(poolSz))
		lsrc, _ = LoadSample(t, LargeUncompressed)
	)
	wg.Add(N)
	for range N {
		go func() {
			defer wg.Done()
			discardWrite(t, lsrc, false, plz4.WithWorkerPool(wp), plz4.WithParallel(-1))
		}()
	}

	wg.Wait()
}

// Run with sizeable worker pool.
func TestLz4WriterWorkerpool(t *testing.T) {
	defer testBorrowed(t)
	_testLz4WriterWorkerPool(t, 32, 16)
}

// Add a minimal worker pool; should finish
// despite only two workers available.
func TestLz4WriterWorkerpoolMinimal(t *testing.T) {
	defer testBorrowed(t)
	_testLz4WriterWorkerPool(t, 10, 2)
}

// Randomly swap between Write and ReadFrom APIs, varying write size.
func testIntersperseWriteAndReadFrom(t *testing.T, opts ...Option) {
	lsrc, _ := LoadSample(t, LargeUncompressed)

	var buf bytes.Buffer
	wr := plz4.NewWriter(&buf, opts...)

	offset := 0
	for offset < len(lsrc) {

		var (
			sz   = rand.IntN(16 << 10)
			stop = offset + sz
		)

		if stop > len(lsrc) {
			stop = len(lsrc)
		}

		if rand.IntN(2) == 1 {
			if n, err := wr.Write(lsrc[offset:stop]); err != nil {
				t.Errorf("Fail write: %v", err)
			} else {
				offset += n
			}
		} else {
			rdr := bytes.NewReader(lsrc[offset:stop])
			if n64, err := wr.ReadFrom(rdr); err != nil {
				t.Fatalf("Fail ReadFrom: %v", err)
			} else {
				offset += int(n64)
			}
		}
	}

	if err := wr.Close(); err != nil {
		t.Errorf("Fail close: %v", err)
	}

	uncompressed := decompressWriteTo(t, buf.Bytes(), nil)

	if !bytes.Equal(lsrc, uncompressed.Bytes()) {
		t.Errorf("Result buffers do not match")
	}
}

// Test interspersed write in synchronous mode.
func TestInterspersedWriteSync(t *testing.T) {
	defer testBorrowed(t)
	testIntersperseWriteAndReadFrom(t, plz4.WithParallel(0))
}

// Test interspersed write in async mode.
func TestInterspersedWriteAsync(t *testing.T) {
	defer testBorrowed(t)
	testIntersperseWriteAndReadFrom(t, plz4.WithParallel(1))
}

// Test interspersed write in parallel mode.
func TestInterspersedWriteAuto(t *testing.T) {
	defer testBorrowed(t)
	testIntersperseWriteAndReadFrom(t, plz4.WithParallel(-1))
}

// Skip frames can be written before, after, and between frames.
// Test skip frames along with consecutive frames.
func TestWriteSkipFrame(t *testing.T) {
	defer testBorrowed(t)
	var (
		buf     bytes.Buffer
		lsrc, _ = LoadSample(t, LargeUncompressed)
	)

	// Helper to write a simple frame containing lsrc
	writeFrame := func(wr io.Writer) {
		fw := plz4.NewWriter(wr, plz4.WithParallel(4))
		n64, err := fw.ReadFrom(bytes.NewReader(lsrc))
		if err != nil {
			t.Errorf("fail ReadFrom %v; n64: %d", err, n64)
		}
		if err := fw.Close(); err != nil {
			t.Errorf("Expected nil error on close, got :%v", err)
		}
	}

	// Helper to write a skip frame
	writeSkip := func(wr io.Writer, nibble uint8, skip []byte) {
		n, err := plz4.WriteSkipFrameHeader(wr, nibble, uint32(len(skip)))
		if n != 8 {
			t.Errorf("Expected skip frame header to be 8 bytes, got :%v", n)
		}
		if err != nil {
			t.Fatalf("Fail write skip frame header: %v", err)
		}
		if _, err := wr.Write(skip); err != nil {
			t.Fatalf("Fail write buffer: %v", err)
		}
	}

	// Write initial skip frame
	skip1 := []byte("skip1_block")
	writeSkip(&buf, 1, skip1)

	// Now write a normal frame.
	writeFrame(&buf)

	// Write an intermediate skip frame
	skip2 := []byte("skip2_block")
	writeSkip(&buf, 2, skip2)

	// Write a second frame
	writeFrame(&buf)

	// Write a third consecutive frame, why not?
	writeFrame(&buf)

	// Write third skip frame
	skip3 := []byte("skip3_block")
	writeSkip(&buf, 3, skip3)

	// Write final skip frame
	skip4 := []byte("final_block")
	writeSkip(&buf, 4, skip4)

	// Run it through the read with skip callback on.
	// Validate payload on skip callback, and track callback hits.
	skipHits := make(map[byte]bool)
	skipCallback := func(rdr io.Reader, nibble byte, sz uint32) (int, error) {
		skipBuf := make([]byte, int(sz))
		n, err := io.ReadFull(rdr, skipBuf)
		if err != nil {
			t.Errorf("Cannot read full skip Frame: %v", err)
			return n, err
		}
		skipHits[nibble] = true
		switch nibble {
		case 1:
			if !bytes.Equal(skip1, skipBuf) {
				t.Errorf("Fail compare skip: %v", nibble)
			}
		case 2:
			if !bytes.Equal(skip2, skipBuf) {
				t.Errorf("Fail compare skip: %v", nibble)
			}
		case 3:
			if !bytes.Equal(skip3, skipBuf) {
				t.Errorf("Fail compare skip: %v", nibble)
			}
		case 4:
			if !bytes.Equal(skip4, skipBuf) {
				t.Errorf("Fail compare skip: %v", nibble)
			}
		default:
			t.Errorf("Unknown nibble:%v", nibble)
		}
		return n, nil
	}

	// Run the payload through the decoder with the skipCallback enabled.
	var oBuf bytes.Buffer
	fr := plz4.NewReader(bytes.NewReader(buf.Bytes()), plz4.WithSkipCallback(skipCallback))
	_, err := fr.WriteTo(&oBuf)
	if err != nil {
		t.Fatalf("Fail writeTo: %v", err)
	}

	// Validate that we emitted all the skip block
	for i := byte(1); i <= 4; i++ {
		if _, ok := skipHits[i]; !ok {
			t.Errorf("No skip callback %v", i)
		}
	}

	// Validate buffer size, 3x for three dupes
	if oBuf.Len() != 3*len(lsrc) {
		t.Errorf("Consecutive did not fire")
	}

	// We wrote the same frame thrice;
	// validate that we got the duped frame back.
	testBuf := append([]byte{}, lsrc...)
	testBuf = append(testBuf, lsrc...)
	testBuf = append(testBuf, lsrc...)
	if !bytes.Equal(testBuf, oBuf.Bytes()) {
		t.Errorf("Consecutive writes not equal")
	}
}

// Emulate failures with a writer than fails on
// incrementally increasing N writes.  Exercises
// the error return code paths.
func TestWriteFail(t *testing.T) {
	defer testBorrowed(t)

	var (
		lsrc, _ = LoadSample(t, LargeUncompressed)
	)
	tests := map[string]struct {
		maxSpins int
		readFrom bool
		opts     []Option
	}{
		"defaults": {
			maxSpins: 4,
		},
		"with_hasher": {
			maxSpins: 9,
			opts:     []Option{plz4.WithContentChecksum(true)},
		},
		"smaller_block": {
			maxSpins: 20,
			opts:     []Option{plz4.WithBlockSize(plz4.BlockIdx1MB)},
		},
		"default_sync": {
			maxSpins: 9,
			opts:     []Option{plz4.WithParallel(0)},
		},
		"sync_smaller_block": {
			maxSpins: 20,
			opts:     []Option{plz4.WithParallel(0), plz4.WithBlockSize(plz4.BlockIdx64KB)},
		},
		"defaults_readFrom": {
			readFrom: true,
			maxSpins: 9,
		},
		"with_hasher_readFrom": {
			readFrom: true,
			maxSpins: 9,
			opts:     []Option{plz4.WithContentChecksum(true)},
		},
		"smaller_block_readFrom": {
			readFrom: true,
			maxSpins: 20,
			opts:     []Option{plz4.WithBlockSize(plz4.BlockIdx1MB)},
		},
		"default_sync_readFrom": {
			readFrom: true,
			maxSpins: 9,
			opts:     []Option{plz4.WithParallel(0)},
		},
		"sync_smaller_block_readFrom": {
			readFrom: true,
			maxSpins: 20,
			opts:     []Option{plz4.WithParallel(0), plz4.WithBlockSize(plz4.BlockIdx64KB)},
		},
	}

	for name, tc := range tests {
		// 'maxSpins' is dependent on block size and sample size,
		// so is a bit fragile if sample size changes.

		for i := 1; i <= tc.maxSpins; i++ {
			t.Run(fmt.Sprintf("%s_fail_%d", name, i), func(t *testing.T) {

				fw := &failWriter{
					err: io.ErrClosedPipe,
					cnt: i,
					wr:  &bytes.Buffer{},
				}

				wr := plz4.NewWriter(fw, tc.opts...)

				var err error
				if tc.readFrom {
					rd := bytes.NewReader(lsrc)
					_, err = wr.ReadFrom(rd)
				} else {
					chunkSz := rand.IntN(8<<20) + 1
					_, err = spinWrite(wr, lsrc, chunkSz, true)
				}

				// Force a flush if error did not come through yet
				if err == nil {
					err = wr.Flush()
				}

				if !errors.Is(err, fw.err) {
					t.Errorf("Expected error on '%v', got '%v'", fw.err, err)
				}

				if err := wr.Close(); err != nil {
					t.Errorf("Expected nil on Close(), got '%v'", err)
				}

				if err := wr.Close(); !errors.Is(err, fw.err) {
					t.Errorf("Expect original error on subsequent Close call, got: %v", err)
				}

				if _, err := wr.Write(nil); !errors.Is(err, fw.err) {
					t.Errorf("Expect original error on any subsequent Write call, got: %v", err)
				}

				if err := wr.Flush(); !errors.Is(err, fw.err) {
					t.Errorf("Expect original error on any subsequent Flush call, got: %v", err)
				}
			})
		}
	}
}

func TestWriteFailReader(t *testing.T) {
	defer testBorrowed(t)

	var (
		lsrc, _ = LoadSample(t, LargeUncompressed)
	)
	tests := map[string]struct {
		maxSpins int
		opts     []Option
	}{
		"defaults": {
			maxSpins: 5,
		},
		"with_hasher": {
			maxSpins: 5,
			opts:     []Option{plz4.WithContentChecksum(true)},
		},
		"default_sync": {
			maxSpins: 5,
			opts:     []Option{plz4.WithParallel(0)},
		},
		"smaller_block": {
			maxSpins: 20,
			opts:     []Option{plz4.WithBlockSize(plz4.BlockIdx64KB)},
		},
	}

	for name, tc := range tests {
		// 'maxSpins' is dependent on block size and sample size,
		// so is a bit fragile if sample size changes.

		for i := 1; i <= tc.maxSpins; i++ {
			t.Run(fmt.Sprintf("%s_fail_%d", name, i), func(t *testing.T) {

				fr := &failReader{
					err: io.ErrShortBuffer,
					cnt: i,
					rd:  bytes.NewReader(lsrc),
				}

				wr := plz4.NewWriter(io.Discard, tc.opts...)

				_, err := wr.ReadFrom(fr)

				if !errors.Is(err, fr.err) {
					t.Errorf("Expected error on '%v', got '%v'", fr.err, err)
				}

				// randomly delay the close to exercise different code paths
				wait := time.Duration(rand.Uint64N(100)) * time.Microsecond
				time.Sleep(time.Duration(wait))

				if err := wr.Close(); err != nil {
					t.Errorf("Expected nil on Close(), got '%v'", err)
				}

				if err := wr.Close(); !errors.Is(err, fr.err) {
					t.Errorf("Expect original error on subsequent Close call, got: %v", err)
				}

				if _, err := wr.Write(nil); !errors.Is(err, fr.err) {
					t.Errorf("Expect original error on any subsequent Write call, got: %v", err)
				}

				if err := wr.Flush(); !errors.Is(err, fr.err) {
					t.Errorf("Expect original error on any subsequent Flush call, got: %v", err)
				}
			})
		}
	}
}

//////////
// Helpers
//////////

func spinWrite(wr plz4.Writer, src []byte, chunkSz int, randFlush bool) (sum int64, err error) {
LOOP:
	for len(src) > 0 {

		sz := chunkSz
		if sz > len(src) {
			sz = len(src)
		}

		var n int
		n, err = wr.Write(src[:sz])
		if err != nil {
			break LOOP
		}

		// Randomly call flush to exercise that code path
		if randFlush && rand.IntN(5) == 0 {
			if err = wr.Flush(); err != nil {
				break LOOP
			}
		}

		sum += int64(n)
		src = src[sz:]
	}
	return
}

// Compress 'src' using Write
func compressWrite(t testing.TB, src []byte, opts []Option, chunkSz int) bytes.Buffer {
	var buf bytes.Buffer
	wr := plz4.NewWriter(&buf, opts...)

	// Iterate across buffer in 4K chunks
	var (
		ssrc = len(src)
	)
	sum, err := spinWrite(wr, src, chunkSz, false)
	if err != nil {
		t.Fatalf("Fail Lz4FrameW.Write(): %v", err)
	}

	if sum != int64(ssrc) {
		t.Errorf("Expected len(src): %v got %v", len(src), ssrc)
	}

	if err := wr.Close(); err != nil {
		t.Errorf("Expected nil error on close, got :%v", err)
	}

	if err := wr.Close(); err != plz4.ErrClosed {
		t.Errorf("Expected ErrClosed on subsequent close, got :%v", err)
	}

	if _, err := wr.Write(nil); err != plz4.ErrClosed {
		t.Errorf("Expected ErrClosed on subsequent Write, got :%v", err)
	}

	if _, err := wr.Write([]byte("notempty")); err != plz4.ErrClosed {
		t.Errorf("Expected ErrClosed on subsequent Write, got :%v", err)
	}

	if err := wr.Flush(); err != plz4.ErrClosed {
		t.Errorf("Expected ErrClosed on subsequent Flush, got :%v", err)
	}

	return buf
}

// Compress 'src' using 'ReadFrom'
func compressReadFrom(t testing.TB, src []byte, opts []Option) bytes.Buffer {
	var buf bytes.Buffer
	wr := plz4.NewWriter(&buf, opts...)

	rd := bytes.NewReader(src)

	n64, err := wr.ReadFrom(rd)

	if err != nil {
		t.Errorf("fail ReadFrom %v; n64: %d", err, n64)
	}

	if n64 != int64(len(src)) {
		t.Errorf("Expected len(src): %v got %v", len(src), n64)
	}

	if err := wr.Close(); err != nil {
		t.Errorf("Expected nil error on close, got :%v", err)
	}

	if err := wr.Close(); err != plz4.ErrClosed {
		t.Errorf("Expected ErrClosed on subsequent close, got :%v", err)
	}

	if _, err := wr.Write(nil); err != plz4.ErrClosed {
		t.Errorf("Expected ErrClosed on subsequent Write, got :%v", err)
	}

	if _, err := wr.Write([]byte("notempty")); err != plz4.ErrClosed {
		t.Errorf("Expected ErrClosed on subsequent Write, got :%v", err)
	}

	if err := wr.Flush(); err != plz4.ErrClosed {
		t.Errorf("Expected ErrClosed on subsequent Flush, got :%v", err)
	}

	return buf
}

// Decompress 'src' using WriteTo
func decompressWriteTo(t testing.TB, src []byte, opts []Option) bytes.Buffer {

	rd := plz4.NewReader(bytes.NewReader(src), opts...)

	var buf bytes.Buffer
	n64, err := rd.WriteTo(&buf)

	if err != nil {
		t.Errorf("fail WriteTo %v; n64: %d", err, n64)
	}

	if n64 != int64(buf.Len()) {
		t.Errorf("Expected len(src): %v got %v", len(src), n64)
	}

	if err := rd.Close(); err != nil {
		t.Errorf("Expected nil error on close, got :%v", err)
	}

	return buf
}

type wrCounter struct {
	cnt int64
}

func (c *wrCounter) Write(data []byte) (int, error) {
	c.cnt += int64(len(data))
	return len(data), nil
}

func discardWrite(b testing.TB, src []byte, randFlush bool, opts ...Option) int64 {
	const chunkSz = 4 << 10

	wrCnt := wrCounter{}
	wr := plz4.NewWriter(&wrCnt, opts...)

	_, err := spinWrite(wr, src, chunkSz, randFlush)
	if err != nil {
		b.Errorf("Fail Lz4FrameW.Write(): %v", err)
	}

	if err := wr.Close(); err != nil {
		b.Errorf("Expected nil error on close, got :%v", err)
	}
	return wrCnt.cnt
}

// Expect 'pos' to contain offsets for all blocks.
// Validate that offsets in source align with
// offset to beginning of block in compressed stream.
func validatePos(t *testing.T, src, dst []byte, pos []int64) {

	var (
		idx    int
		srcOff = 0
		blkSz  = 4 << 20
	)

	for srcOff < len(src) {
		if pos[idx] != int64(srcOff) {
			t.Fatalf("Src offset does not align: %v != %v", pos[idx], srcOff)
		}

		// Use WithReadOffset option to force the decompressor to start at offset
		rd := plz4.NewReader(bytes.NewReader(dst), plz4.WithReadOffset(pos[idx+1]))
		blk := make([]byte, blkSz)
		n, err := rd.Read(blk)
		if err != nil {
			t.Fatalf("Unexpected read error on offset %d,%d,%d: %v", idx, srcOff, pos[idx+1], err)
		}

		srcStop := srcOff + blkSz
		if srcStop > len(src) {
			srcStop = len(src)
		}

		if !bytes.Equal(src[srcOff:srcStop], blk[:n]) {
			t.Fatalf("Blocks don't line up at: %v, %v, %v, %v", idx, srcOff, pos[idx+1], n)
		}

		idx += 2
		srcOff += blkSz

		if err := rd.Close(); err != nil {
			t.Fatalf("Did not expect error on block read: %v", err)
		}
	}
}

type failWriter struct {
	wr  io.Writer
	cnt int
	err error
}

func (wr *failWriter) Write(data []byte) (int, error) {
	wr.cnt -= 1
	if wr.cnt == 0 {
		return 0, wr.err
	}
	return wr.wr.Write(data)
}
