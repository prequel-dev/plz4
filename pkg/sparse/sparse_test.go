package sparse

import (
	"bytes"
	crand "crypto/rand"
	"errors"
	"io"
	"math/rand/v2"
	"os"
	"testing"
)

type preallocWriter struct {
	data []byte
	pos  int
}

func NewPreallocWriter(sz int) *preallocWriter {
	return &preallocWriter{
		data: make([]byte, sz),
	}
}

func (w *preallocWriter) Write(data []byte) (int, error) {
	sz := len(data)
	if w.pos+sz > len(w.data) {
		return 0, errors.New("buffer overflwo")
	}
	copy(w.data[w.pos:], data)
	w.pos += sz
	return sz, nil
}

func (w *preallocWriter) Seek(offset int64, whence int) (int64, error) {
	if whence != io.SeekCurrent {
		return 0, errors.New("unsuppported seek")
	}
	if w.pos+int(offset) > len(w.data) {
		return 0, errors.New("cannot seek past fixed end")
	}
	w.pos += int(offset)
	return int64(w.pos), nil
}

func TestSparseAllZeros(t *testing.T) {

	src := make([]byte, 16<<20)

	dst := NewPreallocWriter(len(src))
	wr := NewWriter(dst)

	n, err := io.Copy(wr, bytes.NewReader(src))
	if err != nil {
		t.Errorf("Fail copy: %v", err)

	}

	if n != int64(len(src)) {
		t.Errorf("Expected %v written, got %v", n, len(src))
	}

	if err := wr.Close(); err != nil {
		t.Errorf("Fail close writer: %v", err)
	}

	if err != nil {
		t.Fatalf("Could not read data from temp file: %v", err)
	}

	if !bytes.Equal(src, dst.data) {
		t.Errorf("Bytes don't match")
		os.WriteFile("/tmp/bad.bin", src, 0644)
		os.WriteFile("/tmp/out.bin", dst.data, 0644)
	}
}

func TestSparseWrite(t *testing.T) {

	src := createRandomSparseFile(t, 2<<20, 16<<10)

	dst := NewPreallocWriter(len(src))
	wr := NewWriter(dst)

	n, err := io.Copy(wr, bytes.NewReader(src))
	if err != nil {
		t.Errorf("Fail copy: %v", err)

	}

	if n != int64(len(src)) {
		t.Errorf("Expected %v written, got %v", n, len(src))
	}

	if err := wr.Close(); err != nil {
		t.Errorf("Fail close writer: %v", err)
	}

	if err != nil {
		t.Fatalf("Could not read data from temp file: %v", err)
	}

	if !bytes.Equal(src, dst.data) {
		t.Errorf("Bytes don't match")
		os.WriteFile("/tmp/bad.bin", src, 0644)
		os.WriteFile("/tmp/out.bin", dst.data, 0644)
	}
}

func TestSparseReadFrom(t *testing.T) {

	src := createRandomSparseFile(t, 2<<20, 16<<10)

	dst := NewPreallocWriter(len(src))
	wr := NewWriter(dst)

	n, err := wr.ReadFrom(bytes.NewReader(src))
	if err != nil {
		t.Errorf("Fail copy: %v", err)

	}

	if n != int64(len(src)) {
		t.Errorf("Expected %v written, got %v", n, len(src))
	}

	if err := wr.Close(); err != nil {
		t.Errorf("Fail close writer: %v", err)
	}

	if err != nil {
		t.Fatalf("Could not read data from temp file: %v", err)
	}

	if !bytes.Equal(src, dst.data) {
		t.Errorf("Bytes don't match")
		os.WriteFile("/tmp/bad.bin", src, 0644)
		os.WriteFile("/tmp/out.bin", dst.data, 0644)
	}
}

func Benchmark_SparseCopy(b *testing.B) {

	src := createFixedSizeSparseFile(b, 64<<20, 16<<10)
	rdr := bytes.NewReader(src)
	dst := NewPreallocWriter(len(src))

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		dst.pos = 0
		rdr.Reset(src)
		wr := NewWriter(dst)
		_, err := io.Copy(wr, rdr)
		if err != nil {
			b.Errorf("Fail copy: %v", err)
		}
		if err := wr.Close(); err != nil {
			b.Errorf("Fail close writer: %v", err)
		}
	}
}

func BenchmarkZeros(b *testing.B) {
	src := make([]byte, 64<<10)
	rdr := bytes.NewReader(src)
	dst := NewPreallocWriter(len(src))

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		dst.pos = 0
		rdr.Reset(src)
		wr := NewWriter(dst)
		_, err := io.Copy(wr, rdr)
		if err != nil {
			b.Errorf("Fail copy: %v", err)
		}
		if err := wr.Close(); err != nil {
			b.Errorf("Fail close writer: %v", err)
		}
	}
}

func Benchmark_SparseReadFrom(b *testing.B) {

	src := createFixedSizeSparseFile(b, 64<<20, 16<<10)
	rdr := bytes.NewReader(src)
	dst := NewPreallocWriter(len(src))

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		dst.pos = 0
		rdr.Reset(src)
		wr := NewWriter(dst)
		_, err := wr.ReadFrom(rdr)
		if err != nil {
			b.Errorf("Fail copy: %v", err)
		}
		if err := wr.Close(); err != nil {
			b.Errorf("Fail close writer: %v", err)
		}
	}

}

func createRandomSparseFile(t testing.TB, maxSz, maxBlk int64) []byte {
	sz := rand.Int64N(maxSz) + 1
	return createFixedSizeSparseFile(t, sz, maxBlk)
}

func createFixedSizeSparseFile(t testing.TB, sz, maxBlk int64) []byte {

	var buf bytes.Buffer

	for sz > 0 {
		blkSz := rand.Int64N(maxBlk) + 1
		if blkSz > sz {
			blkSz = sz
		}
		blk := make([]byte, blkSz)

		if rand.IntN(2) == 1 {
			if _, err := crand.Read(blk); err != nil {
				t.Fatal(err)
			}
		}
		buf.Write(blk)
		sz -= blkSz
	}

	return buf.Bytes()
}
