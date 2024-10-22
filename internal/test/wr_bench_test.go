package test

import (
	"bytes"
	"io"
	"testing"

	"github.com/prequel-dev/plz4"
)

func BenchmarkWriteSync(b *testing.B)     { benchmarkWrite(b, 0, plz4.WithParallel(0)) }
func BenchmarkWriteAsync(b *testing.B)    { benchmarkWrite(b, 0, plz4.WithParallel(1)) }
func BenchmarkWriteAuto(b *testing.B)     { benchmarkWrite(b, 0, plz4.WithParallel(-1)) }
func BenchmarkReadFromSync(b *testing.B)  { benchmarkReadFrom(b, 0, plz4.WithParallel(0)) }
func BenchmarkReadFromAsync(b *testing.B) { benchmarkReadFrom(b, 0, plz4.WithParallel(1)) }
func BenchmarkReadFromAuto(b *testing.B)  { benchmarkReadFrom(b, 0, plz4.WithParallel(-1)) }
func BenchmarkWriteSyncNoContent(b *testing.B) {
	benchmarkWrite(b, 0, plz4.WithParallel(0), plz4.WithContentChecksum(false))
}
func BenchmarkWriteAsyncNoContent(b *testing.B) {
	benchmarkWrite(b, 0, plz4.WithParallel(1), plz4.WithContentChecksum(false))
}
func BenchmarkWriteAutoNoContent(b *testing.B) {
	benchmarkWrite(b, 0, plz4.WithParallel(-1), plz4.WithContentChecksum(false))
}
func BenchmarkReadFromSyncNoContent(b *testing.B) {
	benchmarkReadFrom(b, 0, plz4.WithParallel(0), plz4.WithContentChecksum(false))
}
func BenchmarkReadFromAsyncNoContent(b *testing.B) {
	benchmarkReadFrom(b, 0, plz4.WithParallel(1), plz4.WithContentChecksum(false))
}
func BenchmarkReadFromAutoNoContent(b *testing.B) {
	benchmarkReadFrom(b, 0, plz4.WithParallel(-1), plz4.WithContentChecksum(false))
}
func BenchmarkWriteSync64K(b *testing.B) {
	benchmarkWrite(b, 0, plz4.WithParallel(0), plz4.WithBlockSize(plz4.BlockIdx64KB))
}
func BenchmarkWriteAsync64K(b *testing.B) {
	benchmarkWrite(b, 0, plz4.WithParallel(1), plz4.WithBlockSize(plz4.BlockIdx64KB))
}
func BenchmarkWriteAuto64K(b *testing.B) {
	benchmarkWrite(b, 0, plz4.WithParallel(-1), plz4.WithBlockSize(plz4.BlockIdx64KB))
}
func BenchmarkReadFromSync64K(b *testing.B) {
	benchmarkReadFrom(b, 0, plz4.WithParallel(0), plz4.WithBlockSize(plz4.BlockIdx64KB))
}
func BenchmarkReadFromAsync64K(b *testing.B) {
	benchmarkReadFrom(b, 0, plz4.WithParallel(1), plz4.WithBlockSize(plz4.BlockIdx64KB))
}
func BenchmarkReadFromAuto64K(b *testing.B) {
	benchmarkReadFrom(b, 0, plz4.WithParallel(-1), plz4.WithBlockSize(plz4.BlockIdx64KB))
}
func BenchmarkReadFromSync64KLevel3(b *testing.B) {
	benchmarkReadFrom(b, 0, plz4.WithParallel(0), plz4.WithBlockSize(plz4.BlockIdx64KB), plz4.WithLevel(3))
}
func BenchmarkReadFromAsync64KLevel3(b *testing.B) {
	benchmarkReadFrom(b, 0, plz4.WithParallel(1), plz4.WithBlockSize(plz4.BlockIdx64KB), plz4.WithLevel(3))
}
func BenchmarkReadFromAuto64KLevel3(b *testing.B) {
	benchmarkReadFrom(b, 0, plz4.WithParallel(-1), plz4.WithBlockSize(plz4.BlockIdx64KB), plz4.WithLevel(3))
}
func BenchmarkReadFromSync4MLevel3(b *testing.B) {
	benchmarkReadFrom(b, 0, plz4.WithParallel(0), plz4.WithBlockSize(plz4.BlockIdx4MB), plz4.WithLevel(3))
}
func BenchmarkReadFromAsync4MLevel3(b *testing.B) {
	benchmarkReadFrom(b, 0, plz4.WithParallel(1), plz4.WithBlockSize(plz4.BlockIdx4MB), plz4.WithLevel(3))
}
func BenchmarkReadFromAuto4MLevel3(b *testing.B) {
	benchmarkReadFrom(b, 0, plz4.WithParallel(-1), plz4.WithBlockSize(plz4.BlockIdx4MB), plz4.WithLevel(3))
}
func BenchmarkReadFromAuto4MLevel3NoContent(b *testing.B) {
	benchmarkReadFrom(b, 0, plz4.WithParallel(-1), plz4.WithBlockSize(plz4.BlockIdx4MB), plz4.WithLevel(0), plz4.WithContentChecksum(false))
}

const smallData = 64 << 10 // 64 KB

func BenchmarkSmallWriteSync(b *testing.B)     { benchmarkWrite(b, smallData, plz4.WithParallel(0)) }
func BenchmarkSmallWriteAsync(b *testing.B)    { benchmarkWrite(b, smallData, plz4.WithParallel(1)) }
func BenchmarkSmallWriteAuto(b *testing.B)     { benchmarkWrite(b, smallData, plz4.WithParallel(-1)) }
func BenchmarkSmallReadFromSync(b *testing.B)  { benchmarkReadFrom(b, smallData, plz4.WithParallel(0)) }
func BenchmarkSmallReadFromAsync(b *testing.B) { benchmarkReadFrom(b, smallData, plz4.WithParallel(1)) }
func BenchmarkSmallReadFromAuto(b *testing.B)  { benchmarkReadFrom(b, smallData, plz4.WithParallel(-1)) }
func BenchmarkSmallWriteSyncNoContent(b *testing.B) {
	benchmarkWrite(b, smallData, plz4.WithParallel(0), plz4.WithContentChecksum(false))
}
func BenchmarkSmallWriteAsyncNoContent(b *testing.B) {
	benchmarkWrite(b, smallData, plz4.WithParallel(1), plz4.WithContentChecksum(false))
}
func BenchmarkSmallWriteAutoNoContent(b *testing.B) {
	benchmarkWrite(b, smallData, plz4.WithParallel(-1), plz4.WithContentChecksum(false))
}
func BenchmarkSmallReadFromSyncNoContent(b *testing.B) {
	benchmarkReadFrom(b, smallData, plz4.WithParallel(0), plz4.WithContentChecksum(false))
}
func BenchmarkSmallReadFromAsyncNoContent(b *testing.B) {
	benchmarkReadFrom(b, smallData, plz4.WithParallel(1), plz4.WithContentChecksum(false))
}
func BenchmarkSmallReadFromAutoNoContent(b *testing.B) {
	benchmarkReadFrom(b, smallData, plz4.WithParallel(-1), plz4.WithContentChecksum(false))
}
func BenchmarkSmallWriteSync64K(b *testing.B) {
	benchmarkWrite(b, smallData, plz4.WithParallel(0), plz4.WithBlockSize(plz4.BlockIdx64KB))
}
func BenchmarkSmallWriteAsync64K(b *testing.B) {
	benchmarkWrite(b, smallData, plz4.WithParallel(1), plz4.WithBlockSize(plz4.BlockIdx64KB))
}
func BenchmarkSmallWriteAuto64K(b *testing.B) {
	benchmarkWrite(b, smallData, plz4.WithParallel(-1), plz4.WithBlockSize(plz4.BlockIdx64KB))
}
func BenchmarkSmallReadFromSync64K(b *testing.B) {
	benchmarkReadFrom(b, smallData, plz4.WithParallel(0), plz4.WithBlockSize(plz4.BlockIdx64KB))
}
func BenchmarkSmallReadFromAsync64K(b *testing.B) {
	benchmarkReadFrom(b, smallData, plz4.WithParallel(1), plz4.WithBlockSize(plz4.BlockIdx64KB))
}
func BenchmarkSmallReadFromAuto64K(b *testing.B) {
	benchmarkReadFrom(b, smallData, plz4.WithParallel(-1), plz4.WithBlockSize(plz4.BlockIdx64KB))
}

func BenchmarkSmallReadFromSync64KLevel3(b *testing.B) {
	benchmarkReadFrom(b, smallData, plz4.WithParallel(0), plz4.WithBlockSize(plz4.BlockIdx64KB), plz4.WithLevel(3))
}
func BenchmarkSmallReadFromAsync64KLevel3(b *testing.B) {
	benchmarkReadFrom(b, smallData, plz4.WithParallel(1), plz4.WithBlockSize(plz4.BlockIdx64KB), plz4.WithLevel(3))
}
func BenchmarkSmallReadFromAuto64KLevel3(b *testing.B) {
	benchmarkReadFrom(b, smallData, plz4.WithParallel(-1), plz4.WithBlockSize(plz4.BlockIdx64KB), plz4.WithLevel(3))
}
func BenchmarkSmallReadFromSync4MLevel3(b *testing.B) {
	benchmarkReadFrom(b, smallData, plz4.WithParallel(0), plz4.WithBlockSize(plz4.BlockIdx4MB), plz4.WithLevel(3))
}
func BenchmarkSmallReadFromAsync4MLevel3(b *testing.B) {
	benchmarkReadFrom(b, smallData, plz4.WithParallel(1), plz4.WithBlockSize(plz4.BlockIdx4MB), plz4.WithLevel(3))
}
func BenchmarkSmallReadFromAuto4MLevel3(b *testing.B) {
	benchmarkReadFrom(b, smallData, plz4.WithParallel(-1), plz4.WithBlockSize(plz4.BlockIdx4MB), plz4.WithLevel(3))
}
func BenchmarkSmallReadFromAuto4MLevel3NoContent(b *testing.B) {
	benchmarkReadFrom(b, smallData, plz4.WithParallel(-1), plz4.WithBlockSize(plz4.BlockIdx4MB), plz4.WithLevel(3), plz4.WithContentChecksum(false))
}

func BenchmarkMonsterReadFromAuto4MLevel3(b *testing.B) {
	monster, _ := LoadSample(b, Monster)
	benchmarkReadFromWithSrc(b, monster, plz4.WithParallel(-1), plz4.WithBlockSize(plz4.BlockIdx4MB), plz4.WithLevel(3))
}
func BenchmarkMonsterReadFromAuto4MLevel3NoContent(b *testing.B) {
	monster, _ := LoadSample(b, Monster)
	benchmarkReadFromWithSrc(b, monster, plz4.WithParallel(-1), plz4.WithBlockSize(plz4.BlockIdx4MB), plz4.WithLevel(3), plz4.WithContentChecksum(false))
}

func BenchmarkMonsterReadFromAuto64KBLevel3(b *testing.B) {
	monster, _ := LoadSample(b, Monster)
	benchmarkReadFromWithSrc(b, monster, plz4.WithParallel(-1), plz4.WithBlockSize(plz4.BlockIdx64KB), plz4.WithLevel(3))
}
func BenchmarkMonsterReadFromAuto64KBLevel3NoContent(b *testing.B) {
	monster, _ := LoadSample(b, Monster)
	benchmarkReadFromWithSrc(b, monster, plz4.WithParallel(-1), plz4.WithBlockSize(plz4.BlockIdx64KB), plz4.WithLevel(3), plz4.WithContentChecksum(false))
}

func BenchmarkUncompressable(b *testing.B) {
	monster, _ := LoadSample(b, Uncompressable)
	benchmarkReadFromWithSrc(b, monster, plz4.WithParallel(0), plz4.WithLevel(3), plz4.WithContentChecksum(false))
}

func benchmarkWrite(b *testing.B, sz int, opts ...Option) {
	lsrc, _ := LoadSample(b, LargeUncompressed)
	if sz != 0 {
		lsrc = lsrc[:sz]
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sz := discardWrite(b, lsrc, opts...)
		b.ReportMetric(float64(sz)/float64(len(lsrc))*100.0, "ratio")
	}
}

func discardReadFrom(b testing.TB, rd io.Reader, opts ...Option) int64 {

	wrCnt := wrCounter{}
	wr := plz4.NewWriter(&wrCnt, opts...)

	_, err := wr.ReadFrom(rd)

	if err != nil {
		b.Fatalf("Fail Lz4FrameW.ReadFrom(): %v", err)
	}

	if err := wr.Close(); err != nil {
		b.Errorf("Expected nil error on close, got :%v", err)
	}
	return wrCnt.cnt
}

func benchmarkReadFrom(b *testing.B, sz int, opts ...Option) {
	lsrc, _ := LoadSample(b, LargeUncompressed)
	if sz != 0 {
		lsrc = lsrc[:sz]
	}
	benchmarkReadFromWithSrc(b, lsrc, opts...)
}

func benchmarkReadFromWithSrc(b *testing.B, src []byte, opts ...Option) {
	var (
		rd = bytes.NewReader(src)
	)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rd.Reset(src)
		sz := discardReadFrom(b, rd, opts...)
		b.ReportMetric(float64(sz)/float64(len(src))*100.0, "ratio")
	}
}
