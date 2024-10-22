package test

import (
	"bytes"
	"io"
	"testing"

	"github.com/prequel-dev/plz4"
)

func BenchmarkWriteToLz4_4MB(b *testing.B) {
	sample, _ := LoadSample(b, Lz4_4MB)
	benchmarkWriteTo(b, sample)
}

func BenchmarkReadLz4_4MB(b *testing.B) {
	sample, _ := LoadSample(b, Lz4_4MB)
	benchmarkRead(b, sample)
}

func benchmarkWriteTo(b *testing.B, sample []byte, opts ...plz4.OptT) {
	var (
		rdr = bytes.NewReader(sample)
	)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rdr.Reset(sample)

		rd := plz4.NewReader(rdr, opts...)

		if _, err := io.Copy(io.Discard, rd); err != nil {
			b.Fatalf("err %v", err)
		}

		if err := rd.Close(); err != nil {
			b.Fatalf("err %v", err)
		}
	}
}

func benchmarkRead(b *testing.B, sample []byte, opts ...Option) {
	var (
		rdr     = bytes.NewReader(sample)
		scratch = make([]byte, 4<<20)
	)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rdr.Reset(sample)

		rd := plz4.NewReader(rdr, opts...)

	LOOP:
		for {
			_, err := io.ReadFull(rd, scratch)
			switch err {
			case nil, io.ErrUnexpectedEOF:
			case io.EOF:
				break LOOP
			default:
				b.Fatalf("Unexpected error: %v", err)

			}
		}

		if err := rd.Close(); err != nil {
			b.Fatalf("err %v", err)
		}
	}
}
