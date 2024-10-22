package header

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/prequel-dev/plz4/internal/pkg/descriptor"
	"github.com/prequel-dev/plz4/internal/pkg/opts"
)

// Validate write on various header flags/switches
func TestWrite(t *testing.T) {

	var (
		csz    = uint64(11)
		dictId = uint32(6789)
	)

	tests := map[string]struct {
		opts opts.OptsT
		hdr  []byte
	}{
		"bsz_4M": {
			opts: opts.OptsT{BlockSizeIdx: descriptor.BlockIdx4MB},
			hdr:  []byte{0x60, 0x70, 0x73},
		},
		"bsz_1M": {
			opts: opts.OptsT{BlockSizeIdx: descriptor.BlockIdx1MB},
			hdr:  []byte{0x60, 0x60, 0x51},
		},
		"bsz_256KB": {
			opts: opts.OptsT{BlockSizeIdx: descriptor.BlockIdx256KB},
			hdr:  []byte{0x60, 0x50, 0xfb},
		},
		"bsz_64KB": {
			opts: opts.OptsT{BlockSizeIdx: descriptor.BlockIdx64KB},
			hdr:  []byte{0x60, 0x40, 0x82},
		},
		"linked": {
			opts: opts.OptsT{
				BlockSizeIdx: descriptor.BlockIdx4MB,
				BlockLinked:  true,
			},
			hdr: []byte{0x40, 0x70, 0xDF},
		},
		"block_checksum": {
			opts: opts.OptsT{
				BlockSizeIdx:  descriptor.BlockIdx4MB,
				BlockChecksum: true,
			},
			hdr: []byte{0x70, 0x70, 0x72},
		},
		"content_checksum": {
			opts: opts.OptsT{
				BlockSizeIdx:    descriptor.BlockIdx4MB,
				ContentChecksum: true,
			},
			hdr: []byte{0x64, 0x70, 0xb9},
		},
		"content_size": {
			opts: opts.OptsT{
				BlockSizeIdx: descriptor.BlockIdx4MB,
				ContentSz:    &csz,
			},
			hdr: []byte{0x68, 0x70, 0x0B, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x38},
		},
		"dict_id": {
			opts: opts.OptsT{
				BlockSizeIdx: descriptor.BlockIdx4MB,
				DictionaryId: &dictId,
			},
			hdr: []byte{0x61, 0x70, 0x85, 0x1A, 0x00, 0x00, 0xaf},
		},
		"dict_id + content_size": {
			opts: opts.OptsT{
				BlockSizeIdx: descriptor.BlockIdx4MB,
				DictionaryId: &dictId,
				ContentSz:    &csz,
			},
			hdr: []byte{0x69, 0x70, 0x0B, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x00, 0x85, 0x1A, 0x00, 0x00, 0xe2},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			tc.hdr = append([]byte{0x04, 0x22, 0x4d, 0x18}, tc.hdr...)

			var dst bytes.Buffer
			n, err := WriteHeader(&dst, &tc.opts)

			if err != nil {
				t.Fatalf("Expected no error: %v", err)
			}

			if n != len(tc.hdr) {
				t.Errorf("Len mismatch want: %v got: %v", len(tc.hdr), n)
			}

			if !bytes.Equal(tc.hdr, dst.Bytes()) {
				t.Errorf("Written buffer does not match: %x", dst.Bytes()[4:])
			}
		})
	}
}

type fakeWr struct {
	off int
	err error
}

func (fw *fakeWr) Write([]byte) (int, error) {
	return fw.off, fw.err
}

// WriteHeader should return error if underlying writer fails.
func TestWriteError(t *testing.T) {
	var (
		wr     = fakeWr{off: 3, err: io.ErrClosedPipe}
		opts   = opts.OptsT{BlockSizeIdx: descriptor.BlockIdx4MB}
		n, err = WriteHeader(&wr, &opts)
	)

	if n != wr.off {
		t.Errorf("Expected offset alignment, got :%v", n)
	}

	if !errors.Is(err, wr.err) {
		t.Errorf("Expected error alignment, got :%v", err)
	}
}
