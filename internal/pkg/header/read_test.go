package header

import (
	"bytes"
	"errors"
	"io"
	"math"
	"testing"

	"github.com/prequel-dev/plz4/internal/pkg/zerr"
)

var (
	hdrLen   = 19
	theWorks = []byte{0x04, 0x22, 0x4d, 0x18, 0x7d, 0x40, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0f, 0x07, 0x00, 0x00, 0x80, 0x74, 0x65, 0x73, 0x74, 0x79, 0x0a, 0x0a, 0xb3, 0x89, 0x63, 0xba, 0x00, 0x00, 0x00, 0x00, 0xb3, 0x89, 0x63, 0xba}
)

func TestReadOK(t *testing.T) {
	// Validate input
	n, hdr, err := ReadHeader(bytes.NewReader(theWorks), nil)
	switch {
	case n != hdrLen:
		t.Errorf("Fail header length: %v", n)
	case err != nil:
		t.Errorf("Expected clean input: %v", err)
	case hdr.Sz != int64(hdrLen):
		t.Errorf("Fail header length: %v", hdr.Sz)
	case hdr.ContentSz != 7:
		t.Errorf("Fail context size: %v", hdr.ContentSz)
	case hdr.DictId != 0:
		t.Errorf("Expect zero DictId")
	case !hdr.Flags.DictId():
		t.Errorf("Expect DictId")
	case !hdr.Flags.ContentSize():
		t.Errorf("Expect ContentSize")
	case !hdr.Flags.ContentChecksum():
		t.Errorf("Expect ContentChecksum")
	case !hdr.Flags.BlockChecksum():
		t.Errorf("Expect BlockChecksum")
	}
}

func TestDescriptorFlags(t *testing.T) {

	tests := map[string]struct {
		err      error
		mfunc    func(d []byte)
		expected int
		corrupt  bool
	}{
		"magic": {
			err:      zerr.ErrMagic,
			mfunc:    func(d []byte) { d[1] = 'x' },
			corrupt:  true,
			expected: 7,
		},
		"version": {
			err:      zerr.ErrVersion,
			mfunc:    func(d []byte) { d[4] |= 0x80 },
			corrupt:  false,
			expected: 7,
		},
		"reserved": {
			err:      zerr.ErrReserveBitSet,
			mfunc:    func(d []byte) { d[4] |= 0x02 },
			corrupt:  true,
			expected: 7,
		},
		"bd_reserved0": {
			err:      zerr.ErrBlockDescriptor,
			mfunc:    func(d []byte) { d[5] |= 0x01 },
			corrupt:  true,
			expected: 7,
		},
		"bd_reserved1": {
			err:      zerr.ErrBlockDescriptor,
			mfunc:    func(d []byte) { d[5] |= 0x02 },
			corrupt:  true,
			expected: 7,
		},
		"bd_reserved2": {
			err:      zerr.ErrBlockDescriptor,
			mfunc:    func(d []byte) { d[5] |= 0x04 },
			corrupt:  true,
			expected: 7,
		},
		"bd_reserved3": {
			err:      zerr.ErrBlockDescriptor,
			mfunc:    func(d []byte) { d[5] |= 0x08 },
			corrupt:  true,
			expected: 7,
		},
		"bd_reserved7": {
			err:      zerr.ErrBlockDescriptor,
			mfunc:    func(d []byte) { d[5] |= 0x80 },
			corrupt:  true,
			expected: 7,
		},
		"bd_range0": {
			err:      zerr.ErrBlockDescriptor,
			mfunc:    func(d []byte) { d[5] = 0 },
			corrupt:  true,
			expected: 7,
		},
		"bd_range1": {
			err:      zerr.ErrBlockDescriptor,
			mfunc:    func(d []byte) { d[5] = 0x10 },
			corrupt:  true,
			expected: 7,
		},
		"bd_range2": {
			err:      zerr.ErrBlockDescriptor,
			mfunc:    func(d []byte) { d[5] = 0x20 },
			corrupt:  true,
			expected: 7,
		},
		"bd_range3": {
			err:      zerr.ErrBlockDescriptor,
			mfunc:    func(d []byte) { d[5] = 0x30 },
			corrupt:  true,
			expected: 7,
		},
		"bad_crc": {
			err:      zerr.ErrHeaderHash,
			mfunc:    func(d []byte) { d[18] = d[18] + 1 },
			corrupt:  true,
			expected: 19,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			buf.Write(theWorks)
			tc.mfunc(buf.Bytes())

			n, _, err := ReadHeader(&buf, nil)

			if tc.corrupt && !errors.Is(err, zerr.ErrCorrupted) || !tc.corrupt && errors.Is(err, zerr.ErrCorrupted) {
				t.Errorf("Expected corrupted %v, got:%v", tc.corrupt, err)
			}

			if n != tc.expected {
				t.Errorf("Expected %d read, got %v: ", tc.expected, n)
			}
		})
	}
}

func TestHeaderShortRead(t *testing.T) {

	for i := 1; i < hdrLen; i++ {
		data := theWorks[:hdrLen-i]
		_, _, err := ReadHeader(bytes.NewReader(data), nil)
		if !errors.Is(err, zerr.ErrHeaderRead) {
			t.Errorf("Expected header read error at %d: %v", i, err)
		}
	}
}

// Validate the content size is correctly parsed on read.
func TestContentSizeHeader(t *testing.T) {

	makeSz := func(sz uint64) *uint64 {
		return &sz
	}
	tests := map[string]struct {
		sz  *uint64
		err error
		hdr []byte
	}{
		"unset": {
			sz:  nil,
			hdr: []byte{0x60, 0x40, 0x82},
		},
		"content_set_no_data": {
			sz:  nil,
			err: io.EOF, // Not enough data
			hdr: []byte{0x68, 0x40, 0x82},
		},
		"content_set_enough_data_but_corrupted": {
			sz:  nil,
			err: zerr.ErrHeaderHash, // enough data, but hash doesn't align
			hdr: []byte{0x68, 0x40, 0x82, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x04},
		},
		"content_set_zero": {
			sz:  makeSz(0),
			hdr: []byte{0x68, 0x40, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x05},
		},
		"content_set_one": {
			sz:  makeSz(1),
			hdr: []byte{0x68, 0x40, 0x01, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2C, 0x01, 0x0, 0x0, 0x80, 0x0},
		},
		"content_set_max_uint32": {
			sz:  makeSz(math.MaxUint32),
			hdr: []byte{0x68, 0x40, 0xFF, 0xFF, 0xFF, 0xFF, 0x0, 0x0, 0x0, 0x0, 94},
		},
		"content_set_max_uint64-1": {
			sz:  makeSz(math.MaxUint64 - 1),
			hdr: []byte{0x68, 0x40, 0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 134},
		},
		"content_set_max_uint64": {
			sz:  makeSz(math.MaxUint64),
			hdr: []byte{0x68, 0x40, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 167},
		},
		"content_set_zero_with_dictId": {
			sz:  makeSz(math.MaxUint32),
			hdr: []byte{0x69, 0x40, 0xFF, 0xFF, 0xFF, 0xFF, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x57},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var src bytes.Buffer
			src.Write(lz4FrameMagic[:])
			src.Write(tc.hdr)

			_, hdr, err := ReadHeader(&src, nil)

			if !errors.Is(err, tc.err) {
				t.Errorf("Expected '%v' fail, got:'%v' ", tc.err, err)
				return
			}

			// Flags are invalid on error
			if err != nil {
				return
			}

			switch {
			case tc.sz == nil:
				if hdr.Flags.ContentSize() || hdr.ContentSz != 0 {
					t.Errorf("Expect content size unset")
				}
			case !hdr.Flags.ContentSize():
				t.Errorf("Expect content size set")
			case hdr.ContentSz != *tc.sz:
				t.Errorf("Expect content size '%v' got '%v", *tc.sz, hdr.ContentSz)
			}
		})
	}
}
