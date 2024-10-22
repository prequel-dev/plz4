package header

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/prequel-dev/plz4/internal/pkg/zerr"
)

// Validate that allowed nibble range writes
// correct skip frame headers.
func TestWriteSkipNibbles(t *testing.T) {
	for i := uint8(0); i < 0xF; i++ {
		name := fmt.Sprintf("nibble_%d", i)
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer

			n, err := WriteSkip(&buf, i, 0)

			if n != 8 {
				t.Errorf("Expected fixed skip hdr size got %v", n)
			}

			if buf.Len() != 8 {
				t.Errorf("Expected writer hdr size got %v", buf.Len())
			}

			if err != nil {
				t.Errorf("Expected nil error on write: %v", err)
			}

			magic := []byte{0x50, 0x2A, 0x4d, 0x18}
			magic[0] += i

			b := buf.Bytes()
			if !bytes.Equal(b[:4], magic) {
				t.Errorf("wrong magic: %x", b[:4])
			}

			if binary.LittleEndian.Uint32(b[4:]) != 0 {
				t.Errorf("wrong sz written: %x", b[4:])
			}
		})
	}
}

// Illegal nibble value should return an error
func TestWriteBadNibble(t *testing.T) {
	for i := uint8(0x10); i < 0xFF; i += 1 {
		n, err := WriteSkip(io.Discard, 0xFF, 0)
		if n != 0 {
			t.Errorf("Expected no bytes written on error: %v", n)
		}
		if !errors.Is(err, zerr.ErrNibble) {
			t.Errorf("Expected bad nibble error: %v", err)
		}
	}
}

// Validate that WriteSkip header handles 0-uint32 size.
func TestWriteSkipSize(t *testing.T) {

	tests := map[string]struct {
		sz uint32
	}{
		"zero": {
			sz: 0,
		},
		"one": {
			sz: 1,
		},
		"max_uint16": {
			sz: math.MaxUint16,
		},
		"max_uint32": {
			sz: math.MaxUint32,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer

			n, err := WriteSkip(&buf, 0, tc.sz)

			if n != 8 {
				t.Errorf("Expected fixed skip hdr size got %v", n)
			}

			if buf.Len() != 8 {
				t.Errorf("Expected writer hdr size got %v", buf.Len())
			}

			if err != nil {
				t.Errorf("Expected nil error on write: %v", err)
			}

			b := buf.Bytes()
			if !bytes.Equal(b[:4], []byte{0x50, 0x2A, 0x4d, 0x18}) {
				t.Errorf("wrong magic: %x", b[:4])
			}

			if binary.LittleEndian.Uint32(b[4:]) != tc.sz {
				t.Errorf("wrong sz written: %x", b[4:])
			}
		})
	}
}

// Invalid magic value should return ErrMagic.
func TestSkipCallbackBadMagic(t *testing.T) {
	rd := bytes.NewReader([]byte{0x60, 0x2A, 0x4d, 0x18, 0xFF, 0xFF, 0xFF})
	n, _, err := ReadHeader(rd, nil)
	if n != lz4MinHeaderSz {
		t.Errorf("Expected minimum header read: %v", n)
	}
	if !errors.Is(err, zerr.ErrMagic) || !errors.Is(err, zerr.ErrCorrupted) {
		t.Errorf("Expected corrupted bad magic: %v", err)
	}
}

// Short reads on header should return ErrHeaderRead
func TestSkipShortRead(t *testing.T) {
	var (
		hdr = []byte{0x50, 0x2A, 0x4d, 0x18, 0x00, 0x00, 0x00, 0x00}
	)

	for i := 1; i <= 4; i++ {
		rd := bytes.NewReader(hdr[:len(hdr)-i])
		n, _, err := ReadHeader(rd, nil)
		if n != len(hdr)-i {
			t.Errorf("Expected n read %d got %d", len(hdr)-i, n)
		}
		if !errors.Is(err, zerr.ErrHeaderRead) {
			t.Errorf("Expected header read err: %v", err)
		}
		if errors.Is(err, zerr.ErrCorrupted) {
			t.Errorf("Short header error should not be corrupted err: %v", err)
		}
	}
}

// Test valid skip callbacks for target size.
// Generate payload and parse in callback.
func TestSkipCallback(t *testing.T) {

	tests := map[string]struct {
		sz uint32
	}{
		"zero": {
			sz: 0,
		},
		"one": {
			sz: 1,
		},
		"max_uint16": {
			sz: math.MaxUint16,
		},
		"max_uint32": {
			sz: math.MaxUint32,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			hdr := []byte{0x50, 0x2A, 0x4d, 0x18}
			hdr = binary.LittleEndian.AppendUint32(hdr, tc.sz)

			var buf bytes.Buffer
			buf.Write(hdr)

			// Generate 'sz' payload
			randomBuffer := make([]byte, tc.sz)
			_, err := rand.Read(randomBuffer)
			if err != nil {
				t.Fatalf("Fail rand read %v", err)
			}

			buf.Write(randomBuffer)

			cb := func(rdr io.Reader, nibble uint8, sz uint32) (int, error) {
				if nibble != 0 {
					t.Errorf("Expected zero nibble: %v", nibble)
				}
				if sz != tc.sz {
					t.Errorf("Expected size '%d' got: %v", tc.sz, sz)
				}
				v, err := io.CopyN(io.Discard, rdr, int64(sz))
				return int(v), err
			}

			n, _, err := ReadHeader(&buf, cb)

			if n != int(tc.sz)+8 {
				t.Errorf("Fail read header size: %v", n)
			}

			if err != zerr.EndMark {
				t.Errorf("Expected error '%v' got '%v'", zerr.EndMark, err)
			}
		})
	}
}

// A skip frame should be consumed properly if no callback provided.
// Demonstrate that in a succession of two frames, the first frame
// data is consumer correctly by ReadHeader, by continuing parsing
// on the subsequent frame.

func TestSkipCallbackNil(t *testing.T) {
	// One skip frame with data, followed by a second with empty data
	data := []byte{0x50, 0x2A, 0x4d, 0x18, 0x2, 0x0, 0x0, 0x0, 0xA, 0xB, 0x50, 0x2A, 0x4d, 0x18, 0x0, 0x0, 0x0, 0x0}

	rd := bytes.NewReader(data)
	n, _, err := ReadHeader(rd, nil)

	if err != zerr.EndMark {
		t.Errorf("Expected nil error got %v", err)
	}

	// First read gets first frame.
	if n != 10 {
		t.Errorf("Wrong data read: %v", n)
	}

	n, _, err = ReadHeader(rd, nil)

	if err != zerr.EndMark {
		t.Errorf("Expected nil error got %v", err)
	}

	// Second read gets second frame.
	if n != 8 {
		t.Errorf("Wrong data read: %v", n)
	}
}

type fakeRead struct {
	data []byte
	err  error
}

func (r *fakeRead) Read(dst []byte) (int, error) {
	n := copy(dst, r.data)
	r.data = r.data[n:]
	if len(r.data) == 0 {
		return n, r.err
	}
	return n, nil
}

// ReadHeader should return error if underlying reader fails.
func TestSkipCallbackNilWithBadReader(t *testing.T) {
	data := []byte{0x50, 0x2A, 0x4d, 0x18, 0x1, 0x0, 0x0, 0x0}

	rd := &fakeRead{data: data, err: io.ErrClosedPipe}

	n, _, err := ReadHeader(rd, nil)

	if !errors.Is(err, zerr.ErrSkip) || !errors.Is(err, rd.err) {
		t.Errorf("Expected skip pipe error got %v", err)
	}

	if n != len(data) {
		t.Errorf("Wrong data read: %v", n)
	}
}

// Read header should return error if callback fails
func TestSkipCallbackReturnsError(t *testing.T) {
	data := []byte{0x50, 0x2A, 0x4d, 0x18, 0x2, 0x0, 0x0, 0x0}

	cb := func(rdr io.Reader, nibble uint8, sz uint32) (int, error) {
		if nibble != 0 {
			t.Errorf("Expected zero nibble: %v", nibble)
		}
		if sz != 2 {
			t.Errorf("Expected size '%d' got: %v", 1, sz)
		}

		// Return nRead as 1 and an error; should pass through
		// on the return from ReadHeader below.
		return 1, io.ErrNoProgress
	}

	n, _, err := ReadHeader(bytes.NewReader(data), cb)

	if !errors.Is(err, zerr.ErrSkip) || !errors.Is(err, io.ErrNoProgress) {
		t.Errorf("Expected skip pipe error got %v", err)
	}

	if n != len(data)+1 {
		t.Errorf("Wrong data read: %v", n)
	}
}
