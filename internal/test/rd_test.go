package test

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/prequel-dev/plz4"
	"github.com/prequel-dev/plz4/internal/pkg/opts"
	"github.com/prequel-dev/plz4/internal/pkg/zerr"
)

var (
	magic       = []byte{0x04, 0x22, 0x4D, 0x18}
	endOfFrames = []byte{0x0, 0x0, 0x0, 0x0}
)

// Validate descriptor flags parsing
func TestDescriptorFlags(t *testing.T) {
	defer testBorrowed(t)

	tests := map[string]struct {
		err     error
		mfunc   func(d []byte)
		corrupt bool
	}{
		"magic": {
			err:     plz4.ErrMagic,
			mfunc:   func(d []byte) { d[1] = 'x' },
			corrupt: true,
		},
		"version": {
			err:     plz4.ErrVersion,
			mfunc:   func(d []byte) { d[4] |= 0x80 },
			corrupt: false,
		},
		"reserved": {
			err:     plz4.ErrReserveBitSet,
			mfunc:   func(d []byte) { d[4] |= 0x02 },
			corrupt: true,
		},
		"bd_reserved0": {
			err:     plz4.ErrBlockDescriptor,
			mfunc:   func(d []byte) { d[5] |= 0x01 },
			corrupt: true,
		},
		"bd_reserved1": {
			err:     plz4.ErrBlockDescriptor,
			mfunc:   func(d []byte) { d[5] |= 0x02 },
			corrupt: true,
		},
		"bd_reserved2": {
			err:     plz4.ErrBlockDescriptor,
			mfunc:   func(d []byte) { d[5] |= 0x04 },
			corrupt: true,
		},
		"bd_reserved3": {
			err:     plz4.ErrBlockDescriptor,
			mfunc:   func(d []byte) { d[5] |= 0x08 },
			corrupt: true,
		},
		"bd_reserved7": {
			err:     plz4.ErrBlockDescriptor,
			mfunc:   func(d []byte) { d[5] |= 0x80 },
			corrupt: true,
		},
		"bd_range0": {
			err:     plz4.ErrBlockDescriptor,
			mfunc:   func(d []byte) { d[5] = 0 },
			corrupt: true,
		},
		"bd_range1": {
			err:     plz4.ErrBlockDescriptor,
			mfunc:   func(d []byte) { d[5] = 0x10 },
			corrupt: true,
		},
		"bd_range2": {
			err:     plz4.ErrBlockDescriptor,
			mfunc:   func(d []byte) { d[5] = 0x20 },
			corrupt: true,
		},
		"bd_range3": {
			err:     plz4.ErrBlockDescriptor,
			mfunc:   func(d []byte) { d[5] = 0x30 },
			corrupt: true,
		},
		"bad_crc": {
			err:     plz4.ErrHeaderHash,
			mfunc:   func(d []byte) { d[6] = d[6] + 1 },
			corrupt: true,
		},
	}

	_, lz4 := generateLz4(t)

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			buf.Write(lz4.Bytes())
			tc.mfunc(buf.Bytes())

			var (
				rd  = plz4.NewReader(&buf)
				dst = make([]byte, 1024)
			)

			n, err := rd.Read(dst)
			if !errors.Is(err, tc.err) {
				t.Errorf("Expected %v fail, got:%v", tc.err, err)
			}

			if tc.corrupt && !plz4.Lz4Corrupted(err) || !tc.corrupt && plz4.Lz4Corrupted(err) {
				t.Errorf("Expected corrupted %v, got:%v", tc.corrupt, err)
			}

			if n != 0 {
				t.Errorf("Expected 0 read, got %v: ", n)
			}
		})
	}
}

// Validate that when enabled, content size is checked.
// When disabled, mismatch content size should be ignored.
func TestContentSizeValidate(t *testing.T) {
	defer testBorrowed(t)

	var (
		ploadSzZero           = []byte{0x68, 0x40, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x05}
		ploadSzOne            = []byte{0x68, 0x40, 0x01, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2C, 0x01, 0x0, 0x0, 0x80, 0x0}
		ploadSzOneWithZeroCnt = []byte{0x68, 0x40, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x05, 0x01, 0x0, 0x0, 0x80, 0x0}
		ploadSzZeroWithOneCnt = []byte{0x68, 0x40, 0x01, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2C}
	)

	tests := map[string]struct {
		opts []plz4.OptT
		data []byte
		err  error
	}{
		"zero_ok": {
			data: ploadSzZero,
		},
		"one_ok": {
			data: ploadSzOne,
		},
		"zero_fail": {
			data: ploadSzOneWithZeroCnt,
			err:  plz4.ErrContentSize,
		},
		"one_fail": {
			data: ploadSzZeroWithOneCnt,
			err:  plz4.ErrContentSize,
		},
		"zero_fail_disabled": {
			opts: []plz4.OptT{plz4.WithContentSizeCheck(false)},
			data: ploadSzOneWithZeroCnt,
		},
		"one_fail_disabled": {
			opts: []plz4.OptT{plz4.WithContentSizeCheck(false)},
			data: ploadSzZeroWithOneCnt,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var src bytes.Buffer
			src.Write(magic)
			src.Write(tc.data)
			src.Write(endOfFrames)

			rd := plz4.NewReader(&src, tc.opts...)
			_, err := rd.WriteTo(io.Discard)

			if !errors.Is(err, tc.err) {
				t.Errorf("Expected %v fail, got:%v", tc.err, err)
				return
			}

			if tc.err != nil && !plz4.Lz4Corrupted(err) {
				t.Errorf("Expected '%v' is corrupted", err)
			}
		})
	}
}

// Test that we are able to skip frames and detect invalid frames.
func TestSkipFrame(t *testing.T) {
	defer testBorrowed(t)

	var (
		errFakeReturn = zerr.WrapCorrupted(errors.New("fake return error"))
		skipEmpty     = []byte{0x50, 0x2A, 0x4D, 0x18, 0x0, 0x0, 0x0, 0x0}
		skipOne       = []byte{0x5F, 0x2A, 0x4D, 0x18, 0x1, 0x0, 0x0, 0x0, 0xF7}
		skipBad       = []byte{0x60, 0x2A, 0x4D, 0x18, 0x1, 0x0, 0x0, 0x0, 0xF7}
		ploadSzZero   = []byte{0x04, 0x22, 0x4D, 0x18, 0x68, 0x40, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x05, 0x0, 0x0, 0x0, 0x0}
	)

	mkData := func(parts ...[]byte) (w []byte) {
		for _, p := range parts {
			w = append(w, p...)
		}
		return
	}

	// Emulate a callback that does not properly read 'sz' bytes
	cbBorked := func(rdr io.Reader, nibble uint8, sz uint32) (int, error) {
		v, err := io.CopyN(io.Discard, rdr, int64(sz-1))
		return int(v), err
	}

	// Emulate a callback that returns an error
	cbReturnErr := func(rdr io.Reader, nibble uint8, sz uint32) (int, error) {
		return 0, errFakeReturn
	}

	tests := map[string]struct {
		data    []byte
		err     error
		cb      opts.SkipCallbackT
		nibbles []uint8
	}{
		"skipBad": {
			data: skipBad,
			err:  plz4.ErrMagic,
		},
		"skipEmptyAlone": {
			data: skipEmpty,
		},
		"skipEmptyBad": {
			data: mkData(skipEmpty, skipBad),
			err:  plz4.ErrMagic,
		},
		"skipEmptyEmpty": {
			data: mkData(skipEmpty, skipEmpty),
		},
		"skipOne": {
			data: skipOne,
		},
		"skipOneOne": {
			data: mkData(skipOne, skipOne),
		},
		"skipOneBad": {
			data: mkData(skipOne, skipBad),
			err:  plz4.ErrMagic,
		},
		"skipEmptyFrame": {
			data: mkData(skipEmpty, ploadSzZero),
		},
		"skipOneFrame": {
			data: mkData(skipOne, ploadSzZero),
		},
		"skipOneFrameOne": {
			data:    mkData(skipOne, ploadSzZero, skipOne),
			nibbles: []uint8{0xF, 0xF},
		},
		"skipOneFrameOneFrameEmpty": {
			data:    mkData(skipOne, ploadSzZero, skipOne, ploadSzZero, skipEmpty),
			nibbles: []uint8{0xF, 0xF, 0x0},
		},
		"skipOneFrameOneFrameBad": {
			data: mkData(skipOne, ploadSzZero, skipOne, ploadSzZero, skipBad),
			err:  plz4.ErrMagic,
		},
		"skipCallbackBroken": {
			data: mkData(skipOne, ploadSzZero),
			cb:   cbBorked,
			err:  plz4.ErrMagic, // didn't read enough; magic won't align
		},
		"skipCallbackReturnsError": {
			data: mkData(skipOne, ploadSzZero),
			cb:   cbReturnErr,
			err:  errFakeReturn,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cb := tc.cb
			var nibbles []uint8
			if cb == nil {
				cb = func(rdr io.Reader, nibble uint8, sz uint32) (int, error) {
					nibbles = append(nibbles, nibble)
					v, err := io.CopyN(io.Discard, rdr, int64(sz))
					return int(v), err
				}
			}

			var (
				src    = bytes.NewReader(tc.data)
				rd     = plz4.NewReader(src, plz4.WithSkipCallback(cb))
				_, err = rd.WriteTo(io.Discard)
			)

			if !errors.Is(err, tc.err) {
				t.Errorf("Expected %v fail, got:%v", tc.err, err)
				return
			}

			if tc.err != nil && !plz4.Lz4Corrupted(err) {
				t.Errorf("Expected '%v' is corrupted", err)
			}

			// Check nibbles if defined
			if tc.nibbles != nil {
				if len(tc.nibbles) != len(nibbles) {
					t.Fatalf("nibble lens wrong %d != %d", len(tc.nibbles), len(nibbles))
				}
				for i, n := range tc.nibbles {
					if nibbles[i] != n {
						t.Errorf("Naughty nibble at %d", i)
					}
				}
			}

			// Bail on error state; subsequent errors not tested here
			if err != nil {
				return
			}
		})
	}
}

// Should process two frame concatenated together;
// Similar to above test except real data.
func TestConcatFrameRead(t *testing.T) {
	defer testBorrowed(t)

	var (
		rdr      bytes.Buffer
		p1, sum1 = LoadSample(t, Lz4_4MB)
		p2, sum2 = LoadSample(t, Lz4_64KB)
		sz1      = lz4Size(t, p1)
		sz2      = lz4Size(t, p2)
	)

	rdr.Write(p1)
	rdr.Write(p2)

	var (
		scan = plz4.NewReader(&rdr)
		wr   bytes.Buffer
	)

	n, err := scan.WriteTo(&wr)
	if err != nil {
		t.Errorf("Expected nil error, got :%v", err)
	}

	if n != int64(sz1+sz2) {
		t.Errorf("n != target: %v != %v", n, int64(sz1+sz2))
	}

	data := wr.Bytes()

	if Sha2sum(data[:sz1]) != sum1 {
		t.Errorf("Sha2 fail on first payload: %v", Sha2sum(data[:sz1]))
	}

	if Sha2sum(data[sz1:]) != sum2 {
		t.Errorf("Sha2 fail on second payload: %v", Sha2sum(data[sz1:]))
	}
}

// Read files compressed with dictionary
func TestReadWithDict(t *testing.T) {
	defer testBorrowed(t)

	var (
		dict, _                  = LoadSample(t, Dict)
		data, sha2               = LoadSample(t, Lz4_IndieWithDict)
		sample_no_dict, snd_sha2 = LoadSample(t, Lz4_ContentCRC)
	)

	tests := map[string]struct {
		data []byte
		sha2 string
		err  error
		opts []plz4.OptT
	}{
		"clean_decode": {
			data: data,
			sha2: sha2,
			opts: []plz4.OptT{plz4.WithDictionary(dict)},
		},
		"sync": {
			data: data,
			sha2: sha2,
			opts: []plz4.OptT{plz4.WithDictionary(dict), plz4.WithParallel(0)},
		},
		"forgot_dictionary": {
			data: data,
			sha2: sha2,
			err:  plz4.ErrDecompress,
		},
		"chopped_dictionary": {
			data: data,
			sha2: sha2,
			opts: []plz4.OptT{plz4.WithDictionary(dict[35000:])},
			err:  plz4.ErrDecompress,
		},
		"unnecessary_dictionary": {
			data: sample_no_dict,
			sha2: snd_sha2,
			opts: []plz4.OptT{plz4.WithDictionary(dict)},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var (
				buf    bytes.Buffer
				rd     = plz4.NewReader(bytes.NewReader(tc.data), tc.opts...)
				_, err = rd.WriteTo(&buf)
			)
			if cerr := rd.Close(); cerr != nil {
				t.Errorf("Expected nil on Close(): %v", err)
			}

			if tc.err != nil || err != nil {
				if !errors.Is(err, tc.err) {
					t.Fatalf("Expected error: %v got %v", tc.err, err)
				}
				return
			}
			ss := Sha2sum(buf.Bytes())
			if ss != tc.sha2 {
				t.Errorf("Hash mismatch: got %s wanted %s", ss, sha2)
			}
		})
	}
}

// Test files that have linked blocks (ie. not independent)
func TestLinked(t *testing.T) {
	defer testBorrowed(t)

	var (
		dict, _      = LoadSample(t, Dict)
		data, sha2   = LoadSample(t, Lz4_Linked)
		ddata, dsha2 = LoadSample(t, Lz4_LinkedWithDict)
	)

	tests := map[string]struct {
		data []byte
		dict []byte
		sha2 string
		err  error
		opts []plz4.OptT
	}{
		"simple_linked": {
			data: data,
			sha2: sha2,
		},
		"simple_linked_sync": {
			data: data,
			sha2: sha2,
			opts: []plz4.OptT{plz4.WithParallel(0)},
		},
		"linked_with_dictionary": {
			data: ddata,
			sha2: dsha2,
			opts: []plz4.OptT{plz4.WithDictionary(dict)},
		},
		"linked_with_dictionary_sync": {
			data: ddata,
			sha2: dsha2,
			opts: []plz4.OptT{plz4.WithDictionary(dict), plz4.WithParallel(0)},
		},
		"linked_forgot_dictionary": {
			data: ddata,
			sha2: dsha2,
			err:  plz4.ErrDecompress,
		},
		"linked_forgot_dictionary_sync": {
			data: ddata,
			sha2: dsha2,
			err:  plz4.ErrDecompress,
			opts: []plz4.OptT{plz4.WithParallel(0)},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var (
				buf    bytes.Buffer
				rd     = plz4.NewReader(bytes.NewReader(tc.data), tc.opts...)
				_, err = rd.WriteTo(&buf)
			)

			if cerr := rd.Close(); cerr != nil {
				t.Errorf("Expect nil error on Close(): %v", cerr)
			}

			if tc.err != nil || err != nil {
				if !errors.Is(err, tc.err) {
					t.Fatalf("Expected error: %v got %v", tc.err, err)
				}
				return
			}
			ss := Sha2sum(buf.Bytes())
			if ss != tc.sha2 {
				t.Errorf("Hash mismatch: got %s wanted %s", ss, sha2)
			}
		})
	}
}

// Test error codes at different locations on a short read.
func TestShortRead(t *testing.T) {
	defer testBorrowed(t)

	var (
		decoded = []byte("testycode")
		// One frame contains Content-Size, Content-Checksum, Block-Checksum, DictId
		theWorks = []byte{
			0x04, 0x22, 0x4d, 0x18, // magic
			0x7d, 0x70, 0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0b, 0x00, 0x00, 0x00, 0x0c, // header 15 bytes, off 4
			0x06, 0x00, 0x00, 0x00, // block1 size 4 bytes, off 19
			0x50, 0x74, 0x65, 0x73, 0x74, 0x79, // block1 data 6 bytes, off 23
			0xcf, 0x22, 0x82, 0x16, // block1 crc 4 bytes, off 29
			0x05, 0x00, 0x00, 0x00, // block2 size 4 bytes, off 33
			0x40, 0x63, 0x6f, 0x64, 0x65, // block2 data 5 bytes, off 37
			0xc5, 0x63, 0x71, 0xe5, // block2 crc 4 bytes, off 42
			0x00, 0x00, 0x00, 0x00, // trailer 4 bytes, off 46
			0x4a, 0x73, 0x1c, 0xae, // content crc, 4 bytes, off 50
		}
	)

	tests := map[string]struct {
		data    []byte
		expect  int64
		clipOff int
		clipCnt int
		err     error
	}{

		"clip_content_hash": {
			data:    theWorks,
			clipOff: 54,
			clipCnt: 4,
			expect:  9,
			err:     plz4.ErrContentHashRead,
		},
		"clip_read_trailer": {
			data:    theWorks,
			clipOff: 50,
			clipCnt: 4,
			expect:  9,
			err:     plz4.ErrBlockSizeRead,
		},
		"clip_read_block2_crc": {
			data:    theWorks,
			clipOff: 46,
			clipCnt: 4,
			expect:  5,
			err:     plz4.ErrBlockRead,
		},
		"clip_read_block2": {
			data:    theWorks,
			clipOff: 42,
			clipCnt: 5,
			expect:  5,
			err:     plz4.ErrBlockRead,
		},
		"clip_read_block2_size": {
			data:    theWorks,
			clipOff: 37,
			clipCnt: 4,
			expect:  5,
			err:     plz4.ErrBlockSizeRead,
		},
		"clip_read_block1_crc": {
			data:    theWorks,
			clipOff: 33,
			clipCnt: 4,
			expect:  0,
			err:     plz4.ErrBlockRead,
		},
		"clip_read_block1": {
			data:    theWorks,
			clipOff: 29,
			clipCnt: 6,
			expect:  0,
			err:     plz4.ErrBlockRead,
		},
		"clip_read_block1_size": {
			data:    theWorks,
			clipOff: 23,
			clipCnt: 4,
			expect:  0,
			err:     plz4.ErrBlockSizeRead,
		},
		"clip_read_header_crc": {
			data:    theWorks,
			clipOff: 19,
			clipCnt: 18,
			expect:  0,
			err:     plz4.ErrHeaderRead,
		},
		"clip_full": {
			data: []byte{},
			err:  nil, // no data is not considered an error
		},
	}

	// Run WriteTo version
	for name, tc := range tests {
		t.Run(fmt.Sprintf("WriteTo_%s", name), func(t *testing.T) {
			data := tc.data

			for i := 1; i <= tc.clipCnt; i++ {
				var (
					buf    bytes.Buffer
					rd     = plz4.NewReader(bytes.NewReader(data[:tc.clipOff-i]))
					n, err = rd.WriteTo(&buf)
				)
				if cerr := rd.Close(); cerr != nil {
					t.Errorf("Expect nil error on Close(): %v", cerr)
				}
				if tc.err != nil || err != nil {
					if !errors.Is(err, tc.err) {
						t.Errorf("Expected error: %v got %v", tc.err, err)
					}
					if !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
						t.Errorf("Expected io.ErrUnexpectedEOF, got %v", err)
					}
					if errors.Is(err, plz4.ErrCorrupted) {
						t.Errorf("Short read should not be corrupted: %v", err)
					}
				}
				if n != tc.expect {
					t.Errorf("Expected '%v' bytes, got '%v", tc.expect, n)
				}

				if !bytes.Equal(decoded[:tc.expect], buf.Bytes()) {
					t.Errorf("Buffers do not match")
				}
			}

		})
	}

	// Run Read() version
	for name, tc := range tests {
		t.Run(fmt.Sprintf("Read_%s", name), func(t *testing.T) {
			data := tc.data

			for i := 1; i <= tc.clipCnt; i++ {
				var (
					slop   = 16 //extra buffer space force short reads
					buf    = make([]byte, len(data)+slop)
					rd     = plz4.NewReader(bytes.NewReader(data[:tc.clipOff-i]))
					n, err = rd.Read(buf)
				)

				if err == nil && n > 0 && tc.err != nil {
					// Read one more time to catch the error.
					// Errors are deferred to return valid data
					var (
						n2   int
						buf2 = make([]byte, len(data)+4)
					)

					n2, err = rd.Read(buf2)
					if n2 != 0 {
						t.Errorf("Expected zero bytes on deferred read, got: %v", n2)
					}
				}

				if cerr := rd.Close(); cerr != nil {
					t.Errorf("Expect nil error on Close(): %v", cerr)
				}
				if tc.err != nil || err != nil {
					if !errors.Is(err, tc.err) {
						t.Errorf("Expected error: %v got %v", tc.err, err)
					}
					if !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
						t.Errorf("Expected io.ErrUnexpectedEOF, got %v", err)
					}
					if errors.Is(err, plz4.ErrCorrupted) {
						t.Errorf("Short read should not be corrupted: %v", err)
					}
				}
				if int64(n) != tc.expect {
					t.Errorf("Expected '%v' bytes, got '%v", tc.expect, n)
				}

				if !bytes.Equal(decoded[:tc.expect], buf[:n]) {
					t.Errorf("Buffers do not match: %v", buf[:n])
				}
			}
		})
	}
}

// Validate content CRC calculated correctly

func TestContentCRC(t *testing.T) {
	defer testBorrowed(t)

	var (
		oneFrame       = []byte{0x04, 0x22, 0x4d, 0x18, 0x64, 0x40, 0xa7, 0x06, 0x00, 0x00, 0x80, 0x74, 0x65, 0x73, 0x74, 0x79, 0x0a, 0x00, 0x00, 0x00, 0x00, 0x5d, 0xc7, 0x3f, 0x2a}
		oneFrameNoHash = []byte{0x04, 0x22, 0x4d, 0x18, 0x60, 0x40, 0x82, 0x06, 0x00, 0x00, 0x80, 0x74, 0x65, 0x73, 0x74, 0x79, 0x0a, 0x00, 0x00, 0x00, 0x00}
		large, lsha2   = DupeSample(t, Lz4_ContentCRC)
		oneFrameSha2   = "4e64edc52754ee847f3f043382f70d8cc4f83e38113d3555bdee20442d0d5f50"
	)

	corruptCRC := func(d []byte) []byte {
		var n []byte
		n = append(n, d...)
		n[len(n)-1] = n[len(n)-1] + 1
		return n
	}

	tests := map[string]struct {
		data  []byte
		sha2  string
		munge func([]byte) []byte
		err   error
		opts  []plz4.OptT
	}{
		"no_content_hash": {
			data: oneFrameNoHash,
			sha2: oneFrameSha2,
		},
		"content_hash_clipped": {
			data: oneFrame[:len(oneFrame)-1],
			err:  plz4.ErrContentHashRead,
		},
		"clean_one_frame": {
			data: oneFrame,
			sha2: oneFrameSha2,
		},
		"munge_one_frame": {
			data:  oneFrame,
			munge: corruptCRC,
			err:   plz4.ErrContentHash,
		},
		"clean_large": {
			data: large,
			sha2: lsha2,
		},
		"munged_large": {
			data:  large,
			munge: corruptCRC,
			err:   plz4.ErrContentHash,
		},
		"munged_large_sync": {
			data:  large,
			munge: corruptCRC,
			err:   plz4.ErrContentHash,
			opts:  []Option{plz4.WithParallel(0)},
		},
		"large_clipped": {
			data: large[:len(large)-4],
			err:  plz4.ErrContentHashRead,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			data := tc.data
			if tc.munge != nil {
				data = tc.munge(data)
			}
			var (
				buf    bytes.Buffer
				rd     = plz4.NewReader(bytes.NewReader(data), tc.opts...)
				_, err = rd.WriteTo(&buf)
			)

			if cerr := rd.Close(); cerr != nil {
				t.Errorf("Expect nil error on Close(): %v", cerr)
			}

			if tc.err != nil || err != nil {
				if !errors.Is(err, tc.err) {
					t.Fatalf("Expected error: %v got %v", tc.err, err)
				}
				return
			}

			ss := Sha2sum(buf.Bytes())
			if ss != tc.sha2 {
				t.Errorf("Payload mismatch.  Expected '%s' got '%s'", tc.sha2, ss)
			}
		})
	}
}

// Run through through various fixed sizes on Read
func TestFrameReadFixed(t *testing.T) {
	defer testBorrowed(t)

	// Create an simple lz4
	src, lz4 := generateLz4(t)

	// Run through a bunch of dst buffer sizes to catch error cases
	var cases []int
	for i := 1; i < 32; i++ {
		cases = append(cases, i)
	}

	// Add some cases on the large buffer side
	for i := 1; i <= 8192; i *= 2 {
		cases = append(cases, i<<10)
	}

	for _, i := range cases {

		name := fmt.Sprintf("case_%d", i)
		t.Run(name, func(t *testing.T) {

			var (
				dst = make([]byte, i)
				tst bytes.Buffer
				rd  = plz4.NewReader(bytes.NewReader(lz4.Bytes()), plz4.WithParallel(-1))
			)

		LOOP:
			for {
				n, err := rd.Read(dst)
				tst.Write(dst[:n])

				switch {
				case err == nil:
				case errors.Is(err, io.EOF):
					break LOOP
				default:
					t.Fatalf("Fail read next: %v", err)
				}
			}

			if !bytes.Equal(src.Bytes(), tst.Bytes()) {
				t.Errorf("src and tst don't match: len(src):%v len(tst):%v", src.Len(), tst.Len())
			}
		})
	}
}

// Call read API with random buffer sizes
func TestFrameRandRead(t *testing.T) {
	defer testBorrowed(t)

	// Create an simple lz4
	src, lz4 := generateLz4(t)

	var (
		tst bytes.Buffer
		rd  = plz4.NewReader(bytes.NewReader(lz4.Bytes()), plz4.WithParallel(-1))
	)

LOOP:
	for {
		dst := make([]byte, mrand.IntN(4096))
		n, err := rd.Read(dst)
		tst.Write(dst[:n])

		switch {
		case err == nil:
		case errors.Is(err, io.EOF):
			break LOOP
		default:
			t.Fatalf("Fail read next: %v", err)
		}
	}

	if !bytes.Equal(src.Bytes(), tst.Bytes()) {
		t.Errorf("src and tst don't match: len(src):%v len(tst):%v", src.Len(), tst.Len())
	}

}

// Validate error on block size overflow
func TestBlockSizeOverflow(t *testing.T) {
	defer testBorrowed(t)

	data, _ := DupeSample(t, Lz4_ContentCRC)

	var (
		buf           bytes.Buffer
		trailerOffset = 4 + 4 // len(trailer) + len(content_hash)
		dataOff       = len(data) - trailerOffset
	)

	// corrupt the trailer; flip all but high bit on the high byte
	data[dataOff+3] = data[dataOff+3] | 0x7F

	rd := plz4.NewReader(bytes.NewReader(data))
	_, err := rd.WriteTo(&buf)
	if !errors.Is(err, plz4.ErrBlockSizeOverflow) {
		t.Errorf("Expected bad block hash got %v", err)
	}

	if !errors.Is(err, plz4.ErrCorrupted) {
		t.Errorf("Expected ErrCorrupted got %v", err)
	}

	if err := rd.Close(); err != nil {
		t.Errorf("Expected nil error on close: %v", err)
	}
}

// Corrupted block CRC should return an error
func TestBlockCRC(t *testing.T) {
	defer testBorrowed(t)

	data, _ := DupeSample(t, Lz4_BlockCRC)

	var (
		buf       bytes.Buffer
		crcOffset = 4 + 4 + 4 // len(trailer) + len(content_hash) + len(block_crc)
		dataOff   = len(data) - crcOffset
	)

	// corrupt the CRC
	data[dataOff] = data[dataOff] + 1

	rd := plz4.NewReader(bytes.NewReader(data))
	_, err := rd.WriteTo(&buf)
	if !errors.Is(err, plz4.ErrBlockHash) {
		t.Errorf("Expected bad block hash got %v", err)
	}

	if !errors.Is(err, plz4.ErrCorrupted) {
		t.Errorf("Expected ErrCorrupted got %v", err)
	}

	if cerr := rd.Close(); cerr != nil {
		t.Errorf("Expect nil error on Close(): %v", cerr)
	}

}

// Emulate failures with a writer than fails on
// incrementally increasing N writes.  Exercises
// the error return code paths.
func TestWriteToFail(t *testing.T) {
	defer testBorrowed(t)

	var (
		lsrc, _ = LoadSample(t, Lz4_64KB)
	)
	tests := map[string]struct {
		maxSpins int
		opts     []Option
	}{
		"defaults": {
			maxSpins: 50,
			opts:     []Option{plz4.WithParallel(10)},
		},
		"default_sync": {
			maxSpins: 100,
			opts:     []Option{plz4.WithParallel(0)},
		},
	}

	for name, tc := range tests {
		// 'maxSpins' is dependent on block size and sample size,
		// so is a bit fragile if sample size changes.

		for i := 1; i <= tc.maxSpins; i++ {
			t.Run(fmt.Sprintf("%s_fail_%d", name, i), func(t *testing.T) {

				fr := &failWriter{
					err: io.ErrClosedPipe,
					cnt: i,
					wr:  io.Discard,
				}

				rd := plz4.NewReader(bytes.NewReader(lsrc), tc.opts...)

				_, err := rd.WriteTo(fr)

				if !errors.Is(err, fr.err) {
					t.Errorf("Expected error on '%v', got '%v'", fr.err, err)
				}

				if err := rd.Close(); err != nil {
					t.Errorf("Expected nil on Close(), got '%v'", err)
				}

				if err := rd.Close(); !errors.Is(err, fr.err) {
					t.Errorf("Expect original error on subsequent Close call, got: %v", err)
				}

				if _, err := rd.Read(nil); !errors.Is(err, fr.err) {
					t.Errorf("Expect original error on any subsequent Write call, got: %v", err)
				}
			})
		}
	}

}

// Emulate failures with a reader than fails on
// incrementally increasing N reads.  Exercises
// the error return code paths.
func TestReadFail(t *testing.T) {
	defer testBorrowed(t)

	var (
		lsrc, _ = LoadSample(t, Lz4_64KB)
	)
	tests := map[string]struct {
		maxSpins int
		opts     []Option
	}{
		"defaults": {
			maxSpins: 50,
			opts:     []Option{plz4.WithParallel(10)},
		},
		"default_sync": {
			maxSpins: 100,
			opts:     []Option{plz4.WithParallel(0)},
		},
	}

	for name, tc := range tests {
		// 'maxSpins' is dependent on block size and sample size,
		// so is a bit fragile if sample size changes.

		for i := 1; i <= tc.maxSpins; i++ {
			t.Run(fmt.Sprintf("%s_fail_%d", name, i), func(t *testing.T) {

				fr := &failReader{
					err: io.ErrClosedPipe,
					cnt: i,
					rd:  bytes.NewReader(lsrc),
				}

				rd := plz4.NewReader(fr, tc.opts...)

				_, err := rd.WriteTo(io.Discard)

				if !errors.Is(err, fr.err) {
					t.Errorf("Expected error on '%v', got '%v'", fr.err, err)
				}

				if err := rd.Close(); err != nil {
					t.Errorf("Expected nil on Close(), got '%v'", err)
				}

				if err := rd.Close(); !errors.Is(err, fr.err) {
					t.Errorf("Expect original error on subsequent Close call, got: %v", err)
				}

				if _, err := rd.Read(nil); !errors.Is(err, fr.err) {
					t.Errorf("Expect original error on any subsequent Write call, got: %v", err)
				}
			})
		}
	}
}

// Cancel async read in progress.  A slow
// consumer should not overwhelm the CPU.
func TestSlowConsumerAsyncAbort(t *testing.T) {
	defer testBorrowed(t)

	var (
		lsrc, _ = LoadSample(t, Lz4_64KB)
		_, usrc = generateLz4Uncompressable(t, 10<<20, plz4.WithBlockSize(plz4.BlockIdx64KB))
	)

	tests := map[string]struct {
		src  []byte
		opts []Option
	}{
		"parallel": {
			src:  lsrc,
			opts: []Option{plz4.WithParallel(4)},
		},
		"parallel_uncompressable": {
			src:  usrc.Bytes(),
			opts: []Option{plz4.WithParallel(4)},
		},
		"sync": {
			src:  lsrc,
			opts: []Option{plz4.WithParallel(0)},
		},
		"sync_uncompressable": {
			src:  usrc.Bytes(),
			opts: []Option{plz4.WithParallel(0)},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var (
				scratch = make([]byte, 4<<10)
			)

			rd := plz4.NewReader(
				bytes.NewReader(tc.src),
				tc.opts...,
			)

		LOOP:
			for spins := 0; spins < 3; spins++ {
				time.Sleep(time.Millisecond * 100)
				_, err := io.ReadFull(rd, scratch)
				switch err {
				case nil, io.ErrUnexpectedEOF:
				case io.EOF:
					break LOOP
				default:
					t.Fatalf("Unexpected error: %v", err)

				}
			}

			// Cancel the read
			err := rd.Close()

			if !errors.Is(err, nil) {
				t.Errorf("Expected error on '%v', got '%v'", nil, err)
			}

			// Expect closed on second close
			if err := rd.Close(); err != plz4.ErrClosed {
				t.Errorf("Expected nil on Close(), got '%v'", err)
			}
		})

	}

}

// Test ReadOffset feature on reader that does not support seek.
// In this case we are using bytes.Buffer.

func TestReadOffsetNoSeek(t *testing.T) {
	defer testBorrowed(t)

	_, pos, buf := _testLz4WriteProgress(t)

	rd := plz4.NewReader(&buf, plz4.WithReadOffset(pos[3]))
	blk := make([]byte, 1024)
	n, err := rd.Read(blk)
	if err != nil {
		t.Fatalf("Unexpected read error  %v", err)
	}
	if n != len(blk) {
		t.Fatalf("Expected successful read")
	}
	if cerr := rd.Close(); cerr != nil {
		t.Errorf("Expect nil error on Close(): %v", cerr)
	}
}

// Test a read offset call that is less then header.
// Should return an error.
func TestReadOffsetMisaligned(t *testing.T) {
	defer testBorrowed(t)

	sample, _ := LoadSample(t, Lz4_4MB)

	rd := plz4.NewReader(bytes.NewReader(sample), plz4.WithReadOffset(3))

	n, err := rd.WriteTo(io.Discard)

	if err != plz4.ErrReadOffset {
		t.Fatalf("Unexpected read error  %v", err)
	}
	if n != 0 {
		t.Fatalf("Expected zero read count, got %v", n)
	}

	if err := rd.Close(); err != nil {
		t.Errorf("Expected nil error on close: %v", err)
	}
}

// A read offset on a linked payload is not supported.
func TestReadOffsetLinked(t *testing.T) {
	defer testBorrowed(t)

	sample, _ := LoadSample(t, Lz4_Linked)

	rd := plz4.NewReader(bytes.NewReader(sample), plz4.WithReadOffset(100))

	n, err := rd.WriteTo(io.Discard)

	if err != plz4.ErrReadOffsetLinked {
		t.Fatalf("Unexpected read error  %v", err)
	}
	if n != 0 {
		t.Fatalf("Expected zero read count, got %v", n)
	}
}

// Emulate an error on seek during ReadOffset.
func TestReadOffsetBadSeek(t *testing.T) {
	defer testBorrowed(t)

	_, pos, buf := _testLz4WriteProgress(t)

	bs := &badSeeker{rd: &buf, seekErr: io.ErrShortBuffer}
	rd := plz4.NewReader(bs, plz4.WithReadOffset(pos[3]))
	blk := make([]byte, 1024)
	_, err := rd.Read(blk)
	if err != bs.seekErr {
		t.Fatalf("Unexpected read error  %v", err)
	}
}

// Validate parse and callback on dictId.
func TestReadDictId(t *testing.T) {
	defer testBorrowed(t)

	// Create sample with a dictID
	var (
		buf           bytes.Buffer
		dictId        = uint32(999)
		sample, _     = LoadSample(t, LargeUncompressed)
		sampleDict, _ = LoadSample(t, Dict)
		wr            = plz4.NewWriter(
			&buf,
			plz4.WithDictionaryId(dictId),
			plz4.WithDictionary(sampleDict),
		)
	)

	if _, err := wr.Write(sample); err != nil {
		t.Fatalf("Expected no error on write: %v", err)
	}

	if err := wr.Close(); err != nil {
		t.Fatalf("Expected no error on close: %v", err)
	}

	payload := buf.Bytes()

	tests := map[string]struct {
		cb   opts.DictCallbackT
		err  error
		opts []plz4.OptT
	}{
		"nil": {
			err:  nil,
			opts: []plz4.OptT{plz4.WithDictionary(sampleDict)},
		},
		"noop": {
			cb: func(v uint32) ([]byte, error) {
				if v != dictId {
					t.Errorf("Expected dictId: %v, got %v", dictId, v)
				}
				return nil, nil
			},
			opts: []plz4.OptT{plz4.WithDictionary(sampleDict)},
		},
		"override": {
			cb: func(v uint32) ([]byte, error) {
				if v != dictId {
					t.Errorf("Expected dictId: %v, got %v", dictId, v)
				}
				return sampleDict, nil
			},
			opts: []plz4.OptT{plz4.WithDictionary([]byte("shrubbery"))},
		},
		"fail": {
			err: io.ErrShortBuffer,
			cb:  func(uint32) ([]byte, error) { return nil, io.ErrShortBuffer },
		},
		"dict": {
			cb: func(uint32) ([]byte, error) { return sampleDict, nil },
		},
		"wrong_dict": {
			err: plz4.ErrDecompress,
			cb:  func(uint32) ([]byte, error) { return []byte("shrubbery"), nil },
		},
		"dict_with_fail": {
			err: io.ErrShortBuffer,
			cb:  func(uint32) ([]byte, error) { return sampleDict, io.ErrShortBuffer },
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {

			var (
				opts []plz4.OptT
			)

			if tc.cb != nil {
				opts = append(opts, plz4.WithDictCallback(tc.cb))
			}

			opts = append(opts, tc.opts...)

			rd := plz4.NewReader(
				bytes.NewReader(payload),
				opts...,
			)

			_, err := rd.WriteTo(io.Discard)

			if !errors.Is(err, tc.err) {
				t.Errorf("Expected err:%v got: %v", tc.err, err)
			}

			// Cancel the read
			if err := rd.Close(); err != nil {
				t.Errorf("Expected nil error on Close, got: %v", err)
			}

			// Expect original error on second close; unless tc.err is nil
			err = rd.Close()

			switch {
			case errors.Is(err, tc.err):
			case tc.err != nil:
				t.Errorf("Expected '%v' on second Close(), got '%v'", tc.err, err)
			case err != plz4.ErrClosed:
				t.Errorf("Expected ErrClosed error on second Close(), got '%v'", err)
			}
		})

	}

}

//////////
// Helpers
//////////

type failReader struct {
	rd  io.Reader
	cnt int
	err error
}

func (rd *failReader) Read(data []byte) (int, error) {
	rd.cnt -= 1
	if rd.cnt == 0 {
		return 0, rd.err
	}
	return rd.rd.Read(data)
}

func generateLz4(t *testing.T, opts ...plz4.OptT) (bytes.Buffer, bytes.Buffer) {

	// Create an simple lz4
	var (
		src bytes.Buffer
		dst bytes.Buffer
	)

	wr := plz4.NewWriter(&dst, opts...)

	// This should not compress
	data := []byte("a")
	src.Write(data)
	_, err := wr.Write(data)
	if err != nil {
		t.Fatalf("Fail write: %v", err)
	}

	err = wr.Flush()
	if err != nil {
		t.Fatalf("Fail flush: %v", err)
	}

	// This should compress
	data = []byte(strings.Repeat("abcd", 2048))
	src.Write(data)
	_, err = wr.Write(data)
	if err != nil {
		t.Fatalf("Fail write: %v", err)
	}

	err = wr.Flush()
	if err != nil {
		t.Fatalf("Fail flush: %v", err)
	}

	// Random uncompresseable data
	buf := make([]byte, 32<<10)
	_, err = rand.Read(buf)
	if err != nil {
		t.Fatalf("Fail rand bytes")
	}

	src.Write(buf)
	_, err = wr.Write(buf)
	if err != nil {
		t.Fatalf("Fail write: %v", err)
	}

	err = wr.Flush()
	if err != nil {
		t.Fatalf("Fail flush: %v", err)
	}

	// Random data, compressable because we repeat.
	buf = make([]byte, 1025)
	_, err = rand.Read(buf)
	if err != nil {
		t.Fatalf("Fail rand bytes")
	}
	for i := 0; i < 4096; i++ {
		src.Write(buf)
		_, err = wr.Write(buf)
		if err != nil {
			t.Fatalf("Fail write: %v", err)
		}
	}

	err = wr.Close()
	if err != nil {
		t.Fatalf("Fail close: %v", err)
	}

	return src, dst
}

func generateLz4Uncompressable(t *testing.T, sz int64, opts ...plz4.OptT) (bytes.Buffer, bytes.Buffer) {

	// Create an simple lz4
	var (
		src bytes.Buffer
		dst bytes.Buffer
	)

	wr := plz4.NewWriter(&dst, opts...)

	var sum int64
	for sum < sz {
		// Random uncompresseable data
		buf := make([]byte, 32<<10)
		_, err := rand.Read(buf)
		if err != nil {
			t.Fatalf("Fail rand bytes")
		}

		src.Write(buf)
		nWritten, err := wr.Write(buf)
		if err != nil {
			t.Fatalf("Fail write: %v", err)
		}
		sum += int64(nWritten)
	}

	err := wr.Close()
	if err != nil {
		t.Fatalf("Fail close: %v", err)
	}

	return src, dst
}

func lz4Size(t testing.TB, data []byte) int {
	var (
		wr bytes.Buffer
		rd = plz4.NewReader(bytes.NewReader(data))
	)
	if _, err := rd.WriteTo(&wr); err != nil {
		t.Fatalf("Fail decompress: %v", err)
	}
	return wr.Len()
}

type badSeeker struct {
	rd      io.Reader
	seekErr error
}

func (rd *badSeeker) Read(data []byte) (int, error) {
	return rd.rd.Read(data)
}

func (rd *badSeeker) Seek(int64, int) (int64, error) {
	return 0, rd.seekErr
}
