package header

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/prequel-dev/plz4/internal/pkg/opts"
	"github.com/prequel-dev/plz4/internal/pkg/zerr"
)

const skipMagic = uint32(0x184D2A50)

// Write a skip frame header to wr.
// nibble: 	4-bit unsigned value shifted into low nibble on magic
// sz: 		32-bit unsigned long size of frame.
func WriteSkip(wr io.Writer, nibble uint8, sz uint32) (int, error) {

	if nibble > 0xF {
		return 0, zerr.ErrNibble
	}

	var (
		magic   = skipMagic | uint32(nibble)
		payload = make([]byte, 8)
	)

	binary.LittleEndian.PutUint32(payload[:4], magic)
	binary.LittleEndian.PutUint32(payload[4:8], sz)

	// Yes, payload will escape.  Sigh.
	return wr.Write(payload)
}

// Value : 0x184D2A5X, which means any value from 0x184D2A50 to 0x184D2A5F.
// All 16 values are valid to identify a skippable frame.

func maybeSkipFrame(rdr io.Reader, hdr []byte, cb opts.SkipCallbackT) (int, error) {

	m := binary.LittleEndian.Uint32(hdr[:4])
	if m>>4 != skipMagic>>4 {
		return 0, zerr.WrapCorrupted(zerr.ErrMagic)
	}

	// We read 7 bytes on entry; read the 8th byte into header to determine size
	// There is enough capacity in the hdr slice to accommodate a full 19 byte header.

	nRead, err := io.ReadFull(rdr, hdr[7:8])
	if err != nil {
		err = errors.Join(zerr.ErrHeaderRead)
		return nRead, err
	}

	bsz := binary.LittleEndian.Uint32(hdr[4:8])

	if cb != nil {
		var n int
		n, err = cb(rdr, uint8(m&0xF), bsz)
		nRead += n
	} else {
		var n int64
		n, err = io.CopyN(io.Discard, rdr, int64(bsz))
		nRead += int(n)
	}

	switch err {
	case nil:
		// If no error processing frame; return EndMark.
		// This will shift the reader back into ReadHeader mode
		err = zerr.EndMark
	default:
		err = errors.Join(zerr.ErrSkip, err)
	}

	return nRead, err
}
