package header

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/prequel-dev/plz4/internal/pkg/blk"
	"github.com/prequel-dev/plz4/internal/pkg/descriptor"
	"github.com/prequel-dev/plz4/internal/pkg/opts"
	"github.com/prequel-dev/plz4/internal/pkg/xxh32"
	"github.com/prequel-dev/plz4/internal/pkg/zerr"
)

type HeaderT struct {
	Sz        int64
	DictId    uint32
	ContentSz uint64
	Flags     descriptor.Flags
	BlockDesc descriptor.Block
}

// see lz4_Frame_Format.md

func ReadHeader(rdr io.Reader, cb opts.SkipCallbackT) (nRead int, hdr HeaderT, err error) {
	// Use a block instead of a local []byte buffer because the io.ReadFull
	// call forces an escape so the local buffer becomes heap allocated.
	blkHdr := blk.BorrowBlk(descriptor.BlockIdx64KBSz)
	defer blk.ReturnBlk(blkHdr)

	// Header is minimally 7 bytes, max 19.
	hdrBytes := blkHdr.Prefix(lz4MinHeaderSz)

	if nRead, err = io.ReadFull(rdr, hdrBytes); err != nil {
		// If no data in the stream, return a clean io.EOF.
		// Only considered a header read error if we got > 0 bytes.
		if err != io.EOF {
			err = errors.Join(zerr.ErrHeaderRead, err)
		}
		return
	}

	if !bytes.Equal(hdrBytes[:4], lz4FrameMagic[:]) {
		// Check for special case frame skip
		var n int
		n, err = maybeSkipFrame(rdr, hdrBytes, cb)
		nRead += n
		return
	}

	hdr.Flags = descriptor.Flags(hdrBytes[4])
	hdr.BlockDesc = descriptor.Block(hdrBytes[5])

	if err = sanityCheck(hdr); err != nil {
		return
	}

	// Do we need to grab more data?
	if hdr.Flags.ContentSize() {
		hdrBytes = blkHdr.Prefix(15)

		n, rerr := io.ReadFull(rdr, hdrBytes[7:15])
		nRead += n

		if rerr != nil {
			err = errors.Join(zerr.ErrHeaderRead, rerr)
			return
		}

		hdr.ContentSz = binary.LittleEndian.Uint64(hdrBytes[6:14])
	}

	if hdr.Flags.DictId() {
		hdrSz := len(hdrBytes)
		hdrBytes = blkHdr.Prefix(hdrSz + 4)

		n, rerr := io.ReadFull(rdr, hdrBytes[hdrSz:hdrSz+4])
		nRead += n

		if rerr != nil {
			err = errors.Join(zerr.ErrHeaderRead, rerr)
			return
		}

		hdr.DictId = binary.LittleEndian.Uint32(hdrBytes[hdrSz-1 : hdrSz+3])
	}

	// Validate hash, last byte is the checksum.
	var (
		hdrSz     = len(hdrBytes)
		xxh32Hash = xxh32.ChecksumZero(hdrBytes[4 : hdrSz-1])
		calcHash  = byte((xxh32Hash >> 8) & 0xFF)
		readHash  = hdrBytes[hdrSz-1]
	)

	if calcHash != readHash {
		err = zerr.WrapCorrupted(zerr.ErrHeaderHash)
		return
	}

	hdr.Sz = int64(hdrSz)

	return
}

func sanityCheck(hdr HeaderT) (err error) {

	switch {
	case hdr.Flags.Version() != lz4FrameVers:
		err = zerr.ErrVersion
	case hdr.Flags.Reserved():
		err = zerr.WrapCorrupted(zerr.ErrReserveBitSet)
	case !hdr.BlockDesc.Valid():
		err = zerr.WrapCorrupted(zerr.ErrBlockDescriptor)
	}

	return
}
