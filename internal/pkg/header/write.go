package header

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/prequel-dev/plz4/internal/pkg/descriptor"
	"github.com/prequel-dev/plz4/internal/pkg/opts"
	"github.com/prequel-dev/plz4/internal/pkg/xxh32"
	"github.com/prequel-dev/plz4/internal/pkg/zerr"
)

// see lz4_Frame_format.md
const (
	lz4FrameVers   = uint8(1)
	lz4MinHeaderSz = 7
	lz4MaxHeaderSz = 19
)

var lz4FrameMagic = [4]byte{0x04, 0x22, 0x4d, 0x18}

func WriteHeader(wr io.Writer, opts *opts.OptsT) (int, error) {

	hdrBytes := make([]byte, lz4MinHeaderSz, lz4MaxHeaderSz)
	copy(hdrBytes, lz4FrameMagic[:])

	var (
		flags descriptor.Flags
		bsize descriptor.Block
	)

	flags.SetVersion(lz4FrameVers)
	bsize.SetIdx(opts.BlockSizeIdx)

	if !opts.BlockLinked {
		flags.SetBlockIndependence()
	}

	if opts.BlockChecksum {
		flags.SetBlockChecksum()
	}

	if opts.ContentChecksum {
		flags.SetContentChecksum()
	}

	if opts.ContentSz != nil {
		flags.SetContentSize()
		hdrBytes = hdrBytes[:15]
		binary.LittleEndian.PutUint64(hdrBytes[6:14], *opts.ContentSz)
	}

	if opts.DictionaryId != nil {
		flags.SetDictId()
		hdrSz := len(hdrBytes)
		hdrBytes = hdrBytes[:hdrSz+4]
		binary.LittleEndian.PutUint32(hdrBytes[hdrSz-1:hdrSz+3], *opts.DictionaryId)
	}

	hdrBytes[4] = byte(flags)
	hdrBytes[5] = byte(bsize)

	xxh := xxh32.ChecksumZero(hdrBytes[4 : len(hdrBytes)-1])
	hdrBytes[len(hdrBytes)-1] = byte((xxh >> 8) & 0xFF)

	n, err := wr.Write(hdrBytes)
	if err != nil {
		return n, errors.Join(zerr.ErrHeaderWrite, err)
	}

	return n, err
}
