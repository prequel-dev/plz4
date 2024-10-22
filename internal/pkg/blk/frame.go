package blk

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/prequel-dev/plz4/internal/pkg/descriptor"
	"github.com/prequel-dev/plz4/internal/pkg/xxh32"
	"github.com/prequel-dev/plz4/internal/pkg/zerr"
)

type FrameReader struct {
	rdr      io.Reader // data source
	bsz      int       // blocksize as specifed in header
	srcSum   uint32    // contains content checksum if last frame and enabled
	srcCheck bool      // parse content checksum on EOF
	blkCheck bool      // validate hash on blocks
}

func NewFrameReader(rdr io.Reader, bsz int, srcCheck, blkCheck bool) *FrameReader {
	return &FrameReader{
		rdr:      rdr,
		bsz:      bsz,
		srcCheck: srcCheck,
		blkCheck: blkCheck,
	}
}

type FrameT struct {
	Blk          *BlkT
	ReadCnt      int
	Uncompressed bool
}

func (fr *FrameReader) Read() (FrameT, error) {
	var (
		dstBlk                   = BorrowBlk(fr.bsz)
		nRead, uncompressed, err = fr._read(dstBlk)
	)

	if err != nil {
		ReturnBlk(dstBlk)
		return FrameT{}, err
	}

	return FrameT{
		Blk:          dstBlk,
		ReadCnt:      nRead,
		Uncompressed: uncompressed,
	}, nil
}

func (fr *FrameReader) _read(dstBlk *BlkT) (nRead int, uncompressed bool, err error) {

	var v uint32
	nRead, v, err = fr.readUint32(dstBlk.Prefix(4))
	if err != nil {
		err = errors.Join(zerr.ErrBlockSizeRead, err)
		return
	}

	var (
		n      int
		blkSz  = descriptor.DataBlockSize(v)
		dataSz = blkSz.Size()
	)

	switch {
	// Zero block size indicates EOF
	case blkSz.EOF():
		n, err = fr.maybeReadContentHash(dstBlk.Data())
		nRead += n
		if err == nil {
			err = zerr.EndMark
		}
		return
	// Sanity check on size
	case dataSz > fr.bsz:
		err = zerr.WrapCorrupted(zerr.ErrBlockSizeOverflow)
		return
	// Adjust block size of there is a hash trailer
	case fr.blkCheck:
		dataSz += 4
	}

	// Slice it down to data size
	dstBlk.Trim(dataSz)

	// Read the frame
	n, err = io.ReadFull(fr.rdr, dstBlk.Data())
	nRead += n

	// Return on failure
	if err != nil {
		err = errors.Join(zerr.ErrBlockRead, err)
		return
	}

	// Check hash on block if option is enabled
	if fr.blkCheck {
		if err = checkBlkHash(dstBlk.Data()); err != nil {
			return
		}
		// Strip off blk trailer
		dstBlk.Trim(dstBlk.Len() - 4)

	}

	uncompressed = blkSz.Uncompressed()
	return
}

func checkBlkHash(data []byte) error {
	var (
		hashOff  = len(data) - 4
		blkData  = data[:hashOff]
		calcHash = xxh32.ChecksumZero(blkData)
		readHash = binary.LittleEndian.Uint32(data[hashOff:])
	)

	if readHash != calcHash {
		return zerr.WrapCorrupted(zerr.ErrBlockHash)
	}

	return nil
}

func (fr *FrameReader) maybeReadContentHash(buf []byte) (int, error) {
	if !fr.srcCheck {
		return 0, nil
	}
	nRead, v, err := fr.readUint32(buf[:4])
	if err != nil {
		return nRead, errors.Join(zerr.ErrContentHashRead, err)
	}
	fr.srcSum = v
	return nRead, nil
}

func (fr *FrameReader) ContentChecksum() uint32 {
	return fr.srcSum
}

// Read block header into pre-allocated buffer;
// binary.Read causes an escape to heap because
// of the underlying io.Read call

func (fr *FrameReader) readUint32(buf []byte) (nRead int, v uint32, err error) {

	if nRead, err = io.ReadFull(fr.rdr, buf); err != nil {
		return
	}
	v = binary.LittleEndian.Uint32(buf)
	return
}
