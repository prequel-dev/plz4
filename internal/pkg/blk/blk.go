package blk

import (
	"encoding/binary"

	"github.com/prequel-dev/plz4/internal/pkg/compress"
	"github.com/prequel-dev/plz4/internal/pkg/descriptor"
	"github.com/prequel-dev/plz4/internal/pkg/xxh32"
)

// Protect the 'data' slice to avoid an accidental
// reslice which changes the capacity and breaks the pool.
type BlkT struct {
	data []byte
}

func (b *BlkT) Len() int {
	if b == nil {
		return 0
	}
	return len(b.data)
}

func (b *BlkT) Cap() int {
	return cap(b.data)
}

func (b *BlkT) Trim(sz int) {
	b.data = b.data[:sz]
}

func (b *BlkT) View(start, stop int) []byte {
	return b.data[start:stop]
}

func (b *BlkT) Prefix(pos int) []byte {
	return b.data[:pos]
}

func (b *BlkT) Suffix(pos int) []byte {
	return b.data[pos:]
}

func (b *BlkT) Data() []byte {
	return b.data
}

func (b *BlkT) Decompress(dc compress.Decompressor) (*BlkT, error) {
	dstBlk := BorrowBlk(b.Cap() - szOverhead)

	n, err := dc.Decompress(b.data, dstBlk.data)
	if err != nil {
		ReturnBlk(dstBlk)
		return nil, err
	}

	dstBlk.Trim(n)
	return dstBlk, nil
}

// Compress object to a lz4 block.

func (b *BlkT) Compress(cmp compress.Compressor, bsz int, checksum bool, dict []byte) (*BlkT, error) {
	return CompressToBlk(b.data, cmp, bsz, checksum, dict)
}

func CompressToBlk(src []byte, cmp compress.Compressor, bsz int, checksum bool, dict []byte) (*BlkT, error) {

	dstBlk := BorrowBlk(bsz)

	n, err := cmp.Compress(src, dstBlk.View(4, bsz+4), dict)

	if err != nil {
		// If the src data is uncompressable for whatever reason,
		//  write out the src block as non-compressed and keep on truckin'
		n = 0
	}

	var blkSz descriptor.DataBlockSize

	if n == 0 {
		blkSz.SetUncompressed()
		n = copy(dstBlk.View(4, bsz+4), src)
	}

	// Write the data block size to beginning of dstBlk
	blkSz.SetSize(n)
	binary.LittleEndian.PutUint32(dstBlk.Data(), uint32(blkSz))

	if checksum {
		xxh := xxh32.ChecksumZero(dstBlk.View(4, n+4))
		binary.LittleEndian.PutUint32(dstBlk.Suffix(n+4), xxh)
		// Reslice the result buffer to include length + hash trailer
		dstBlk.Trim(n + 8)
	} else {
		// Reslice the result buffer to include length
		dstBlk.Trim(n + 4)
	}

	return dstBlk, nil
}
