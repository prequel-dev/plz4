package sync

import (
	"io"

	"github.com/prequel-dev/plz4/internal/pkg/blk"
	"github.com/prequel-dev/plz4/internal/pkg/compress"
	"github.com/prequel-dev/plz4/internal/pkg/header"
	"github.com/prequel-dev/plz4/internal/pkg/opts"
	"github.com/prequel-dev/plz4/internal/pkg/xxh32"
	"github.com/prequel-dev/plz4/internal/pkg/zerr"
)

type BlkT = blk.BlkT

type syncReaderT struct {
	dc       compress.Decompressor
	frameRdr blk.FrameReader
	hasher   *xxh32.XXHZero
}

func NewSyncReader(rdr io.Reader, hdr header.HeaderT, opts *opts.OptsT) *syncReaderT {

	var hasher *xxh32.XXHZero
	if hdr.Flags.ContentChecksum() && (opts.ReadOffset == 0) && opts.ContentChecksum {
		hasher = &xxh32.XXHZero{}
	}

	var dict *compress.DictT
	if opts.Dictionary != nil || !hdr.Flags.BlockIndependence() {
		dict = compress.NewDictT(opts.Dictionary, !hdr.Flags.BlockIndependence())
	}

	return &syncReaderT{
		dc: compress.NewDecompressor(
			hdr.Flags.BlockIndependence(),
			dict,
		),
		frameRdr: *blk.NewFrameReader(
			rdr,
			hdr.BlockDesc.Idx().Size(),
			hdr.Flags.ContentChecksum(),
			hdr.Flags.BlockChecksum(),
		),
		hasher: hasher,
	}
}

func (r *syncReaderT) NextBlock(prevBlk *BlkT) (*BlkT, int, error) {

	switch {
	case prevBlk == nil:
	case r.hasher == nil:
		blk.ReturnBlk(prevBlk)
	default:
		r.hasher.Write(prevBlk.Data())
		blk.ReturnBlk(prevBlk)
	}

	frame, err := r.frameRdr.Read()

	switch err {
	case nil:
	case zerr.EndMark:
		if r.hasher != nil {
			if r.hasher.Sum32() != r.frameRdr.ContentChecksum() {
				err = zerr.ErrContentHash
			}
		}
		fallthrough
	default:
		return nil, frame.ReadCnt, err
	}

	// If not compressed, directly return the block from reader
	if frame.Uncompressed {
		return frame.Blk, frame.ReadCnt, nil
	}

	// Decompress the frame block
	dstBlk, err := frame.Blk.Decompress(r.dc)

	// We are done with the srcBlk, can return for reuse.
	blk.ReturnBlk(frame.Blk)

	return dstBlk, frame.ReadCnt, err
}

func (r *syncReaderT) Close() {
}
