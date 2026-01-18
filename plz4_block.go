package plz4

import (
	"github.com/prequel-dev/plz4/internal/pkg/compress"
)

const (
	maxTries     = 3
	initMultiple = 4
)

// Block options for CompressBlock and DecompressBlock.
type BlockOpt func(blockOpt) blockOpt

type blockOpt struct {
	lvl  LevelT
	dst  []byte
	dict *compress.DictT
}

func (o blockOpt) dictData() []byte {
	if o.dict == nil {
		return nil
	}
	return o.dict.Data()
}

// Specify write compression level [1-12].  Defaults to Level1.
// This applies only to block compression.
func WithBlockCompressionLevel(level LevelT) BlockOpt {
	return func(o blockOpt) blockOpt {
		// Clamp level to the valid range [Level1, Level12] to avoid invalid levels
		// propagating to the underlying compressor and causing a panic.
		if level < Level1 {
			level = Level1
		} else if level > Level12 {
			level = Level12
		}
		o.lvl = level
		return o
	}
}

// Specify dictionary to use for block compression/decompression.
// This applies only to block compression/decompression.
func WithBlockDictionary(dict []byte) BlockOpt {
	return func(o blockOpt) blockOpt {
		o.dict = compress.NewDictT(dict, false)
		return o
	}
}

// Specify destination buffer for compression/decompression.
// If not provided, will allocate sufficient space.
// This applies only to block compression/decompression.
func WithBlockDst(dst []byte) BlockOpt {
	return func(o blockOpt) blockOpt {
		o.dst = dst
		return o
	}
}

// Returns maximum compressed block size for input size sz.
func CompressBlockBound(sz int) int {
	return compress.CompressBound(sz)
}

func parseBlockOpts(opts ...BlockOpt) blockOpt {
	o := blockOpt{
		lvl: Level1,
	}
	for _, opt := range opts {
		o = opt(o)
	}
	return o
}

// Compress src block given options and return compressed block.
// If dst not provided, will allocate sufficient space.
// If dst is provided as an option and that slice is too small,
// an error will be returned.
func CompressBlock(src []byte, opts ...BlockOpt) ([]byte, error) {

	var (
		o   = parseBlockOpts(opts...)
		f   = compress.NewCompressorFactory(o.lvl, true, o.dict)
		c   = f.NewCompressor()
		dst = o.dst
	)

	if dst == nil {
		dst = make([]byte, CompressBlockBound(len(src)))
	}

	n, err := c.Compress(src, dst, o.dictData())
	if err != nil {
		return nil, err
	}

	return dst[:n], nil
}

// Decompress src block given options and return decompressed block.
// If dst not provided, will attempt to allocate sufficient space.
// If dst is provided as an option and that slice is too small,
// an error will be returned.
func DecompressBlock(src []byte, opts ...BlockOpt) ([]byte, error) {
	var (
		o   = parseBlockOpts(opts...)
		dst = o.dst
	)

	d := compress.NewDecompressor(true, o.dict)

	if dst != nil {
		n, err := d.Decompress(src, dst)
		if err != nil {
			return nil, err
		}
		return dst[:n], nil
	}

	// No dst provided, allocate a buffer.
	// Since we don't know the decompressed size, we start with
	// a multiple of the src and reallocate as necessary.
	// Unfortunately, the lz4 API does not distinguish between dst-too-small
	// and other errors, so we must apply a heuristic in the case of an error.
	var (
		nTry    = 1
		bufSize = len(src) * initMultiple
	)

	for {
		dst = make([]byte, bufSize)
		n, err := d.Decompress(src, dst)

		switch {
		case err == nil:
			return dst[:n], nil
		case nTry < maxTries:
			nTry += 1
			bufSize *= 2
		default:
			return nil, err
		}
	}
}
