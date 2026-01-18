//go:build !cgo

package compress

import (
	"fmt"

	"github.com/pierrec/lz4/v4"
	"github.com/prequel-dev/plz4/internal/pkg/zerr"
)

type LevelT int

type Compressor interface {
	Compress(src, dst, dict []byte) (int, error)
}

type CompressorFactory struct {
	indie bool
	level LevelT
	dict  *DictT
}

func NewCompressorFactory(level LevelT, independent bool, dict *DictT) CompressorFactory {

	f := CompressorFactory{
		level: level,
		indie: independent,
		dict:  dict,
	}

	return f
}

func (f CompressorFactory) NewCompressor() Compressor {
	switch {
	case !f.indie:
		return &failedCompressor{fmt.Errorf("%w: lz4 backend does not support linked blocks", zerr.ErrUnsupported)}
	case f.dict != nil:
		return &failedCompressor{fmt.Errorf("%w: lz4 backend does not support dictionaries", zerr.ErrUnsupported)}
	case f.level <= 1:
		return &fastCompressor{}
	default:
	}

	return NewCompressorHC(int(f.level - 1))
}

type fastCompressor struct {
}

func (c *fastCompressor) Compress(src, dst, dict []byte) (int, error) {
	return lz4.CompressBlock(src, dst, nil)
}

func NewCompressorHC(level int) Compressor {
	if level > 9 {
		level = 9
	}
	return &hcCompressor{
		level: lz4Level(level),
	}
}

type hcCompressor struct {
	level lz4.CompressionLevel
}

func (c *hcCompressor) Compress(src, dst, dict []byte) (int, error) {
	return lz4.CompressBlockHC(src, dst, c.level, nil, nil)
}

type failedCompressor struct {
	err error
}

func (c *failedCompressor) Compress(src, dst, dict []byte) (int, error) {
	return 0, c.err
}

func lz4Level(l int) lz4.CompressionLevel {

	var lz4Level lz4.CompressionLevel
	switch l {
	case 0:
		lz4Level = lz4.Fast
	case 1:
		lz4Level = lz4.Level1
	case 2:
		lz4Level = lz4.Level2
	case 3:
		lz4Level = lz4.Level3
	case 4:
		lz4Level = lz4.Level4
	case 5:
		lz4Level = lz4.Level5
	case 6:
		lz4Level = lz4.Level6
	case 7:
		lz4Level = lz4.Level7
	case 8:
		lz4Level = lz4.Level8
	case 9:
		lz4Level = lz4.Level9
	default:
		panic("fail map lz4 compression level")
	}
	return lz4Level
}

func CompressBound(sz int) int {
	return lz4.CompressBlockBound(sz)
}
