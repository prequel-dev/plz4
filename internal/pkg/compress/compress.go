//go:build cgo

package compress

import "github.com/prequel-dev/plz4/internal/pkg/clz4"

type Compressor interface {
	Compress(src, dst, dict []byte) (int, error)
}

var statelessFast indieCompressorFast

// memoize compressors avoids unnecessary alloc
// at the cost of init time and global ram
var memoizeCompressors = []indieCompressorLevel{
	2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
}

type LevelT int

type CompressorFactory struct {
	indie     bool
	level     LevelT
	dictCtx   *clz4.DictCtx
	dictCtxHC *clz4.DictCtxHC
}

func NewCompressorFactory(level LevelT, independent bool, dict *DictT) CompressorFactory {

	f := CompressorFactory{
		level: level,
		indie: independent,
	}

	switch {
	case dict == nil:
	case level == 1:
		f.dictCtx = clz4.NewDictCtx(dict.Data())
	default:
		f.dictCtxHC = clz4.NewDictCtxHC(dict.Data(), int(level))
	}

	return f
}

func (f CompressorFactory) NewCompressor() Compressor {

	if f.indie {
		return f.newIndie()
	}

	return f.newLinked()
}

func (f CompressorFactory) newIndie() Compressor {
	switch {
	case f.dictCtx != nil:
		return newIndieCompressorDict(f.dictCtx)
	case f.dictCtxHC != nil:
		return newIndieCompressorDictHC(f.level, f.dictCtxHC)
	case f.level <= 1:
		return &statelessFast
	case f.level <= 12:
		return memoizeCompressors[f.level-2]
	default:
		panic("invalid level")
	}
}

func (f CompressorFactory) newLinked() Compressor {
	switch {
	case f.level == 1:
		return newLinkedCompressor(f.dictCtx)
	default:
		return newLinkedCompressorHC(f.level, f.dictCtxHC)
	}
}
