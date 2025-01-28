//go:build cgo

package compress

import (
	"errors"

	"github.com/prequel-dev/plz4/internal/pkg/clz4"
	"github.com/prequel-dev/plz4/internal/pkg/zerr"
)

type linkedCompressor struct {
	strm *clz4.StreamLinkedCtx
}

func newLinkedCompressor(dict *clz4.DictCtx) Compressor {
	return &linkedCompressor{
		strm: clz4.NewStreamLinkedCtx(dict),
	}
}

func (ctx *linkedCompressor) Compress(src, dst, dict []byte) (int, error) {
	n, err := ctx.strm.Compress(src, dst, dict)

	if err != nil {
		return 0, errors.Join(zerr.ErrCompress, err)
	}

	return n, err
}

type linkedCompressorHC struct {
	strm *clz4.StreamLinkedCtxHC
}

func newLinkedCompressorHC(level LevelT, dict *clz4.DictCtxHC) Compressor {
	return &linkedCompressorHC{
		strm: clz4.NewStreamLinkedCtxHC(dict, int(level)),
	}
}

func (ctx *linkedCompressorHC) Compress(src, dst, dict []byte) (int, error) {
	return ctx.strm.Compress(src, dst, dict)
}
