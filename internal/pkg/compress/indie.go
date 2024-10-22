package compress

import (
	"errors"

	"github.com/prequel-dev/plz4/internal/pkg/clz4"
	"github.com/prequel-dev/plz4/internal/pkg/zerr"
)

//---

type indieCompressorDict struct {
	strm *clz4.StreamIndieCtx
}

func newIndieCompressorDict(dict *clz4.DictCtx) *indieCompressorDict {
	if dict == nil {
		dict = clz4.NewDictCtx([]byte{})
	}

	strm := clz4.NewStreamIndieCtx(dict)
	return &indieCompressorDict{strm: strm}
}

func (ctx *indieCompressorDict) Compress(src, dst, _ []byte) (int, error) {
	n, err := ctx.strm.Compress(src, dst)

	if err != nil {
		return 0, errors.Join(zerr.ErrCompress, err)
	}

	return n, err
}

//---

type indieCompressorDictHC struct {
	strm *clz4.StreamCtxHC
}

func newIndieCompressorDictHC(level LevelT, dict *clz4.DictCtxHC) *indieCompressorDictHC {
	if dict == nil {
		dict = clz4.NewDictCtxHC([]byte{}, int(level))
	}
	strm := clz4.NewStreamCtxHC(int(level), dict)
	return &indieCompressorDictHC{strm: strm}
}

func (ctx *indieCompressorDictHC) Compress(src, dst, _ []byte) (int, error) {
	n, err := ctx.strm.Compress(src, dst)

	if err != nil {
		return 0, errors.Join(zerr.ErrCompress, err)
	}

	return n, err
}

//---

type indieCompressorFast struct {
}

func (ctx *indieCompressorFast) Compress(src, dst, _ []byte) (int, error) {
	n, err := clz4.CompressFast(src, dst, 1)

	if err != nil {
		return 0, errors.Join(zerr.ErrCompress, err)
	}

	return n, err
}

//---

type indieCompressorLevel uint8

func (level indieCompressorLevel) Compress(src, dst, dict []byte) (int, error) {
	n, err := clz4.CompressHC(src, dst, int(level))

	if err != nil {
		return 0, errors.Join(zerr.ErrCompress, err)
	}

	return n, err
}
