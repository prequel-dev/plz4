//go:build cgo

package compress

import (
	"errors"

	"github.com/prequel-dev/plz4/internal/pkg/clz4"
	"github.com/prequel-dev/plz4/internal/pkg/zerr"
)

var statelessDecompressor indieDecompressor

type Decompressor interface {
	Decompress(src, dst []byte) (int, error)
}

func NewDecompressor(independent bool, dict *DictT) Decompressor {

	if !independent || dict != nil {
		return &indieDecompressorWithDict{dict: dict}
	}

	return &statelessDecompressor
}

//---

type indieDecompressor struct {
}

func (ctx *indieDecompressor) Decompress(src, dst []byte) (int, error) {
	n, err := clz4.DecompressSafe(src, dst)
	if err != nil {
		return 0, errors.Join(zerr.ErrCorrupted, zerr.ErrDecompress, err)
	}
	return n, nil
}

//---

type indieDecompressorWithDict struct {
	dict *DictT
}

func (ctx *indieDecompressorWithDict) Decompress(src, dst []byte) (int, error) {
	n, err := clz4.DecompressSafeWithDict(src, dst, ctx.dict.Data())

	if err != nil {
		return 0, errors.Join(zerr.ErrCorrupted, zerr.ErrDecompress, err)
	}

	if ctx.dict.NeedsUpdate() {
		ctx.dict.Update(dst[:n])
	}

	return n, nil
}
