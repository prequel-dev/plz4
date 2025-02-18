//go:build !cgo

package compress

import (
	"errors"
	"fmt"

	"github.com/pierrec/lz4/v4"
	"github.com/prequel-dev/plz4/internal/pkg/zerr"
)

var statelessDecompressor indieDecompressor

type Decompressor interface {
	Decompress(src, dst []byte) (int, error)
}

func NewDecompressor(independent bool, dict *DictT) Decompressor {
	switch {
	case !independent:
		return &failedDecompressor{err: fmt.Errorf("%w: lz4 backend does not support linked blocks", zerr.ErrUnsupported)}
	case dict != nil:
		return &dictDecompressor{dict: dict}
	default:
	}
	return statelessDecompressor
}

type indieDecompressor struct {
}

func (ctx indieDecompressor) Decompress(src, dst []byte) (n int, err error) {
	n, err = lz4.UncompressBlock(src, dst)
	if err != nil {
		return 0, errors.Join(zerr.ErrCorrupted, zerr.ErrDecompress, err)
	}
	return
}

type dictDecompressor struct {
	dict *DictT
}

func (ctx *dictDecompressor) Decompress(src, dst []byte) (n int, err error) {
	n, err = lz4.UncompressBlockWithDict(src, dst, ctx.dict.Data())
	if err != nil {
		return 0, errors.Join(zerr.ErrCorrupted, zerr.ErrDecompress, err)
	}
	return
}

type failedDecompressor struct {
	err error
}

func (c *failedDecompressor) Decompress(src, dst []byte) (int, error) {
	return 0, c.err
}
