package clz4

// #cgo CFLAGS: -O3
// #include "lz4.h"
// #include "lz4hc.h"
import "C"
import (
	"errors"
	"fmt"
	"unsafe"
)

var (
	ErrLz4Compress   = errors.New("lz4 fail compress; insufficient destination buffer")
	ErrLz4Decompress = errors.New("lz4 fail decompress")
)

func byteSliceToCharPointer(b []byte) *C.char {
	if len(b) == 0 {
		return (*C.char)(unsafe.Pointer(nil))
	}
	return (*C.char)(unsafe.Pointer(&b[0]))
}

func CompressFast(source, dest []byte, acceleration int) (int, error) {
	ret := int(C.LZ4_compress_fast(
		byteSliceToCharPointer(source),
		byteSliceToCharPointer(dest),
		C.int(len(source)),
		C.int(len(dest)),
		C.int(acceleration),
	))

	if ret == 0 {
		return ret, ErrLz4Compress
	}

	return ret, nil
}

func DecompressSafe(source, dest []byte) (int, error) {
	ret := int(C.LZ4_decompress_safe(
		byteSliceToCharPointer(source),
		byteSliceToCharPointer(dest),
		C.int(len(source)),
		C.int(len(dest)),
	))

	if ret < 0 {
		return ret, fmt.Errorf("%w: code %d", ErrLz4Decompress, ret)
	}

	return ret, nil
}

func DecompressSafeWithDict(source, dest, dict []byte) (int, error) {
	ret := int(C.LZ4_decompress_safe_usingDict(
		byteSliceToCharPointer(source),
		byteSliceToCharPointer(dest),
		C.int(len(source)),
		C.int(len(dest)),
		byteSliceToCharPointer(dict),
		C.int(len(dict)),
	))

	if ret < 0 {
		return ret, fmt.Errorf("%w: code %d", ErrLz4Decompress, ret)
	}

	return ret, nil

}

func CompressHC(source, dest []byte, level int) (int, error) {
	ret := int(C.LZ4_compress_HC(
		byteSliceToCharPointer(source),
		byteSliceToCharPointer(dest),
		C.int(len(source)),
		C.int(len(dest)),
		C.int(level),
	))

	if ret == 0 {
		return ret, ErrLz4Compress
	}

	return ret, nil
}

type DictCtx struct {
	data []byte
	strm C.LZ4_stream_t
}

func NewDictCtx(dict []byte) *DictCtx {

	// dupe 'dict' for stability
	// must remained pinned for life of ctx.
	dupe := make([]byte, len(dict))
	copy(dupe, dict)

	ctx := &DictCtx{
		data: dupe,
	}

	C.LZ4_resetStream_fast(&ctx.strm)

	C.LZ4_loadDictSlow(
		&ctx.strm,
		byteSliceToCharPointer(ctx.data),
		C.int(len(ctx.data)),
	)
	return ctx
}

type DictCtxHC struct {
	data []byte
	strm C.LZ4_streamHC_t
}

func NewDictCtxHC(dict []byte, level int) *DictCtxHC {

	// dupe 'dict' for stability
	// must remained pinned for life of ctx.
	dupe := make([]byte, len(dict))
	copy(dupe, dict)

	ctx := &DictCtxHC{
		data: dupe,
	}

	C.LZ4_resetStreamHC_fast(
		&ctx.strm,
		C.int(level),
	)

	C.LZ4_loadDictHC(
		&ctx.strm,
		byteSliceToCharPointer(ctx.data),
		C.int(len(ctx.data)),
	)
	return ctx
}

type StreamIndieCtx struct {
	ctx  C.LZ4_stream_t
	dict *DictCtx
}

func NewStreamIndieCtx(dict *DictCtx) *StreamIndieCtx {
	return &StreamIndieCtx{dict: dict}
}

func (c *StreamIndieCtx) Compress(src, dst []byte) (int, error) {

	C.LZ4_resetStream_fast(&c.ctx)
	C.LZ4_attach_dictionary(&c.ctx, &c.dict.strm)

	ret := int(C.LZ4_compress_fast_continue(
		&c.ctx,
		byteSliceToCharPointer(src),
		byteSliceToCharPointer(dst),
		C.int(len(src)),
		C.int(len(dst)),
		C.int(1),
	))

	if ret == 0 {
		return ret, ErrLz4Compress
	}

	return ret, nil
}

type StreamCtxHC struct {
	ctx   C.LZ4_streamHC_t
	dict  *DictCtxHC
	level int
}

func NewStreamCtxHC(level int, dict *DictCtxHC) *StreamCtxHC {
	return &StreamCtxHC{level: level, dict: dict}
}

func (c *StreamCtxHC) Compress(src, dst []byte) (int, error) {

	C.LZ4_resetStreamHC_fast(&c.ctx, C.int(c.level))
	C.LZ4_attach_HC_dictionary(&c.ctx, &c.dict.strm)

	ret := int(C.LZ4_compress_HC_continue(
		&c.ctx,
		byteSliceToCharPointer(src),
		byteSliceToCharPointer(dst),
		C.int(len(src)),
		C.int(len(dst)),
	))

	if ret == 0 {
		return ret, ErrLz4Compress
	}

	return ret, nil
}

type StreamLinkedCtx struct {
	ctx C.LZ4_stream_t
}

func NewStreamLinkedCtx(dict *DictCtx) *StreamLinkedCtx {
	c := new(StreamLinkedCtx)
	C.LZ4_resetStream_fast(&c.ctx)
	if dict != nil {
		C.LZ4_attach_dictionary(&c.ctx, &dict.strm)
	}
	return c
}

func (c *StreamLinkedCtx) Compress(src, dst, dict []byte) (int, error) {

	if dict != nil {
		C.LZ4_loadDict(
			&c.ctx,
			byteSliceToCharPointer(dict),
			C.int(len(dict)),
		)
	}

	ret := int(C.LZ4_compress_fast_continue(
		&c.ctx,
		byteSliceToCharPointer(src),
		byteSliceToCharPointer(dst),
		C.int(len(src)),
		C.int(len(dst)),
		C.int(1),
	))

	if ret == 0 {
		return ret, ErrLz4Compress
	}

	return ret, nil
}

type StreamLinkedCtxHC struct {
	ctx C.LZ4_streamHC_t
}

func NewStreamLinkedCtxHC(dict *DictCtxHC, level int) *StreamLinkedCtxHC {
	c := new(StreamLinkedCtxHC)
	C.LZ4_resetStreamHC_fast(&c.ctx, C.int(level))
	if dict != nil {
		C.LZ4_attach_HC_dictionary(&c.ctx, &dict.strm)
	}
	return c
}

func (c *StreamLinkedCtxHC) Compress(src, dst, dict []byte) (int, error) {

	if dict != nil {
		C.LZ4_loadDictHC(
			&c.ctx,
			byteSliceToCharPointer(dict),
			C.int(len(dict)),
		)
	}

	ret := int(C.LZ4_compress_HC_continue(
		&c.ctx,
		byteSliceToCharPointer(src),
		byteSliceToCharPointer(dst),
		C.int(len(src)),
		C.int(len(dst)),
	))

	if ret == 0 {
		return ret, ErrLz4Compress
	}

	return ret, nil
}
