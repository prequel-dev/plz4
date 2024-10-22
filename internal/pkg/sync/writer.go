package sync

import (
	"io"

	"github.com/prequel-dev/plz4/internal/pkg/blk"
	"github.com/prequel-dev/plz4/internal/pkg/compress"
	"github.com/prequel-dev/plz4/internal/pkg/header"
	"github.com/prequel-dev/plz4/internal/pkg/opts"
	"github.com/prequel-dev/plz4/internal/pkg/trailer"
	"github.com/prequel-dev/plz4/internal/pkg/xxh32"
	"github.com/prequel-dev/plz4/internal/pkg/zerr"
)

type OptsT = opts.OptsT

type syncWriterT struct {
	wr      io.Writer
	cmp     compress.Compressor
	bsz     int
	srcOff  int
	srcBlk  *BlkT
	srcMark int64
	dstMark int64
	state   error
	opts    *OptsT
	srcHash *xxh32.XXHZero
}

func NewSyncWriter(wr io.Writer, opts *OptsT) *syncWriterT {

	var (
		bsz     = opts.BlockSizeIdx.Size()
		srcBlk  = blk.BorrowBlk(bsz)
		factory = opts.NewCompressorFactory()
	)

	// Scope it down to our block size
	srcBlk.Trim(bsz)

	w := &syncWriterT{
		wr:     wr,
		cmp:    factory.NewCompressor(),
		bsz:    bsz,
		srcBlk: srcBlk,
		srcOff: -1,
		opts:   opts,
	}

	if opts.ContentChecksum {
		w.srcHash = &xxh32.XXHZero{}
	}
	return w
}

func (w *syncWriterT) Write(src []byte) (int, error) {
	if w.state != nil {
		return 0, w.state
	}

	var n int
	n, w.state = w._write(src)
	return n, w.state
}

func (w *syncWriterT) _write(src []byte) (nConsumed int, err error) {

	if w.srcOff < 0 {
		if err = w._writeHeader(); err != nil {
			return
		}
	}

	if w.srcOff > 0 {

		// Append to current srcBlk
		n := copy(w.srcBlk.Suffix(w.srcOff), src)

		// Update our internal offset pointer that
		// tracks where we are in copying from dstBuffer
		w.srcOff += n

		// Update output size
		nConsumed += n

		if w.srcOff == w.bsz {
			if err = w._writeFrame(w.srcBlk.Data()); err != nil {
				return
			}

			w.srcOff = 0
		}

		// Slide the src buffer over by N for next spin
		src = src[n:]
	}

	for len(src) > 0 {

		// Write src directly if large enough to avoid a copy.
		if len(src) >= w.bsz {

			nConsumed += w.bsz

			if err = w._writeFrame(src[:w.bsz]); err != nil {
				return
			}

			src = src[w.bsz:]
		} else {
			// Cache the data in w.srcBlk for the next spin
			n := copy(w.srcBlk.Data(), src)
			nConsumed += n
			w.srcOff = n
			src = src[n:]
		}
	}

	return
}

// Close finishes
func (w *syncWriterT) Close() error {

	// If s.srcBlk is nil, close has already been called
	if w.srcBlk == nil {
		return w.state
	}

	var err error

	switch {
	case w.srcOff < 0:
		// It is possible header was not written if Write not called.
		err = w._writeHeader()
	default:
		// Flush out any bytes that might be in srcBlk
		err = w._flush()
	}

	// Release the buffer whether or not there was an error on Flush
	blk.ReturnBlk(w.srcBlk)
	w.srcBlk = nil
	w.srcOff = 0

	switch {
	case w.state != nil:
		// Close should succeed even if we are in an error state,
		// but should not try to write the trailer.
		return nil
	case err != nil:
		// Return error if flush failed.  Close effectively fails.
		w.state = err
		return err
	}

	w.opts.Handler(w.srcMark, w.dstMark)

	if w.srcHash != nil {
		_, err = trailer.WriteTrailerWithHash(w.wr, w.srcHash.Sum32())
	} else {
		_, err = trailer.WriteTrailer(w.wr)
	}

	// Cache error for future closes
	switch {
	case err == nil:
		w.state = zerr.ErrClosed
	default:
		w.state = err
	}

	return err
}

func (w *syncWriterT) Flush() error {
	if w.state != nil {
		return w.state
	}
	w.state = w._flush()
	return w.state
}

func (w *syncWriterT) _flush() error {
	if w.srcOff <= 0 {
		return nil
	}

	if err := w._writeFrame(w.srcBlk.Prefix(w.srcOff)); err != nil {
		return err
	}

	w.srcOff = 0
	return nil
}

func (w *syncWriterT) ReadFrom(r io.Reader) (int64, error) {
	if w.state != nil {
		return 0, w.state
	}

	var n int64
	n, w.state = w._readFrom(r)
	return n, w.state
}

func (w *syncWriterT) _readFrom(r io.Reader) (nConsumed int64, err error) {

	// Check if header has been written yet
	if w.srcOff < 0 {
		if err = w._writeHeader(); err != nil {
			return
		}
	}

LOOP:
	for {
		n, rerr := io.ReadFull(r, w.srcBlk.Suffix(w.srcOff))
		nConsumed += int64(n)

		switch rerr {
		case nil:
		case io.EOF, io.ErrUnexpectedEOF:
			w.srcOff += n
			break LOOP
		default:
			err = rerr
			break LOOP
		}

		if err = w._writeFrame(w.srcBlk.Data()); err != nil {
			break LOOP
		}

		w.srcOff = 0
	}

	return
}

func (w *syncWriterT) _writeHeader() error {
	w.srcOff = 0

	hdrSz, err := header.WriteHeader(w.wr, w.opts)
	if err != nil {
		return err
	}
	w.dstMark = int64(hdrSz)

	return nil
}

func (w *syncWriterT) _writeFrame(src []byte) error {

	if w.srcHash != nil {
		if _, err := w.srcHash.Write(src); err != nil {
			return err
		}
	}

	dstBlk, err := blk.CompressToBlk(src, w.cmp, w.bsz, w.opts.BlockChecksum, nil)
	if err != nil {
		return err
	}
	defer blk.ReturnBlk(dstBlk)

	wsz, err := w.wr.Write(dstBlk.Data())

	if err != nil {
		return err
	}

	w.opts.Handler(w.srcMark, w.dstMark)
	w.srcMark += int64(len(src))
	w.dstMark += int64(wsz)

	return err
}
