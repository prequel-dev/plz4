package rdr

import (
	"io"
	"os"

	"github.com/prequel-dev/plz4/internal/pkg/async"
	"github.com/prequel-dev/plz4/internal/pkg/blk"
	"github.com/prequel-dev/plz4/internal/pkg/header"
	"github.com/prequel-dev/plz4/internal/pkg/opts"
	"github.com/prequel-dev/plz4/internal/pkg/sync"
	"github.com/prequel-dev/plz4/internal/pkg/zerr"
)

type Reader struct {
	rdr       io.Reader
	dstOff    int
	dstBlk    *blk.BlkT
	blkReader blk.BlkRdrI
	opts      *opts.OptsT
	srcPos    int64
	dstPos    int64
	contentSz uint64
	state     error
}

// Construct a Reader to decompress the LZ4 Frame stream 'rdr'.
//
// Specify optional parameters in 'opts'.
func NewReader(rdr io.Reader, o *opts.OptsT) *Reader {
	// Will escape
	return &Reader{
		rdr:  rdr,
		opts: o,
	}
}

// Read decompressed data into 'dst'.  Implements the io.Reader interface.
func (r *Reader) Read(dst []byte) (int, error) {
	if r.state != nil {
		return 0, r.state
	}

	var (
		err   error
		nRead int
	)

	switch {
	case r.inBodyMode():
		nRead, err = r.modeBody(dst)
	default:
		nRead, err = r.modeHeader(dst)
	}

	switch {
	case err == nil:
		// NOOP
	case err == zerr.EndMark:
		// Handle end of frame
		err = r.handleEndMark()
		// Set state to returned error, if there is one.
		r.state = err
	case nRead > 0 && r.inBodyMode():
		// In the case that we received *SOME* data during a body read,
		// return the nRead count and set the return value to 'nil'.
		// This defers returning the error until the next read, which allows
		// caller to consume data up until the actual error.
		// While not strictly necessary, many consumers of an io.Reader do not properly
		// handle an error with n > 0.  As such, this implementation improves compatibility.
		// See https://pkg.go.dev/io#Reader
		r.state = err
		err = nil
	default:
		// Set state of Reader to error, which will short-circuit any subsequent calls.
		r.state = err
	}

	return nRead, err
}

// Reset state; next Read() will read the header and likely EOF
// This supports transparent reads of consecutive LZ4 frames.
func (r *Reader) handleEndMark() (err error) {

	if r.opts.ContentSz != nil && !r.opts.SkipContentSz && *r.opts.ContentSz != r.contentSz {
		err = zerr.WrapCorrupted(zerr.ErrContentSize)
	}

	r.contentSz = 0
	r.blkReader.Close()
	r.blkReader = nil
	return
}

// Close the reader to release underlying resources.
//
// Close() *MUST* be called on completion whether or not the Reader
// is in an error state.
func (r *Reader) Close() error {
	// r.opts is a sentinel for Close
	if r.opts == nil {
		return r.state
	}

	// Close blk reader if still allocated
	if r.blkReader != nil {
		r.blkReader.Close()
		r.blkReader = nil
	}

	// Clear dstBlk if still around.
	if r.dstBlk != nil {
		blk.ReturnBlk(r.dstBlk)
		r.dstBlk = nil
		r.dstOff = 0
	}

	// Set state to zerr.ErrClosed if undefined;
	// otherwise subsequent calls should return original error
	if r.state == nil {
		r.state = zerr.ErrClosed
	}

	// Use r.opts as a sentinel for Close
	r.opts = nil
	return nil
}

// Decompress to 'wr'.
// Implements the io.WriteTo interface.
func (r *Reader) WriteTo(wr io.Writer) (int64, error) {

	var sum int64

LOOP:
	for r.state == nil {

		if !r.inBodyMode() {
			var nBytes int
			nBytes, r.state = r._readHeader()

			// Update srcPos to account for header.
			r.srcPos += int64(nBytes)

			if r.state != nil {
				if r.state == io.EOF {
					// Eat io.EOF on readHeader per WriterTo interface
					// io.EOF on readHeader simply means read stream is done.
					r.state = nil
				}
				break LOOP
			}
		}

		var n int64
		n, r.state = r._writeTo(wr)

		sum += n

		if r.state == zerr.EndMark {
			r.state = r.handleEndMark()
		}
	}

	return sum, r.state
}

func (r *Reader) _writeTo(w io.Writer) (n int64, err error) {

LOOP:
	for {
		if r.dstOff < r.dstBlk.Len() {

			// Write out any pending data
			var nWritten int
			nWritten, err = w.Write(r.dstBlk.Suffix(r.dstOff))

			// Update our internal offset pointer that
			// tracks where we are in copying from dstBuffer
			r.dstOff += nWritten

			// Update return cnt
			n += int64(nWritten)

			if err != nil {
				break LOOP
			}
		}

		// Retrieve next lz4 block, and on success copy at top of loop
		if err = r.nextBlock(); err != nil {
			break LOOP
		}
	}

	return
}

func (r *Reader) nextBlock() (err error) {
	var nRead int

	// Reset dstOff; we are retrieving a new dstBlk.
	r.dstOff = 0

	// Grab the next uncompressed output dstBlk.
	// nRead is the amount of data read from the src.
	r.dstBlk, nRead, err = r.blkReader.NextBlock(r.dstBlk)
	r.opts.Handler(r.srcPos, r.dstPos)

	// Update srcPos to account for compressed block size.
	r.srcPos += int64(nRead)

	// Update dstPos to account for decompressed block size
	r.dstPos += int64(r.dstBlk.Len())

	// Content size tracks the cummulative size of this frame
	r.contentSz += uint64(r.dstBlk.Len())
	return
}

func (r *Reader) modeHeader(dst []byte) (int, error) {
	_, err := r._readHeader()

	if err != nil {
		return 0, err
	}

	// And read body immediately
	return r.modeBody(dst)
}

func (r *Reader) _readHeader() (int, error) {

	nRead, hdr, err := header.ReadHeader(r.rdr, r.opts.SkipCallback)

	// If we hit an EndMark on ReadHeader, this means we processes a
	// skipFrame.  Keeping skipping until we hit a different error.
	for err == zerr.EndMark {
		var n int
		n, hdr, err = header.ReadHeader(r.rdr, r.opts.SkipCallback)
		nRead += n
	}

	if err == nil && hdr.Flags.DictId() && r.opts.DictCallback != nil {
		var ndict []byte
		if ndict, err = r.opts.DictCallback(hdr.DictId); err == nil && ndict != nil {
			r.opts.Dictionary = ndict
		}
	}

	switch {
	case err != nil:
		return nRead, err
	case r.opts.ReadOffset == 0 || r.opts.ReadOffset == hdr.Sz: // NOOP
	case r.opts.ReadOffset < hdr.Sz:
		return nRead, zerr.ErrReadOffset
	case !hdr.Flags.BlockIndependence():
		return nRead, zerr.ErrReadOffsetLinked
	default:
		var n int64
		n, err := skipOffset(r.rdr, r.opts.ReadOffset-hdr.Sz)
		nRead += int(n)
		if err != nil {
			return nRead, err
		}

		// Read offset only applies to first of possible consecutive frames
		r.opts.ReadOffset = 0

		// Do not validate content checksum if we skipped; will fail.
		hdr.Flags.ClrContentChecksum()

		// Do not check the content size if we skipped; will fail.
		r.opts.SkipContentSz = true
	}

	// Cache the content size in the opts ptr to save RAM;
	// Will be validated on end of frame
	if hdr.Flags.ContentSize() {
		v := hdr.ContentSz
		r.opts.ContentSz = &v
	}

	r.blkReader = r.makeBlockReader(hdr)
	return nRead, nil
}

func (r *Reader) modeBody(dst []byte) (nRead int, err error) {
LOOP:
	for {
		if r.dstOff < r.dstBlk.Len() {
			// We have some extra data in dstBuffer
			// Copy into 'dst' up to dst capacity
			n := copy(dst, r.dstBlk.Suffix(r.dstOff))

			// Update our internal offset pointer that
			// tracks where we are in copying from dstBuffer
			r.dstOff += n

			// Update output nRead, used in return.
			nRead += n

			// If we have completely filled 'dst', return.
			if n == len(dst) {
				return
			}

			// Slide dst buffer over by N for next spin
			// after we try to grab the next block.
			dst = dst[n:]
		}

		// Retrieve next lz4 block, and on success copy at top of loop
		if err = r.nextBlock(); err != nil {
			break LOOP
		}
	}

	return
}

func (r *Reader) makeBlockReader(hdr header.HeaderT) blk.BlkRdrI {

	if r.opts.NParallel <= 0 {
		return sync.NewSyncReader(r.rdr, hdr, r.opts)
	}

	// If dependent blocks, must run in serial
	if !hdr.Flags.BlockIndependence() {
		r.opts.NParallel = 1
	}

	return async.NewAsyncReader(r.rdr, hdr, r.opts)
}

// Return true if header already read and reading body.
func (r *Reader) inBodyMode() bool {
	return r.blkReader != nil
}

func skipOffset(rdr io.Reader, offset int64) (int64, error) {

	// Attempt to seek ahead
	if rdr != os.Stdin && rdr != os.Stderr {
		if seeker, ok := rdr.(io.Seeker); ok {
			if _, err := seeker.Seek(offset, io.SeekCurrent); err != nil {
				return 0, err
			}
			return offset, nil
		}
	}

	// Reader is not an io.Seeker, much read and discard
	return io.CopyN(io.Discard, rdr, offset)

}
