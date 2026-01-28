package async

import (
	"io"
	"sync"
	"sync/atomic"

	"github.com/prequel-dev/plz4/internal/pkg/blk"
	"github.com/prequel-dev/plz4/internal/pkg/compress"
	"github.com/prequel-dev/plz4/internal/pkg/header"
	"github.com/prequel-dev/plz4/internal/pkg/opts"
	"github.com/prequel-dev/plz4/internal/pkg/trailer"
	"github.com/prequel-dev/plz4/internal/pkg/xxh32"
	"github.com/prequel-dev/plz4/internal/pkg/zerr"
)

type stateFlagT uint8

const (
	flagClosed   stateFlagT = 1 << iota // 0001 (binary)
	flagReported                        // 0010 (binary)
)

func (f stateFlagT) IsSet(flag stateFlagT) bool {
	return f&flag != 0
}

func (f *stateFlagT) Set(flag stateFlagT) {
	*f |= flag
}

type asyncWriterT struct {
	wr      io.Writer
	bsz     int
	srcIdx  int
	srcOff  int
	srcBlk  *blk.BlkT
	inChan  chan inBlkT
	outChan chan outBlkT
	synChan chan int
	semChan chan struct{}
	dict    *blk.BlkT
	wg      sync.WaitGroup
	opts    *opts.OptsT
	hasher  *AsyncHashIdx
	cmpF    compress.CompressorFactory
	state   atomic.Pointer[error]
	flags   stateFlagT
}

func NewAsyncWriter(wr io.Writer, opts *opts.OptsT) *asyncWriterT {

	var (
		bsz  = opts.BlockSizeIdx.Size()
		cmpF = opts.NewCompressorFactory()
	)

	w := &asyncWriterT{
		wr:   wr,
		bsz:  bsz,
		cmpF: cmpF,
		opts: opts,
	}

	// Defer spinning up async goroutines tasks until first buffer is flushed.
	// This allows us to avoid unnecessary resource consumption
	// in the case of very small payloads that fit within
	// a single block.

	// However, if user set content size, we know how large the source
	// data is and can launch the appropriate number of tasks up front.
	if opts.ContentSz != nil {
		if nBuffers := int(*opts.ContentSz)/bsz + 1; nBuffers > 1 {
			w.kickoffAsync()
		}
	}

	return w
}

func (w *asyncWriterT) Write(src []byte) (int, error) {
	var nConsumed int

	for len(src) > 0 && !w.errState() {

		if w.srcBlk == nil {
			w.srcBlk = blk.BorrowBlk(w.bsz)
			w.srcBlk.Trim(w.bsz)
			w.srcOff = 0 // Should be NOOP
		}

		// Copy the source data into our srcBlk
		n := copy(w.srcBlk.Suffix(w.srcOff), src)
		w.srcOff += n
		nConsumed += n

		// Flush block if completely filled
		if w.srcOff == w.bsz {
			w._flushBlk(false)
		}

		// Slide the src buffer over by N for next spin
		src = src[n:]
	}

	return nConsumed, w.reportError()
}

func (w *asyncWriterT) Flush() error {

	// Check error before consuming;
	if err := w.reportError(); err != nil {
		return err
	}

	// Flush out pending data if any
	w.flushBlk(false)

	// If no data has been queue, return.
	if w.srcIdx == 0 {
		return nil
	}

	// Force a flush signal.
	// Notify writeLoop to respond when it processes up to w.srcIdx
	w.synChan <- w.srcIdx

	// Now wait for response
	<-w.synChan

	// Return any error that might have been generated in the meantime
	return w.reportError()
}

func (w *asyncWriterT) Close() error {
	if w.flags.IsSet(flagClosed) {
		return w.reportError()
	}

	// Flush any outstanding data
	w.flushBlk(true)

	if w.semChan != nil {
		// Close down the semaphore.
		// No long necessary after last flush.
		close(w.semChan)

		// Close down the inChan.
		// This will cause the producer goroutines to exit.
		close(w.inChan)

		// Wait for the producer go routines to cycle down;
		// Not safe to close the w.outChan until all have exited.
		w.wg.Wait()

		// Close down the outChan. This is safe because
		// all the producers have closed down via wg.Wait()
		close(w.outChan)

		// Wait for the writeLoop goroutine to exit
		<-w.synChan
	}

	// Return the srcBlk to the pool
	blk.ReturnBlk(w.srcBlk)
	w.srcBlk = nil
	w.srcOff = 0

	// Dump the dict if still around
	blk.ReturnBlk(w.dict)
	w.dict = nil

	var err error

	switch {
	case w.flags.IsSet(flagReported):
		// Should return no error on Close() if error already reported
	case !w.errState():
		// If no error, set our internal error to ErrClosed for
		// subsequent API calls, but return nil.
		w.setError(zerr.ErrClosed)
		w.reportError()
	default:
		//  We are in an error state that is not yet reported.
		//  Mark error as reported,  and return it to the caller.
		err = w.reportError()
	}

	w.flags.Set(flagClosed)
	return err
}

func (w *asyncWriterT) ReadFrom(r io.Reader) (int64, error) {
	var nConsumed int64

LOOP:
	for !w.errState() {

		if w.srcBlk == nil {
			w.srcBlk = blk.BorrowBlk(w.bsz)
			w.srcBlk.Trim(w.bsz)
			w.srcOff = 0 // Should be NOOP
		}

		n, rerr := io.ReadFull(r, w.srcBlk.Suffix(w.srcOff))
		w.srcOff += n
		nConsumed += int64(n)

		switch rerr {
		case nil:
			// srcBlk was filled; flush the block to the out channel
			w._flushBlk(false)
		case io.ErrUnexpectedEOF:
			// Some bytes were read and add to w.srcBlk.
			// Defer flush and spin loop again.
			// Expect io.EOF on next spin.
		case io.EOF:
			// Exit loop; note we may have bytes left in w.srcBlk.
			// Those will get flushed if more data is added on another
			// call to ReadFrom/Write, or Flush/Close.
			break LOOP
		default:
			// Unexpected error, set the error state.
			// Will break on check at top of loop.
			w.setError(rerr)
		}
	}

	return nConsumed, w.reportError()
}

func (w *asyncWriterT) compressLoop() {
	defer w.wg.Done()

	var (
		bsz      = w.bsz
		cmp      = w.cmpF.NewCompressor()
		blkCheck = w.opts.BlockChecksum
	)

	freeSrcBlk := func(srcBlk inBlkT) {
		blk.ReturnBlk(srcBlk.dict)

		if w.hasher != nil {
			// Coordinate block free with hasher
			w.hasher.Free(srcBlk.blk, srcBlk.idx)
		} else {
			blk.ReturnBlk(srcBlk.blk)
		}
	}

LOOP:
	for {
		srcBlk, ok := <-w.inChan
		if !ok {
			// compressLoop only exits on close of w.inChan
			break LOOP
		}

		// Check for error state;
		// On error, don't bother compressing, just drop and continue.
		if w.errState() {
			freeSrcBlk(srcBlk)
			<-w.semChan
			continue
		}

		// Set aside the source size for the outBlkT
		srcSz := srcBlk.blk.Len()

		dstBlk, err := srcBlk.blk.Compress(cmp, bsz, blkCheck, srcBlk.Dict())

		freeSrcBlk(srcBlk)

		w.outChan <- outBlkT{
			err:   err,
			idx:   srcBlk.idx,
			blk:   dstBlk,
			srcSz: srcSz,
		}
	}
}

func (w *asyncWriterT) writeLoop() {

	var (
		wr       = w.wr
		nextIdx  = 0
		flushIdx = -1
		srcMark  = int64(0)
		dstMark  = int64(0)
		pending  = make(map[int]outBlkT, w.opts.NParallel)
	)

	if hdrSz, herr := header.WriteHeader(wr, w.opts); herr != nil {
		w.setError(herr)
	} else {
		dstMark = int64(hdrSz)
	}

LOOP:
	for {
		select {
		case flushIdx = <-w.synChan:
		case outBlk, ok := <-w.outChan:
			// Poll error state in case we get a setError() during ReadFrom
			err := w.getError()

			switch {
			case !ok:
				// WriteLoop may exit *ONLY* when the outCh is closed.
				break LOOP
			case err != nil:
				// Drop block on error state; cannot process in an error state
				w.kickBlock(outBlk.blk)
			case outBlk.idx != nextIdx:
				// Deal with pending block later
				pending[outBlk.idx] = outBlk
			case outBlk.err != nil:
				// Error on incoming block puts parser in error state
				w.kickBlock(outBlk.blk)
				w.setError(outBlk.err)
			default:
				// Main write loop; write the block just received on w.outChan
				for moreData := true; moreData; {

					n, werr := wr.Write(outBlk.blk.Data())

					w.opts.Handler(srcMark, dstMark)
					srcMark += int64(outBlk.srcSz)
					dstMark += int64(n)

					// Return the block whether we get an error or not
					w.kickBlock(outBlk.blk)

					// Bump the nextIdx
					nextIdx += 1

					switch {
					case werr != nil:
						// On error, put parser in error state
						w.setError(werr)
						moreData = false
					default:
						// Check for pending blocks and continue loop if nextIdx available
						if outBlk, moreData = pending[nextIdx]; moreData {
							delete(pending, nextIdx)
						}
					}
				}
			}
		}

		// If a flush is pending and we hit that index, or in error state, respond.
		if flushIdx != -1 && (flushIdx <= nextIdx || w.errState()) {
			w.synChan <- nextIdx
			flushIdx = -1
		}
	}

	// There could be some pending items left in an error case
	for _, outBlk := range pending {
		w.kickBlock(outBlk.blk)
	}

	switch {
	case !w.errState():
		w.opts.Handler(srcMark, dstMark)

		// Write trailer if exiting cleanly
		if _, werr := w.writeTrailer(wr); werr != nil {
			w.setError(werr)
		}
	case w.hasher != nil:
		// Must close down the asyncHash goroutine on error to avoid leak
		w.hasher.Done()
	}

	// Signal completion by closing the syn channel
	close(w.synChan)
}

// Return block and kick the semaphore
func (w *asyncWriterT) kickBlock(doneBlk *blk.BlkT) {
	// Kick the pipeline by releasing semaphore
	<-w.semChan

	// Block has been consumed; return block to the pool
	blk.ReturnBlk(doneBlk)
}

func (w *asyncWriterT) writeTrailer(wr io.Writer) (int, error) {

	if w.hasher == nil {
		return trailer.WriteTrailer(wr)
	}

	xxh := w.hasher.Done()
	return trailer.WriteTrailerWithHash(wr, xxh)
}

func (w *asyncWriterT) flushBlk(close bool) {
	if w.srcOff == 0 {
		return
	}

	// Clip w.srcBlk to whatever we are currently cached to
	w.srcBlk.Trim(w.srcOff)
	w._flushBlk(close)
}

func (w *asyncWriterT) _genDict() (outDict *blk.BlkT) {
	if !w.opts.BlockLinked {
		return nil
	}

	// Return the previously cached dictionary
	outDict = w.dict

	// Set aside last (up to) 64K of the previous
	// block as dictionary input for the next block.
	const maxDict = 64 * 1024

	var (
		off   int
		srcSz = w.srcBlk.Len()
	)

	if srcSz > maxDict {
		off = srcSz - maxDict
	}

	w.dict = blk.BorrowBlk(maxDict)
	w.dict.Trim(srcSz - off)
	copy(w.dict.Data(), w.srcBlk.View(off, srcSz))
	return
}

func (w *asyncWriterT) kickoffAsync() {

	var (
		nPending = w.opts.CalcPending()
	)

	w.inChan = make(chan inBlkT)
	w.outChan = make(chan outBlkT)
	w.synChan = make(chan int)
	w.semChan = make(chan struct{}, nPending)

	go w.writeLoop()

	if w.opts.ContentChecksum {
		w.hasher = NewAsyncHashIdx(w.opts.NParallel)
		go w.hasher.Run()
	}

	// Fire the compressor tasks; limit if we know content size.
	nTasks := w.opts.NParallel
	if w.opts.ContentSz != nil {
		nTasks = min(w.opts.NParallel, int(*w.opts.ContentSz)/w.bsz+1)
	}

	w.wg.Add(nTasks)
	for range nTasks {
		w.opts.WorkerPool.Submit(w.compressLoop)
	}
}

func (w *asyncWriterT) _flushBlk(close bool) {
	if w.srcBlk == nil || w.srcOff == 0 {
		// Nothing to flush; return
		return
	}

	switch {
	case w.semChan != nil:
		// Async already kicked; proceed
	case close:
		// Go close before flushing first block.
		// Run synchronous write to avoid spinning up goroutines unnecessarily.
		w.writeSync()
		return
	default:
		w.kickoffAsync()
	}

	// Defer content hash to minimize pipeline blockage
	if w.hasher != nil {
		w.hasher.Queue(w.srcBlk)
	}

	// Acquire semaphore before outputing
	w.semChan <- struct{}{}

	// Queue the block to a compressor task
	w.inChan <- inBlkT{
		idx:  w.srcIdx,
		blk:  w.srcBlk,
		dict: w._genDict(),
	}

	w.srcBlk = nil
	w.srcOff = 0
	w.srcIdx += 1
}

func (w *asyncWriterT) writeSync() {
	if err := w._writeSync(); err != nil {
		w.setError(err)
	}
}

func (w *asyncWriterT) _writeSync() error {

	hdrSz, err := header.WriteHeader(w.wr, w.opts)
	if err != nil {
		return err
	}

	var (
		cmp = w.cmpF.NewCompressor()
	)

	// Compress and write out the final block.
	// The dictionary, if included, is applied by the compressor.
	dstBlk, err := blk.CompressToBlk(w.srcBlk.Data(), cmp, w.bsz, w.opts.BlockChecksum, nil)
	if err != nil {
		return err
	}
	defer blk.ReturnBlk(dstBlk)

	_, err = w.wr.Write(dstBlk.Data())

	if err != nil {
		return err
	}

	w.opts.Handler(0, int64(hdrSz))

	switch w.opts.ContentChecksum {
	case true:
		var hasher xxh32.XXHZero
		hasher.Write(w.srcBlk.Data())
		_, err = trailer.WriteTrailerWithHash(w.wr, hasher.Sum32())
	default:
		_, err = trailer.WriteTrailer(w.wr)
	}

	return err
}

// First error wins.  Nil error will panic.
func (w *asyncWriterT) setError(err error) bool {
	return w.state.CompareAndSwap(nil, &err)
}

// Mark the errors as reported;
// ie. we have returned it to the caller at least once.
// This helps differentiate on the Close() call,
// where we want to return nil on a Close() after an
// error has already reported.
func (w *asyncWriterT) reportError() error {
	err := w.getError()
	if err != nil {
		// Once we are in an error state, the state is marked reported
		// if error is returned on a user facing API.
		// Does not require mutex because only called from caller goroutine.
		w.flags.Set(flagReported)
	}
	return err
}

func (w *asyncWriterT) getError() error {
	v := w.state.Load()
	if v == nil {
		return nil
	}
	return *v
}

func (w *asyncWriterT) errState() bool {
	return w.state.Load() != nil
}
