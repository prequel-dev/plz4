package async

import (
	"io"
	"sync"

	"github.com/prequel-dev/plz4/internal/pkg/blk"
	"github.com/prequel-dev/plz4/internal/pkg/compress"
	"github.com/prequel-dev/plz4/internal/pkg/header"
	"github.com/prequel-dev/plz4/internal/pkg/opts"
	"github.com/prequel-dev/plz4/internal/pkg/zerr"
)

type asyncRdrT struct {
	frameRdr blk.FrameReader
	nextIdx  int
	inChan   chan inBlkT
	outChan  chan outBlkT
	finChan  chan struct{}
	semChan  chan struct{}
	pending  map[int]outBlkT
	hasher   *AsyncHash
	dc       compress.Decompressor
	wg       sync.WaitGroup
}

type inBlkT struct {
	idx   int
	srcSz int
	blk   *blk.BlkT
	dict  *blk.BlkT
}

type outBlkT struct {
	err   error
	idx   int
	srcSz int
	blk   *blk.BlkT
}

func (b inBlkT) Dict() []byte {
	if b.dict == nil {
		return nil
	}
	return b.dict.Data()
}

func NewAsyncReader(rdr io.Reader, hdr header.HeaderT, opts *opts.OptsT) *asyncRdrT {

	var (
		dict     *compress.DictT
		bsz      = hdr.BlockDesc.Idx().Size()
		nPending = opts.CalcPending()
	)
	if opts.Dictionary != nil || !hdr.Flags.BlockIndependence() {
		dict = compress.NewDictT(opts.Dictionary, !hdr.Flags.BlockIndependence())
	}

	r := &asyncRdrT{
		inChan:  make(chan inBlkT),
		outChan: make(chan outBlkT),
		finChan: make(chan struct{}),
		semChan: make(chan struct{}, nPending),
		pending: make(map[int]outBlkT, opts.NParallel),
		frameRdr: *blk.NewFrameReader(
			rdr,
			bsz,
			hdr.Flags.ContentChecksum(),
			hdr.Flags.BlockChecksum(),
		),
		dc: compress.NewDecompressor(
			hdr.Flags.BlockIndependence(),
			dict,
		),
	}

	// Note: control routine must be outside of workerpool.
	// Otherwise could dead lock on too many simultaneous request

	if hdr.Flags.ContentChecksum() && (opts.ReadOffset == 0) && opts.ContentChecksum {
		r.hasher = NewAsyncHash(opts.NParallel)
		go r.hasher.Run()
	}

	// Spin up all the goroutines
	// Note: control routine must be outside of workerpool.
	// Otherwise could dead lock on too many simultaneous request
	go r.dispatch()

	nTasks := opts.NParallel
	if hdr.Flags.ContentSize() && hdr.ContentSz > 0 {
		// If content size known, can limit number of tasks
		nTasks = min(opts.NParallel, int(hdr.ContentSz)/bsz+1)
	}

	r.wg.Add(nTasks)
	for range nTasks {
		opts.WorkerPool.Submit(r.decompress)
	}

	return r
}

func (r *asyncRdrT) dispatch() {

	curIdx, err := r._readLoop()

	// We are done on the input channel; close it down.
	// This will signal the compressor goroutines to exit.
	close(r.inChan)

	// Close down the semaphore. Avoid blocking on release.
	close(r.semChan)

	// Send final error packet on the outChan if necessary.
	if err != nil {
		outBlk := outBlkT{
			err: err,
			idx: curIdx,
		}
		select {
		case r.outChan <- outBlk:
		case <-r.finChan:
		}
	}
}

func (r *asyncRdrT) _readLoop() (int, error) {

	var curIdx int

LOOP:
	for {

		select {
		// Acquire semaphore
		case r.semChan <- struct{}{}:
		// Abort on fin
		case <-r.finChan:
			break LOOP
		}

		frame, err := r.frameRdr.Read()

		if err != nil {
			return curIdx, err
		}

		if frame.Uncompressed {
			// If uncompressed; send directly to outChan.
			// Decompress not necessary.
			outBlk := outBlkT{
				idx:   curIdx,
				blk:   frame.Blk,
				srcSz: frame.ReadCnt,
			}

			select {
			case r.outChan <- outBlk:
			case <-r.finChan:
				blk.ReturnBlk(frame.Blk)
				break LOOP
			}

		} else {
			// Otherwise, queue up on inChan for decompressor.
			inBlk := inBlkT{
				idx:   curIdx,
				srcSz: frame.ReadCnt,
				blk:   frame.Blk,
			}

			select {
			case r.inChan <- inBlk:
			case <-r.finChan:
				blk.ReturnBlk(frame.Blk)
				break LOOP
			}
		}

		curIdx += 1
	}

	return curIdx, nil
}

func (r *asyncRdrT) decompress() {
	defer r.wg.Done()
	r._decompressLoop()
}

func (r *asyncRdrT) _decompressLoop() {

LOOP:
	for {
		srcBlk, ok := <-r.inChan
		if !ok {
			break LOOP // inChan closed, we are done
		}

		// Decompress the source block
		dstBlk, err := srcBlk.blk.Decompress(r.dc)

		// Return srcBlk no longer necessary.
		blk.ReturnBlk(srcBlk.blk)

		select {
		case r.outChan <- outBlkT{
			err:   err,
			idx:   srcBlk.idx,
			blk:   dstBlk,
			srcSz: srcBlk.srcSz,
		}:
		case <-r.finChan:
			if dstBlk != nil {
				blk.ReturnBlk(dstBlk)
			}
			break LOOP
		}
	}
}

func (r *asyncRdrT) NextBlock(prevBlk *blk.BlkT) (*blk.BlkT, int, error) {

	if prevBlk != nil {
		switch r.hasher {
		case nil:
			blk.ReturnBlk(prevBlk)
		default:
			r.hasher.Queue(prevBlk)
		}
	}

	nextBlk, nRead, err := r._nextBlock()

	switch {
	case err != zerr.EndMark:
	case r.hasher == nil:
	case r.hasher.Done() != r.frameRdr.ContentChecksum():
		err = zerr.WrapCorrupted(zerr.ErrContentHash)
	}

	return nextBlk, nRead, err
}

func (r *asyncRdrT) _nextBlock() (*blk.BlkT, int, error) {

	// Check if nextIdx is already in pending list.
	// If so, remove from list and continue
	if p, ok := r.pending[r.nextIdx]; ok {
		<-r.semChan // Release semaphore
		delete(r.pending, r.nextIdx)
		r.nextIdx += 1
		return p.blk, p.srcSz, p.err
	}

	for {
		outBlk := <-r.outChan

		// Check if outBlk from outChan is the nextIdx,
		// If so return it.
		if outBlk.idx == r.nextIdx {
			<-r.semChan // Release semaphore
			r.nextIdx += 1
			return outBlk.blk, outBlk.srcSz, outBlk.err
		}

		// Block came in out of order; add to pending map
		r.pending[outBlk.idx] = outBlk
	}
}

func (r *asyncRdrT) Close() {
	// defend double close
	select {
	case <-r.finChan:
	default:
		r._close()
	}
}

func (r *asyncRdrT) _close() {

	// Send close signal with finChan
	close(r.finChan)

	// Wait for all compression workers to close
	r.wg.Wait()

	// Drain inChan
	for inBlk := range r.inChan {
		if inBlk.blk != nil {
			blk.ReturnBlk(inBlk.blk)
		}
	}

	// Drain outChan; not closed so have to loop until empty
LOOP:
	for {
		select {
		case outBlk := <-r.outChan:
			if outBlk.blk != nil {
				blk.ReturnBlk(outBlk.blk)
			}
		default:
			break LOOP
		}
	}

	// Drain pending
	for idx, p := range r.pending {
		if p.blk != nil {
			blk.ReturnBlk(p.blk)
		}
		delete(r.pending, idx)
	}

	// Close hasher; could be a noop.
	if r.hasher != nil {
		r.hasher.Done()
	}

}
