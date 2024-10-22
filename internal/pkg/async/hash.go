package async

import (
	"sync"
	"sync/atomic"

	"github.com/prequel-dev/plz4/internal/pkg/blk"
	"github.com/prequel-dev/plz4/internal/pkg/xxh32"
)

// Note: AsyncHash is not thread safe.
// All API's must be synchronized.

type AsyncHash struct {
	hash xxh32.XXHZero
	wg   sync.WaitGroup
	ch   chan *blk.BlkT
	done atomic.Bool
}

func NewAsyncHash(sz int) *AsyncHash {
	h := &AsyncHash{
		ch: make(chan *blk.BlkT, sz),
	}
	h.wg.Add(1)
	return h
}

func (h *AsyncHash) Queue(qBlk *blk.BlkT) {
	h.ch <- qBlk
}

func (h *AsyncHash) Done() uint32 {
	// Avoid double Done calls
	if !h.done.CompareAndSwap(false, true) {
		return 0
	}
	close(h.ch)
	h.wg.Wait()

	return h.hash.Sum32()
}

func (h *AsyncHash) Run() {
	defer h.wg.Done()

	for srcBlk := range h.ch {
		h.hash.Write(srcBlk.Data())
		blk.ReturnBlk(srcBlk)
	}
}

type blkRef struct {
	blk *blk.BlkT
	idx int64
}

type AsyncHashIdx struct {
	hash xxh32.XXHZero
	wg   sync.WaitGroup
	ch   chan blkRef
	next atomic.Int64
	done atomic.Bool
}

func NewAsyncHashIdx(sz int) *AsyncHashIdx {
	h := &AsyncHashIdx{
		ch: make(chan blkRef, sz),
	}
	h.wg.Add(1)
	return h
}

// Blocks must arrive in order
func (h *AsyncHashIdx) Queue(qBlk *blk.BlkT) {
	h.ch <- blkRef{qBlk, -1}
}

// Free can be called from any goroutine
func (h *AsyncHashIdx) Free(qBlk *blk.BlkT, idx int) {
	if int64(idx) < h.next.Load() {
		blk.ReturnBlk(qBlk)
		return
	}
	h.ch <- blkRef{qBlk, int64(idx)}
}

func (h *AsyncHashIdx) Done() uint32 {
	// Avoid double Done calls
	if !h.done.CompareAndSwap(false, true) {
		return 0
	}
	close(h.ch)
	h.wg.Wait()

	return h.hash.Sum32()
}

func (h *AsyncHashIdx) Run() {
	defer h.wg.Done()

	for ref := range h.ch {
		switch {
		case ref.idx < 0:
			h.hash.Write(ref.blk.Data())
			h.next.Add(1)
		default:
			blk.ReturnBlk(ref.blk)
		}
	}
}
