package blk

import (
	"sync"
	"sync/atomic"

	"github.com/prequel-dev/plz4/internal/pkg/descriptor"
)

// allocate +4 for header +4 for trailer
// We will need the extra space for processing
// checksums on read and checksums+headers on write.

const (
	szOverhead   = 8
	szAlloc4MB   = descriptor.BlockIdx4MBSz + szOverhead
	szAlloc1MB   = descriptor.BlockIdx1MBSz + szOverhead
	szAlloc256KB = descriptor.BlockIdx256KBSz + szOverhead
	szAlloc64KB  = descriptor.BlockIdx64KBSz + szOverhead
)

var (
	pool4MB   = sync.Pool{New: func() any { return &BlkT{data: make([]byte, szAlloc4MB)} }}
	pool1MB   = sync.Pool{New: func() any { return &BlkT{data: make([]byte, szAlloc1MB)} }}
	pool256KB = sync.Pool{New: func() any { return &BlkT{data: make([]byte, szAlloc256KB)} }}
	pool64KB  = sync.Pool{New: func() any { return &BlkT{data: make([]byte, szAlloc64KB)} }}
)

var cntBorrowed int64

func CntBorrowed() int64 {
	return atomic.LoadInt64(&cntBorrowed)
}

func BorrowBlk(bsz int) *BlkT {
	atomic.AddInt64(&cntBorrowed, 1)
	switch bsz {
	case descriptor.BlockIdx4MBSz:
		return pool4MB.Get().(*BlkT)
	case descriptor.BlockIdx1MBSz:
		return pool1MB.Get().(*BlkT)
	case descriptor.BlockIdx256KBSz:
		return pool256KB.Get().(*BlkT)
	case descriptor.BlockIdx64KBSz:
		return pool64KB.Get().(*BlkT)
	}
	panic("bad block size")
}

func ReturnBlk(blk *BlkT) {
	if blk == nil {
		return
	}
	atomic.AddInt64(&cntBorrowed, -1)
	bsz := cap(blk.data)
	blk.data = blk.data[:bsz]
	switch bsz {
	case szAlloc4MB:
		pool4MB.Put(blk)
	case szAlloc1MB:
		pool1MB.Put(blk)
	case szAlloc256KB:
		pool256KB.Put(blk)
	case szAlloc64KB:
		pool64KB.Put(blk)
	default:
		panic("bad block size")
	}
}
