package plz4

import (
	"runtime"

	"github.com/prequel-dev/plz4/internal/pkg/compress"
	"github.com/prequel-dev/plz4/internal/pkg/descriptor"
	"github.com/prequel-dev/plz4/internal/pkg/opts"
)

// OptT is a function that sets an option on the processor.
type OptT func(*opts.OptsT)

// WorkerPool is an interface for a worker pool implementation.
type WorkerPool = opts.WorkerPool

// BlockIdxT is a type for block size index.
type BlockIdxT = descriptor.BlockIdxT

// LevelT is a type for compression level.
type LevelT = compress.LevelT

// Progress callback function type.
type CbProgressT = opts.ProgressFuncT

// Skip callback function type.
type CbSkipT = opts.SkipCallbackT

// Dictionary callback function type.
type CbDictT = opts.DictCallbackT

const (
	// 64 KiB block size
	BlockIdx64KB = descriptor.BlockIdx64KB

	// 256 KiB block size
	BlockIdx256KB = descriptor.BlockIdx256KB

	// 1 MiB block size
	BlockIdx1MB = descriptor.BlockIdx1MB

	// 4 MiB block size
	BlockIdx4MB = descriptor.BlockIdx4MB
)

const (
	Level1 LevelT = iota + 1
	Level2
	Level3
	Level4
	Level5
	Level6
	Level7
	Level8
	Level9
	Level10
	Level11
	Level12
)

/////////////////
// Global options
/////////////////

// Specify number of go routines to run in parallel.  Defaults to 1.
//
//	0   Process synchronously
//	1+  Process asynchronously
//	<0  Process asynchronously with the number of goroutines up to the CPU count
func WithParallel(n int) OptT {
	return func(o *opts.OptsT) {
		numCPU := runtime.NumCPU()
		if n < 0 || n > numCPU {
			o.NParallel = numCPU
		} else {
			o.NParallel = n
		}
	}
}

// Specify the maximum pending buffer size.  Defaults to nParallel * blockSz.
//
// Larger maximum pending size improves parallel processing throughput
// at the expense of RAM.  The default is the minimal allowed size.
// This option only applies to the asynchronous case.
// It is ignored in the synchronous case.
//
// Setting the pending size to -1 enables auto mode.  In auto mode, the processor will automatically
// scale the pending size for maximum speed based on the block size and nParallel.
func WithPendingSize(n int) OptT {
	return func(o *opts.OptsT) {
		o.PendingSz = n
	}
}

// Enable full content checksum. Defaults to enabled.
//
//	ReadMode: 	Calculate and append content checksum if enabled
//	WriteMode: 	Validate content checksum if provided; ignore if disabled.
func WithContentChecksum(enable bool) OptT {
	return func(o *opts.OptsT) {
		o.ContentChecksum = enable
	}
}

// Optional worker pool for both compress and decompress modes.
func WithWorkerPool(wp WorkerPool) OptT {
	return func(o *opts.OptsT) {
		o.WorkerPool = wp
	}
}

// Processor will emit tuple (src_block_offset, dst_blk_offset) on each
// block boundary.  Applies to both compress and decompress modes.
//
// Offsets are relative to the start of the frame.
//
// Note: Callback may be called from a secondary goroutine.
// However, offsets will emit in order from only that goroutine.
func WithProgress(cb CbProgressT) OptT {
	return func(o *opts.OptsT) {
		o.Handler = cb
	}
}

// Provide a dictionary for compress or decompress mode.
// Only last 64KiB is used.
func WithDictionary(data []byte) OptT {
	return func(o *opts.OptsT) {
		o.Dictionary = data
	}
}

/////////////////////////////////
// Write Options: ignored on read
/////////////////////////////////

// Specify write compression level [1-12].  Defaults to Level1.
func WithLevel(lvl LevelT) OptT {
	return func(o *opts.OptsT) {
		switch {
		case lvl < Level1:
			lvl = Level1
		case lvl > Level12:
			lvl = Level12
		}
		o.Level = compress.LevelT(lvl)
	}
}

// Enable block checksums on write.  Defaults to disabled.
func WithBlockChecksum(enable bool) OptT {
	return func(o *opts.OptsT) {
		o.BlockChecksum = enable
	}
}

// Specify write block size.  Defaults to BlockIdx4MB.
func WithBlockSize(idx BlockIdxT) OptT {
	return func(o *opts.OptsT) {
		if !idx.Valid() {
			// Use default on invalid input
			idx = BlockIdx4MB
		}
		o.BlockSizeIdx = idx
	}
}

// Enable linked blocks on write.  Defaults to disabled.
func WithBlockLinked(enable bool) OptT {
	return func(o *opts.OptsT) {
		o.BlockLinked = enable
	}
}

// Specify write content size to embed in header.
func WithContentSize(sz uint64) OptT {
	return func(o *opts.OptsT) {
		o.ContentSz = &sz
	}
}

// Specify dictionary identifer to embed in header on write.
func WithDictionaryId(id uint32) OptT {
	return func(o *opts.OptsT) {
		o.DictionaryId = &id
	}
}

////////////////////////////////
// Read Options; ignored on write
////////////////////////////////

// Read block starting at byte 'offset'.
//
// The offset is the first byte of the data block relative to the start of the frame.
func WithReadOffset(offset int64) OptT {
	return func(o *opts.OptsT) {
		o.ReadOffset = offset
	}
}

// Enable content size check.  Defaults to enabled.
//
// According to spec, the content size is informational so in some cases it
// may be desirable to skip the check.
func WithContentSizeCheck(enabled bool) OptT {
	return func(o *opts.OptsT) {
		o.SkipContentSz = !enabled
	}
}

// Specify skip block callback function.
//
// Callback will emit on a skip frame. The callback
// must consume exactly 'sz' bytes from the reader.
func WithSkipCallback(cb CbSkipT) OptT {
	return func(o *opts.OptsT) {
		o.SkipCallback = cb
	}
}

// Specify optional dictionary callback.
//
// Engine will emit callback when a dictionary identifier
// is read in the frame header.  An optional dictionary
// may be returned from callback.  This dictionary will
// overide any dictionary previously specified with the
// WithDictionary() option.
func WithDictCallback(cb CbDictT) OptT {
	return func(o *opts.OptsT) {
		o.DictCallback = cb
	}
}

func defaultHandler(int64, int64) {}

func parseOpts(optFuncs ...OptT) opts.OptsT {
	o := opts.OptsT{
		Level:           Level1,              // Fast by default
		NParallel:       1,                   // Run async by default
		Handler:         defaultHandler,      // NOOP
		BlockSizeIdx:    BlockIdx4MB,         // 4MB is default size
		WorkerPool:      opts.StubWorkerPool, // Stub worker pool with simple go dispatch
		ContentChecksum: true,                // Spec recommends default true; but does slow things down.
		ContentSz:       nil,                 // Default to unset
		SkipContentSz:   false,               // Check content size enabled by default
	}

	for _, oFunc := range optFuncs {
		oFunc(&o)
	}

	return o
}
