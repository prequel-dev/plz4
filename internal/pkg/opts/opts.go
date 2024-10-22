package opts

import (
	"io"

	"github.com/prequel-dev/plz4/internal/pkg/compress"
	"github.com/prequel-dev/plz4/internal/pkg/descriptor"
)

type BlockIdxT = descriptor.BlockIdxT

// The 'nibble' parameter contains the user defined
// low nibble in the skip frame magic prefix.
//
// Callback must return number of bytes read.
type SkipCallbackT func(rdr io.Reader, nibble uint8, sz uint32) (int, error)

// Emits offset in bytes from beginning of stream of src and corresponding block.
type ProgressFuncT func(srcOffset, dstPos int64)

// Emits 'id' embedded in header.  Callback may return optional dictionary.
type DictCallbackT func(id uint32) ([]byte, error)

type OptsT struct {
	NParallel       int
	PendingSz       int
	Level           compress.LevelT
	ContentSz       *uint64
	ReadOffset      int64
	BlockChecksum   bool
	BlockLinked     bool
	ContentChecksum bool
	SkipContentSz   bool
	Dictionary      []byte
	DictionaryId    *uint32
	DictCallback    DictCallbackT
	BlockSizeIdx    BlockIdxT
	Handler         ProgressFuncT
	WorkerPool      WorkerPool
	SkipCallback    SkipCallbackT
}

type WorkerPool interface {
	Submit(task func())
}

func (o OptsT) NewCompressorFactory() compress.CompressorFactory {

	var dict *compress.DictT

	if o.Dictionary != nil {
		dict = compress.NewDictT(o.Dictionary, o.BlockLinked)
	}

	return compress.NewCompressorFactory(
		o.Level,
		!o.BlockLinked,
		dict,
	)
}

func (o OptsT) CalcPending() int {

	if o.PendingSz == 0 {
		return o.NParallel
	}

	// If auto mode, calculate limit to maximize
	// throughput based on block size
	if o.PendingSz == -1 {
		var mult int
		switch o.BlockSizeIdx {
		case descriptor.BlockIdx64KB:
			mult = 16
		case descriptor.BlockIdx256KB:
			mult = 8
		case descriptor.BlockIdx1MB:
			mult = 4
		default:
			mult = 2
		}
		return mult * o.NParallel
	}

	var (
		bsz      = o.BlockSizeIdx.Size()
		nPending = o.NParallel
	)

	if nCap := o.PendingSz / bsz; nCap > nPending {
		nPending = nCap
	}

	return nPending
}

type stubWorkerPool struct {
}

func (s *stubWorkerPool) Submit(task func()) {
	go task()
}

var StubWorkerPool = &stubWorkerPool{}
