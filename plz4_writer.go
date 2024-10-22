package plz4

import (
	"io"

	"github.com/prequel-dev/plz4/internal/pkg/async"
	"github.com/prequel-dev/plz4/internal/pkg/header"
	"github.com/prequel-dev/plz4/internal/pkg/sync"
)

type Writer interface {
	// Compress 'src' data; return number of bytes written.
	// May be used in sequence with ReadFrom.
	Write(src []byte) (n int, err error)

	// Compress data from 'rd'; return number of bytes read.
	// May be used in sequence with Write.
	ReadFrom(rd io.Reader) (n int64, err error)

	// Flush pending data immediately to 'wr', generating
	//   a new LZ4 Frame block.  If no data is pending, no
	//   block is generated.
	//
	// This is a synchronous call; it will completely flush an
	//   asynchronous pipeline.
	Flush() error

	// Close the Writer to release underlying resources.
	// Close() *MUST* be called on completion whether or not
	//   the Writer is in an error state.
	Close() error
}

// Construct a Writer to compress an LZ4 frame into 'wr'.
//
// Specify optional paramters in 'opts'.
func NewWriter(wr io.Writer, opts ...OptT) Writer {
	o := parseOpts(opts...)

	// Linked blocks is only implemented in async mode.
	if o.BlockLinked && o.NParallel <= 0 {
		o.NParallel = 1
	}

	if o.NParallel <= 0 {
		return sync.NewSyncWriter(wr, &o)
	}

	return async.NewAsyncWriter(wr, &o)
}

// Write a skip frame header to 'wr'.
// A skip frame of exactly size 'sz' must follow the header.
//
//	'sz' 		32-bit unsigned long size of frame.
//	'nibble' 	4-bit value shifted into block magic field
func WriteSkipFrameHeader(wr io.Writer, nibble uint8, sz uint32) (int, error) {
	return header.WriteSkip(wr, nibble, sz)
}
