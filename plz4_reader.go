package plz4

import (
	"io"

	"github.com/prequel-dev/plz4/internal/pkg/rdr"
)

// Reader is an interface for reading LZ4 compressed data.
//
// It implements the io.ReadCloser and the io.WriterTo interfaces.
type Reader interface {
	// Read decompressed data into 'dst'.  Return number bytes read.
	Read(dst []byte) (n int, err error)

	// Decompress to 'wr'.  Return number bytes written.
	WriteTo(wr io.Writer) (int64, error)

	// Close the Reader to release underlying resources.
	// Close() *MUST* be called on completion whether or not
	//   the Reader is in an error state.
	Close() error
}

// Construct a Reader to decompress the LZ4 frame from 'rd'.
//
// Specify optional parameters in 'opts'.
func NewReader(rd io.Reader, opts ...OptT) Reader {
	// 'o' will escape as we are taking a ptr in Reader.
	// We do this to save space since also passing to underlying implementation.
	o := parseOpts(opts...)
	return rdr.NewReader(rd, &o)
}
