package plz4

import (
	"errors"

	"github.com/prequel-dev/plz4/internal/pkg/zerr"
)

//  Forward declare internal errors

const (
	ErrClosed            = zerr.ErrClosed
	ErrCorrupted         = zerr.ErrCorrupted
	ErrMagic             = zerr.ErrMagic
	ErrVersion           = zerr.ErrVersion
	ErrHeaderHash        = zerr.ErrHeaderHash
	ErrBlockHash         = zerr.ErrBlockHash
	ErrContentHash       = zerr.ErrContentHash
	ErrHeaderRead        = zerr.ErrHeaderRead
	ErrHeaderWrite       = zerr.ErrHeaderWrite
	ErrDescriptorRead    = zerr.ErrDescriptorRead
	ErrBlockSizeRead     = zerr.ErrBlockSizeRead
	ErrBlockRead         = zerr.ErrBlockRead
	ErrBlockSizeOverflow = zerr.ErrBlockSizeOverflow
	ErrCompress          = zerr.ErrCompress
	ErrDecompress        = zerr.ErrDecompress
	ErrReserveBitSet     = zerr.ErrReserveBitSet
	ErrBlockDescriptor   = zerr.ErrBlockDescriptor
	ErrContentHashRead   = zerr.ErrContentHashRead
	ErrContentSize       = zerr.ErrContentSize
	ErrReadOffset        = zerr.ErrReadOffset
	ErrReadOffsetLinked  = zerr.ErrReadOffsetLinked
	ErrSkip              = zerr.ErrSkip
	ErrNibble            = zerr.ErrNibble
	ErrUnsupported       = zerr.ErrUnsupported
)

// Returns true if 'err' indicates that the read input is corrupted.
//
// Note that a short read is not considered corrupted. In that case
// the returned error will be a join of the error context, and the
// underlying error, either an io.EOF or io.ErrUnexpectedEOF.
func Lz4Corrupted(err error) bool {
	return errors.Is(err, ErrCorrupted)
}
