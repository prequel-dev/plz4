package zerr

import "fmt"

type constError string

func (err constError) Error() string {
	return string(err)
}

const (
	EndMark              constError = "lz4 end mark"
	ErrClosed            constError = "lz4 closed"
	ErrCorrupted         constError = "lz4 corrupted"
	ErrHeaderHash        constError = "lz4 header hash mismatch"
	ErrBlockHash         constError = "lz4 block hash mismatch"
	ErrContentHash       constError = "lz4 content hash mismatch"
	ErrHeaderRead        constError = "lz4 fail read header"
	ErrHeaderWrite       constError = "lz4 fail write header"
	ErrMagic             constError = "lz4 bad magic"
	ErrVersion           constError = "lz4 unsupported version"
	ErrDescriptorRead    constError = "lz4 fail read descriptor"
	ErrBlockSizeRead     constError = "lz4 fail read block size"
	ErrBlockRead         constError = "lz4 fail read block"
	ErrBlockSizeOverflow constError = "lz4 block size overflow"
	ErrCompress          constError = "lz4 fail compress"
	ErrDecompress        constError = "lz4 fail decompress"
	ErrReserveBitSet     constError = "lz4 reserved bit set"
	ErrBlockDescriptor   constError = "lz4 invalid BD byte"
	ErrContentHashRead   constError = "lz4 fail read content hash"
	ErrContentSize       constError = "lz4 content size mismatch"
	ErrReadOffset        constError = "lz4 bad read offset"
	ErrReadOffsetLinked  constError = "lz4 read offset unsupported in block linked mode"
	ErrSkip              constError = "lz4 fail skip"
	ErrNibble            constError = "lz4 bad nibble"
	ErrUnsupported       constError = "lz4 unsupported feature"
)

func WrapCorrupted(err error) error {
	return fmt.Errorf("%w: %w", ErrCorrupted, err)
}
