package trailer

import (
	"encoding/binary"
	"io"
)

var emptyTrailer [4]byte

func WriteTrailer(wr io.Writer) (int, error) {
	// Use global to avoid escaped alloc on wr.Write
	return wr.Write(emptyTrailer[:])
}

func WriteTrailerWithHash(wr io.Writer, xxh uint32) (int, error) {
	var buf [8]byte
	binary.LittleEndian.PutUint32(buf[4:], xxh)
	return wr.Write(buf[:])
}
