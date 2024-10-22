package sparse

import (
	"io"
	"unsafe"
)

const (
	blkSz  = 4 << 10
	readSz = 4 << 10
)

type Writer struct {
	wr   io.Writer
	skip int64
}

func NewWriter(wr io.Writer) *Writer {
	return &Writer{wr: wr}
}

func (w *Writer) Write(data []byte) (int, error) {

	seeker, ok := w.wr.(io.Seeker)
	if !ok {
		return w.wr.Write(data)
	}

	var (
		n         int
		nBlks     = len(data) / blkSz
		loopLimit = nBlks * blkSz
	)

	scanFunc := func(i, sz int) error {
		skip := skipZeros(data[i : i+sz])
		w.skip += int64(skip)

		// Report skips on response to Write even if deferred.
		// io.Write requires an non-nil error if n < len(p)
		n += skip

		if skip < sz {

			// Skip ahead
			if w.skip > 0 {
				if _, err := seeker.Seek(w.skip, io.SeekCurrent); err != nil {
					return err
				}
				w.skip = 0
			}

			// Write the remainder of the block
			wn, err := w.wr.Write(data[i+skip : i+sz])
			n += wn
			if err != nil {
				return err
			}
		}
		return nil
	}

	// Scan at predefined block size
	for i := 0; i < loopLimit; i += blkSz {
		if err := scanFunc(i, blkSz); err != nil {
			return n, err
		}
	}

	var err error

	if nLeft := len(data) % blkSz; nLeft > 0 {
		err = scanFunc(nBlks*blkSz, nLeft)
	}

	return n, err
}

func (w *Writer) ReadFrom(rd io.Reader) (n int64, err error) {

	buf := make([]byte, readSz)

LOOP:
	for {
		var nRead int
		nRead, err = io.ReadFull(rd, buf)
		n += int64(nRead)

		switch err {
		case nil:
			// buf filled, continue
		case io.ErrUnexpectedEOF:
			// buf partially filled, continue
			// expect next io.ReadFull returns io.EOF
		case io.EOF:
			// io.EOF is expected, not an error
			err = nil
			break LOOP
		default:
			break LOOP
		}

		if _, err = w.Write(buf[:nRead]); err != nil {
			break LOOP
		}
	}

	return
}

type flusherI interface {
	Flush() error
}

func (w *Writer) Flush() error {
	if w.skip > 0 {
		// Skip ahead
		seeker := w.wr.(io.Seeker)
		if _, err := seeker.Seek(w.skip, io.SeekCurrent); err != nil {
			return err
		}
		w.skip = 0
	}

	if flusher, ok := w.wr.(flusherI); ok {
		return flusher.Flush()
	}

	return nil
}

// Calls to wr.Write force an escape of the buffer.
// Avoid the escape by preallocating.
var avoidEscape = []byte{0}

func (w *Writer) Close() error {

	if w.skip > 0 {
		// Skip ahead, set pos to last skip - 1, then write the last.
		// This causes the underlying writer, usually a file, to commit the seek.
		if w.skip > 1 {
			seeker := w.wr.(io.Seeker)
			if _, err := seeker.Seek(w.skip-1, io.SeekCurrent); err != nil {
				return err
			}
			w.skip = 1
		}
		if _, err := w.wr.Write(avoidEscape); err != nil {
			return err
		}
		w.skip = 0
	}

	// Should we do this?
	if closer, ok := w.wr.(io.Closer); ok {
		return closer.Close()
	}

	return nil
}

func skipZeros(data []byte) int {
	var (
		i   = 0
		n   = len(data)
		cnt = (n / 256) * 256
	)

	// Unroll the loop at bit increases performance;
	for ; i < cnt; i += 256 {

		v := (*[32]uint64)(unsafe.Pointer(&data[i]))

		var b uint64
		b |= v[0]
		b |= v[1]
		b |= v[2]
		b |= v[3]
		b |= v[4]
		b |= v[5]
		b |= v[6]
		b |= v[7]
		b |= v[8]
		b |= v[9]
		b |= v[10]
		b |= v[11]
		b |= v[12]
		b |= v[13]
		b |= v[14]
		b |= v[15]
		b |= v[16]
		b |= v[17]
		b |= v[18]
		b |= v[19]
		b |= v[20]
		b |= v[21]
		b |= v[22]
		b |= v[23]
		b |= v[24]
		b |= v[25]
		b |= v[26]
		b |= v[27]
		b |= v[28]
		b |= v[29]
		b |= v[30]
		b |= v[31]

		if b != 0 {
			return i
		}
	}

	for ; i < n; i++ {
		if data[i] != 0 {
			return i
		}
	}

	return i
}
