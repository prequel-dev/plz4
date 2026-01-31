package blk

func ShrinkToFit(buf []byte, n int) []byte {
	if n >= len(buf) {
		return buf
	}
	nBuf := make([]byte, n)
	copy(nBuf, buf[:n])
	return nBuf
}
