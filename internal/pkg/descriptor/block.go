package descriptor

type Block uint8

func (m Block) Size() int {
	return m.Idx().Size()
}

func (m Block) Valid() (v bool) {

	switch {
	case m.Idx() < 4: // 4-7 only supported
	case m&0x80 != 0: // Hit bit is reserved
	case m&0xF != 0: // 4 Low bits are reserved
	default:
		v = true
	}

	return
}

func (m *Block) SetIdx(idx BlockIdxT) {
	*m = Block(idx&0x7) << 4
}

// Convert to BlockIdx, see spec.
func (m Block) Idx() BlockIdxT {
	return BlockIdxT(m >> 4 & 0x7)
}
