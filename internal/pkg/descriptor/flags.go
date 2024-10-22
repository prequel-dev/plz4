package descriptor

const (
	bitDictId            = 0
	bitReserved          = 1
	bitContentChecksum   = 2
	bitSize              = 3
	bitBlockChecksum     = 4
	bitBlockIndependence = 5
)

type Flags uint8

func (m Flags) DictId() bool            { return m.isSet(bitDictId) }
func (m Flags) Reserved() bool          { return m.isSet(bitReserved) }
func (m Flags) ContentChecksum() bool   { return m.isSet(bitContentChecksum) }
func (m Flags) ContentSize() bool       { return m.isSet(bitSize) }
func (m Flags) BlockChecksum() bool     { return m.isSet(bitBlockChecksum) }
func (m Flags) BlockIndependence() bool { return m.isSet(bitBlockIndependence) }
func (m Flags) Version() uint8          { return uint8(m >> 6 & 0x3) }

func (m *Flags) SetDictId()            { m.setBit(bitDictId) }
func (m *Flags) SetReserved()          { m.setBit(bitReserved) }
func (m *Flags) SetContentChecksum()   { m.setBit(bitContentChecksum) }
func (m *Flags) SetContentSize()       { m.setBit(bitSize) }
func (m *Flags) SetBlockChecksum()     { m.setBit(bitBlockChecksum) }
func (m *Flags) SetBlockIndependence() { m.setBit(bitBlockIndependence) }
func (m *Flags) SetVersion(v uint8)    { *m = *m&^(0x3<<6) | (Flags(v&0x3) << 6) }

func (m *Flags) ClrContentChecksum() { m.clrBit(bitContentChecksum) }

func (m Flags) isSet(pos uint8) bool {
	return (m & (1 << pos)) != 0
}

func (m *Flags) setBit(pos uint8) {
	*m |= (1 << pos)
}

func (m *Flags) clrBit(pos uint8) {
	*m &= ^(1 << pos)
}
